use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::convert::TryFrom;
use std::thread;
use std::time::Duration;

extern crate inotify;

extern crate failure;

extern crate lapin_futures;
use lapin_futures::channel::{BasicConsumeOptions, QueueDeclareOptions, QueueBindOptions};
use lapin_futures::types::FieldTable;

use tokio::net::TcpStream;
use tokio::runtime::current_thread::Runtime;
use tokio::prelude::*;

use crate::settings;
use crate::metrics;

use cortex_core::sftp_connection::SftpConnection;
use cortex_core::SftpDownload;

use failure::Error;

use tee::TeeReader;
use sha2::{Sha256, Digest};

pub struct SftpDownloader {
    pub sftp_source: settings::SftpSource,
    pub sftp_connection: SftpConnection,
    pub db_connection: postgres::Connection,
    pub local_storage_path: PathBuf
}

impl SftpDownloader {
    pub fn start(
        amqp_client: lapin_futures::client::Client<TcpStream>,
        sftp_source: settings::SftpSource,
        data_dir: PathBuf,
        db_url: String,
    ) -> thread::JoinHandle<()> {
        let join_handle = thread::spawn(move || {
            let conn = loop {
                let conn_result = SftpConnection::new(
                    &sftp_source.address.clone(),
                    &sftp_source.username.clone(),
                    sftp_source.compress,
                );

                match conn_result {
                    Ok(c) => break c,
                    Err(e) => error!("Could not connect: {}", e),
                }

                thread::sleep(Duration::from_millis(1000));
            };

            let db_conn_result = postgres::Connection::connect(db_url.clone(), postgres::TlsMode::None);

            let db_conn = match db_conn_result {
                Ok(c) => {
                    info!("Connected to database");
                    c
                }
                Err(e) => {
                    error!("Error connecting to database: {}", e);
                    ::std::process::exit(2);
                }
            };

            let mut runtime = Runtime::new().unwrap();

            runtime.spawn(future::ok(()));

            let mut sftp_downloader = SftpDownloader {
                sftp_source: sftp_source.clone(),
                sftp_connection: conn,
                db_connection: db_conn,
                local_storage_path: data_dir.clone(),
            };

            let sftp_source_name = sftp_source.name.clone();
            let sftp_source_name_2 = sftp_source.name.clone();

            let stream = amqp_client.create_channel().map_err(Error::from).and_then(|mut channel| {
                info!("Created channel with id {}", channel.id());

                let queue_name = format!("source.{}", &sftp_source_name);

                channel.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::new()).map(|queue| (channel, queue)).and_then(move |(mut channel, queue)| {
                    info!("Channel {} declared queue {}", channel.id(), &queue_name);

                    let routing_key = format!("source.{}", &sftp_source_name);
                    let exchange = "amq.direct";

                    channel.queue_bind(&queue_name, &exchange, &routing_key, QueueBindOptions::default(), FieldTable::new())
                        .map(|_| (channel, queue))
                }).and_then(move |(mut channel, queue)| {
                    // basic_consume returns a future of a message
                    // stream. Any time a message arrives for this consumer,
                    // the for_each method would be called
                    channel.basic_consume(&queue, "my_consumer", BasicConsumeOptions::default(), FieldTable::new()).map(|stream| (channel, stream))
                }).and_then(move |(mut channel, stream)| {
                    info!("got consumer stream");

                    stream.for_each(move |message| -> Box<dyn Future< Item = (), Error = lapin_futures::error::Error> + 'static + Send> {
                        metrics::MESSAGES_RECEIVED_COUNTER.with_label_values(&[&sftp_source_name_2]).inc();
                        debug!("Received message from RabbitMQ");

                        let deserialize_result: serde_json::Result<SftpDownload> = serde_json::from_slice(message.data.as_slice());

                        match deserialize_result {
                            Ok(command) => {
                                let download_result = sftp_downloader.handle(&command);

                                match download_result {
                                    Ok(_) => Box::new(channel.basic_ack(message.delivery_tag, false)),
                                    Err(e) => {
										error!("Error downloading {}: {}", &command.path, e);
										Box::new(channel.basic_nack(message.delivery_tag, false, false))
									}
                                }
                            },
                            Err(e) => {
                                error!("Error deserializing command: {}", e);
                                Box::new(channel.basic_nack(message.delivery_tag, false, false))
                            }
                        }
                    })
                }).and_then(|_| {
                    info!("Consumer stream ended");
                    future::ok(())
                })
                .map_err(Error::from)
            }).map_err(|e| {
                error!("{}", e);
            });

            runtime.block_on(stream).unwrap();

            runtime.run().unwrap();
        });

        join_handle
    }

    pub fn handle(&mut self, msg: &SftpDownload) -> Result<(), String> {
        let remote_path = Path::new(&msg.path);

        let local_path = if remote_path.is_absolute() {
            self.local_storage_path.join(remote_path.strip_prefix("/").unwrap())
        } else {
            self.local_storage_path.join(remote_path)
        };

        match msg.size {
            Some(size) => {
                info!(
                    "{} downloading '{}' -> '{}' {} bytes",
                    self.sftp_source.name,
                    msg.path,
                    local_path.to_str().unwrap(),
                    size
                );
            },
            None => {
                info!(
                    "{} downloading '{}' size unknown",
                    self.sftp_source.name,
                    msg.path
                );
            }
        }

        let open_result = self.sftp_connection.sftp.open(&remote_path);

        let mut remote_file = match open_result {
            Ok(remote_file) => remote_file,
            Err(e) => {
                return Err(format!("Error opening remote file {}: {}", msg.path, e));
            }
        };

        let local_path_parent = local_path.parent().unwrap();

        if !local_path_parent.exists() {
            let create_dir_result = std::fs::create_dir_all(local_path_parent);

            match create_dir_result {
                Ok(()) => {
                    info!("Created containing directory '{}'", local_path_parent.to_str().unwrap());
                },
                Err(e) => {
                    return Err(format!("Error creating containing directory '{}': {}", local_path_parent.to_str().unwrap(), e));
                }
            }
        }

        let file_create_result = File::create(&local_path);

        let mut local_file = match file_create_result {
            Ok(file) => file,
            Err(e) => {
                return Err(format!("Could not create file {}: {}", local_path.to_str().unwrap(), e));
            }
        };

        let mut sha256 = Sha256::new();

        let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

        let copy_result = io::copy(&mut tee_reader, &mut local_file);

        match copy_result {
            Ok(bytes_copied) => {
                info!("{} downloaded '{}', {} bytes", self.sftp_source.name, msg.path, bytes_copied);

                let hash = format!("{:x}", sha256.result());

                let execute_result = self.db_connection.execute(
                    "insert into dispatcher.sftp_download (remote, path, size, hash) values ($1, $2, $3, $4)",
                    &[&self.sftp_source.name, &msg.path, &i64::try_from(bytes_copied).unwrap(), &hash]
                );

                match execute_result {
                    Ok(_) => {},
                    Err(e) => {
                        error!("Error inserting download record into database: {}", e);
                    }
                }

                metrics::FILE_DOWNLOAD_COUNTER_VEC.with_label_values(&[&self.sftp_source.name]).inc();
                metrics::BYTES_DOWNLOADED_COUNTER_VEC.with_label_values(&[&self.sftp_source.name]).inc_by(bytes_copied as i64);

                let remove_after_download = true;

                if remove_after_download {
                    let unlink_result = self.sftp_connection.sftp.unlink(&remote_path);
                    
                    match unlink_result {
                        Ok(_) => {
                            info!("{} removed '{}'", self.sftp_source.name, msg.path);
                        },
                        Err(e) => {
                            error!("{} error removing '{}': {}", self.sftp_source.name, msg.path, e);
                        }
                    }
                }
                
                Ok(())
            }
            Err(e) => {
                Err(format!("{} error downloading '{}': {}", self.sftp_source.name, msg.path, e))
            }
        }
    }
}
