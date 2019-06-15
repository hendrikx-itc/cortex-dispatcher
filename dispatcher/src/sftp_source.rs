use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

extern crate failure;

extern crate lapin_futures;
use lapin_futures::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin_futures::types::FieldTable;

use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;
use tokio::sync::mpsc::UnboundedSender;

use crate::event::FileEvent;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;

use cortex_core::sftp_connection::SftpConnection;
use cortex_core::SftpDownload;

use failure::Error;

use sha2::{Digest, Sha256};
use tee::TeeReader;

pub struct SftpDownloader<T>
where
    T: Persistence,
{
    pub sftp_source: settings::SftpSource,
    pub sftp_connection: SftpConnection,
    pub persistence: T,
    pub local_storage_path: PathBuf,
}

impl<T> SftpDownloader<T>
where
    T: Persistence,
    T: Send,
    T: Clone,
    T: 'static,
{
    pub fn start(
        amqp_client: lapin_futures::Client,
        config: settings::SftpSource,
        mut sender: UnboundedSender<FileEvent>,
        data_dir: PathBuf,
        persistence: T,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let conn = loop {
                let conn_result = SftpConnection::new(
                    &config.address.clone(),
                    &config.username.clone(),
                    config.password.clone(),
                    config.compress,
                );

                match conn_result {
                    Ok(c) => break c,
                    Err(e) => error!("Could not connect: {}", e),
                }

                thread::sleep(Duration::from_millis(1000));
            };

            let mut runtime = Runtime::new().unwrap();

            runtime.spawn(future::ok(()));

            let mut sftp_downloader = SftpDownloader {
                sftp_source: config.clone(),
                sftp_connection: conn,
                persistence: persistence,
                local_storage_path: data_dir.clone(),
            };

            let sftp_source_name = config.name.clone();
            let sftp_source_name_2 = config.name.clone();

            let stream = amqp_client
                .create_channel()
                .map_err(Error::from)
                .and_then(|channel| {
                    info!("Created channel with id {}", channel.id());

                    let queue_name = format!("source.{}", &sftp_source_name);

                    channel
                        .queue_declare(
                            &queue_name,
                            QueueDeclareOptions::default(),
                            FieldTable::default(),
                        )
                        .map(|queue| (channel, queue))
                        .and_then(move |(channel, queue)| {
                            info!("Channel {} declared queue {}", channel.id(), &queue_name);

                            let routing_key = format!("source.{}", &sftp_source_name);
                            let exchange = "amq.direct";

                            channel
                                .queue_bind(
                                    &queue_name,
                                    &exchange,
                                    &routing_key,
                                    QueueBindOptions::default(),
                                    FieldTable::default(),
                                )
                                .map(|_| (channel, queue))
                        })
                        .and_then(move |(channel, queue)| {
                            // basic_consume returns a future of a message
                            // stream. Any time a message arrives for this consumer,
                            // the for_each method would be called
                            channel
                                .basic_consume(
                                    &queue,
                                    "cortex-dispatcher",
                                    BasicConsumeOptions::default(),
                                    FieldTable::default(),
                                )
                                .map(|stream| (channel, stream))
                        })
                        .and_then(move |(channel, stream)| {
                            stream.for_each(
                                move |message| -> Box<
                                    dyn Future<Item = (), Error = lapin_futures::Error>
                                        + 'static
                                        + Send,
                                > {
                                    metrics::MESSAGES_RECEIVED_COUNTER
                                        .with_label_values(&[&sftp_source_name_2])
                                        .inc();
                                    debug!("Received message from RabbitMQ");

                                    let deserialize_result: serde_json::Result<SftpDownload> =
                                        serde_json::from_slice(message.data.as_slice());

                                    match deserialize_result {
                                        Ok(command) => {
                                            let download_result = sftp_downloader.handle(&command);

                                            match download_result {
                                                Ok(_) => {
                                                    sender
                                                        .try_send(FileEvent {
                                                            source_name: sftp_source_name_2.clone(),
                                                            path: PathBuf::from(command.path),
                                                        })
                                                        .unwrap();
                                                    Box::new(
                                                        channel
                                                            .basic_ack(message.delivery_tag, false),
                                                    )
                                                }
                                                Err(e) => {
                                                    error!(
                                                        "Error downloading {}: {}",
                                                        &command.path, e
                                                    );
                                                    Box::new(channel.basic_nack(
                                                        message.delivery_tag,
                                                        false,
                                                        false,
                                                    ))
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Error deserializing command: {}", e);
                                            Box::new(channel.basic_nack(
                                                message.delivery_tag,
                                                false,
                                                false,
                                            ))
                                        }
                                    }
                                },
                            )
                        })
                        .and_then(|_| {
                            info!("Consumer stream ended");
                            future::ok(())
                        })
                        .map_err(Error::from)
                })
                .map_err(|e| {
                    error!("{}", e);
                });

            runtime.block_on(stream).unwrap();

            runtime.run().unwrap();
        })
    }

    pub fn handle(&mut self, msg: &SftpDownload) -> Result<(), String> {
        let remote_path = Path::new(&msg.path);

        let local_path = if remote_path.is_absolute() {
            self.local_storage_path
                .join(remote_path.strip_prefix("/").unwrap())
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
            }
            None => {
                info!(
                    "{} downloading '{}' size unknown",
                    self.sftp_source.name, msg.path
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
                    info!(
                        "Created containing directory '{}'",
                        local_path_parent.to_str().unwrap()
                    );
                }
                Err(e) => {
                    return Err(format!(
                        "Error creating containing directory '{}': {}",
                        local_path_parent.to_str().unwrap(),
                        e
                    ));
                }
            }
        }

        let file_create_result = File::create(&local_path);

        let mut local_file = match file_create_result {
            Ok(file) => file,
            Err(e) => {
                return Err(format!(
                    "Could not create file {}: {}",
                    local_path.to_str().unwrap(),
                    e
                ));
            }
        };

        let mut sha256 = Sha256::new();

        let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

        let copy_result = io::copy(&mut tee_reader, &mut local_file);

        match copy_result {
            Ok(bytes_copied) => {
                info!(
                    "{} downloaded '{}', {} bytes",
                    self.sftp_source.name, msg.path, bytes_copied
                );

                let hash = format!("{:x}", sha256.result());

                self.persistence.store(
                    &self.sftp_source.name,
                    &msg.path,
                    i64::try_from(bytes_copied).unwrap(),
                    &hash,
                );

                metrics::FILE_DOWNLOAD_COUNTER_VEC
                    .with_label_values(&[&self.sftp_source.name])
                    .inc();
                metrics::BYTES_DOWNLOADED_COUNTER_VEC
                    .with_label_values(&[&self.sftp_source.name])
                    .inc_by(bytes_copied as i64);

                let remove_after_download = true;

                if remove_after_download {
                    let unlink_result = self.sftp_connection.sftp.unlink(&remote_path);

                    match unlink_result {
                        Ok(_) => {
                            info!("{} removed '{}'", self.sftp_source.name, msg.path);
                        }
                        Err(e) => {
                            error!(
                                "{} error removing '{}': {}",
                                self.sftp_source.name, msg.path, e
                            );
                        }
                    }
                }

                Ok(())
            }
            Err(e) => Err(format!(
                "{} error downloading '{}': {}",
                self.sftp_source.name, msg.path, e
            )),
        }
    }
}
