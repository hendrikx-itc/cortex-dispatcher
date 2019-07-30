use std::convert::TryFrom;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;

extern crate failure;

extern crate lapin_futures;
use lapin_futures::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin_futures::types::FieldTable;

use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use retry::retry;
use retry::delay::{Exponential, jitter};

use crate::event::FileEvent;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;

use cortex_core::sftp_connection::SftpConnectionManager;
use cortex_core::SftpDownload;

use failure::Error;

use sha2::{Digest, Sha256};
use tee::TeeReader;

pub struct SftpDownloader<T>
where
    T: Persistence,
{
    pub sftp_source: settings::SftpSource,
    pub sftp_connection: SftpConnectionManager,
    pub persistence: T,
    pub local_storage_path: PathBuf,
}

enum SftpErrorSource {
    SshError(ssh2::Error),
    IoError(std::io::Error),
    Other,
}

pub struct SftpError {
    source: SftpErrorSource,
    msg: String,
}

impl SftpError {
    fn new(source: SftpErrorSource, msg: String) -> SftpError {
        SftpError { source, msg }
    }
}

impl fmt::Display for SftpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            SftpErrorSource::SshError(ref e) => write!(f, "{}: {}", self.msg, e),
            SftpErrorSource::IoError(ref e) => write!(f, "{}: {}", self.msg, e),
            SftpErrorSource::Other => write!(f, "{}", self.msg)
        }
    }
}

impl fmt::Debug for SftpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            SftpErrorSource::SshError(ref e) => e.fmt(f),
            SftpErrorSource::IoError(ref e) => e.fmt(f),
            SftpErrorSource::Other => write!(f, "{}", self.msg)
        }
    }
}

impl std::error::Error for SftpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.source {
            SftpErrorSource::SshError(ref e) => Some(e),
            SftpErrorSource::IoError(ref e) => Some(e),
            SftpErrorSource::Other => None
        }
    }
}

impl<T> SftpDownloader<T>
where
    T: Persistence,
    T: Send,
    T: Clone,
    T: 'static,
{
    pub fn start(
        stop_receiver: oneshot::Receiver<()>,
        amqp_client: lapin_futures::Client,
        config: settings::SftpSource,
        mut sender: UnboundedSender<FileEvent>,
        data_dir: PathBuf,
        persistence: T,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let conn_mgr = SftpConnectionManager::new(
                config.address.clone(),
                config.username.clone(),
                config.password.clone(),
                config.key_file.clone(),
                config.compress,
            );

            let mut runtime = Runtime::new().unwrap();

            let mut sftp_downloader = SftpDownloader {
                sftp_source: config.clone(),
                sftp_connection: conn_mgr,
                persistence: persistence,
                local_storage_path: data_dir.clone(),
            };

            let sftp_source_name = config.name.clone();
            let sftp_source_name_2 = config.name.clone();

            let stream = amqp_client
                .create_channel()
                .map_err(Error::from)
                .and_then(|channel| {

                    let channel_nack = channel.clone();
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
                        .map_err(|e| SftpError::new(SftpErrorSource::Other, format!("{}", e)))
                        .and_then(move |(channel, stream)| {
                            stream
                            .map_err(|e| SftpError::new(SftpErrorSource::Other, format!("Error parsing message: {}", e)))
                            .and_then(move |message| -> Result<(SftpDownload, u64), SftpError> {
                                metrics::MESSAGES_RECEIVED_COUNTER
                                    .with_label_values(&[&sftp_source_name_2])
                                    .inc();
                                debug!("Received message from RabbitMQ");

                                let deserialize_result: serde_json::Result<SftpDownload> = serde_json::from_slice(message.data.as_slice());

                                match deserialize_result {
                                    Ok(sftp_download) => Ok((sftp_download, message.delivery_tag)),
                                    Err(e) => Err(SftpError::new(SftpErrorSource::Other, format!("Error parsing message: {}", e)))
                                }
                            })
                            .and_then(move |(command, delivery_tag)| {
                                let retry_strategy = Exponential::from_millis(100).map(jitter).take(3);

                                retry(retry_strategy, || -> Result<FileEvent, SftpError> {
                                    sftp_downloader.handle(&command)
                                })
                                .map_err(|e| SftpError::new(SftpErrorSource::Other, format!("Error parsing message: {}", e)))
                                .map(move |file_event| (file_event, delivery_tag))

                                //sftp_downloader.handle(&command).map(|file_event| (file_event, delivery_tag))
                            })
                            .and_then(move |(file_event, delivery_tag)|{
                                // Notify about new data from this SFTP source
                                sender
                                    .try_send(file_event)
                                    .map_err(|e| SftpError::new(SftpErrorSource::Other, format!("Error sending file event: {}", e)))
                                    .map(|_| delivery_tag)
                            }) 
                            .and_then(move |delivery_tag| {
                                Box::new(
                                    channel
                                        .basic_ack(delivery_tag, false)
                                        .map_err(|e| SftpError::new(SftpErrorSource::Other, format!("{}", e)))
                                )
                            })
                            .or_else(move |e| {
                                error!("Error downloading: {}", e);

                                Box::new(channel_nack.basic_nack(
                                    100,//delivery_tag,
                                    false,
                                    false,
                                ).map_err(|e| SftpError::new(SftpErrorSource::Other, format!("{}", e))))
                            })
                            .for_each(
                                |_| -> Result<(), SftpError> {
                                    Ok(())
                                }
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

            let stoppable_stream = stream.into_future().select2(stop_receiver.into_future());

            runtime.spawn(
                stoppable_stream
                    .map(|_result| debug!("End SFTP stream"))
                    .map_err(|_e| error!("Error: ")),
            );

            runtime.run().unwrap();

            debug!("SFTP source stream ended");
        })
    }

    pub fn handle(&mut self, msg: &SftpDownload) -> Result<FileEvent, SftpError> {
        let remote_path = Path::new(&msg.path);

        let local_path = if remote_path.is_absolute() {
            self.local_storage_path
                .join(remote_path.strip_prefix("/").unwrap())
        } else {
            self.local_storage_path.join(remote_path)
        };

        match msg.size {
            Some(size) => {
                debug!(
                    "Downloading <{}> '{}' -> '{}' {} bytes",
                    self.sftp_source.name,
                    msg.path,
                    local_path.to_str().unwrap(),
                    size
                );
            }
            None => {
                debug!(
                    "Downloading <{}> '{}' size unknown",
                    self.sftp_source.name, msg.path
                );
            }
        }

        let conn = self.sftp_connection.get();

        let open_result = conn.sftp.open(&remote_path);

        let mut remote_file = match open_result {
            Ok(remote_file) => remote_file,
            Err(e) => {
                // Reset the connection on an error, so that it will reconnect next time.
                self.sftp_connection.reset();

                return Err(SftpError::new(
                    SftpErrorSource::SshError(e),
                    format!("Error opening remote file {}", msg.path),
                ));
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
                    return Err(SftpError::new(
                        SftpErrorSource::IoError(e),
                        format!(
                            "Error creating containing directory '{}'",
                            local_path_parent.to_str().unwrap()
                        ),
                    ));
                }
            }
        }

        let file_create_result = File::create(&local_path);

        let mut local_file = match file_create_result {
            Ok(file) => file,
            Err(e) => {
                return Err(SftpError::new(
                    SftpErrorSource::IoError(e),
                    format!("Error creating local file {}", local_path.to_str().unwrap()),
                ));
            }
        };

        let mut sha256 = Sha256::new();

        let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

        let copy_result = io::copy(&mut tee_reader, &mut local_file);

        match copy_result {
            Ok(bytes_copied) => {
                info!(
                    "Downloaded <{}> '{}' {} bytes",
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

                if msg.remove {
                    let unlink_result = conn.sftp.unlink(&remote_path);

                    match unlink_result {
                        Ok(_) => {
                            debug!("Removed <{}> '{}'", self.sftp_source.name, msg.path);
                        }
                        Err(e) => {
                            error!(
                                "Error removing <{}> '{}': {}",
                                self.sftp_source.name, msg.path, e
                            );
                        }
                    }
                }

                Ok(FileEvent {
                    source_name: self.sftp_source.name.clone(),
                    path: local_path,
                })
            }
            Err(e) => Err(SftpError::new(SftpErrorSource::IoError(e), "".to_string())),
        }
    }
}
