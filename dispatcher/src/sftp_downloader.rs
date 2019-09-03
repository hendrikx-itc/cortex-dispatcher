use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::cell::RefCell;
use std::path::Path;
use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use proctitle;
extern crate failure;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use retry::{retry, OperationResult, delay::Fixed};

use crate::event::FileEvent;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;
use crate::base_types::MessageResponse;
use crate::local_storage::LocalStorage;

use cortex_core::sftp_connection::{SftpConfig, SftpConnection};
use cortex_core::SftpDownload;

use sha2::{Digest, Sha256};
use tee::TeeReader;

error_chain! {
    errors {
        DisconnectedError
        NoSuchFileError
        ConnectInterrupted
    }
}

pub struct SftpDownloader<T>
where
    T: Persistence,
{
    pub sftp_source: settings::SftpSource,
    pub persistence: T,
    pub local_storage: LocalStorage,
}

impl<T> SftpDownloader<T>
where
    T: Persistence,
    T: Send,
    T: Clone,
    T: 'static,
{
    pub fn start(
        stop: Arc<AtomicBool>,
        receiver: Receiver<(u64, SftpDownload)>,
        mut ack_sender: tokio::sync::mpsc::Sender<MessageResponse>,
        config: settings::SftpSource,
        mut sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        local_storage: LocalStorage,
        persistence: T,
    ) -> thread::JoinHandle<Result<()>> {
        thread::spawn(move || {
            proctitle::set_title("sftp_dl");

            let sftp_config = SftpConfig {
                address: config.address.clone(),
                username: config.username.clone(),
                password: config.password.clone(),
                key_file: config.key_file.clone(),
                compress: config.compress
            };

            let connect_result = SftpConnection::connect_loop(sftp_config.clone(), stop.clone());

            let sftp_connection = match connect_result {
                Ok(c) => Arc::new(RefCell::new(c)),
                Err(e) => return Err(Error::with_chain(e, "SFTP connect failed"))
            };

            let mut sftp_downloader = SftpDownloader {
                sftp_source: config.clone(),
                persistence: persistence,
                local_storage: local_storage.clone(),
            };

            let timeout = time::Duration::from_millis(500);

            while !(stop.load(Ordering::Relaxed) && receiver.is_empty()) {
                let receive_result = receiver.recv_timeout(timeout);

                match receive_result {
                    Ok((delivery_tag, command)) => {
                        let download_result = retry(Fixed::from_millis(1000), || {
                            match sftp_downloader.handle(sftp_connection.clone(), &command) {
                                Ok(file_event) => OperationResult::Ok(file_event),
                                Err(e) => {
                                    match e {
                                        Error(ErrorKind::DisconnectedError, _) => {
                                            info!("Sftp connection disconnected, reconnecting");
                                            let connect_result = SftpConnection::connect_loop(sftp_config.clone(), stop.clone());

                                            match connect_result {
                                                Ok(c) => {
                                                    sftp_connection.replace(c);
                                                    info!("Sftp connection reconnected");
                                                    OperationResult::Retry(e)
                                                },
                                                Err(er) => {
                                                    OperationResult::Err(Error::with_chain(er, "Error reconnecting SFTP"))
                                                }
                                            }
                                        },
                                        _ => OperationResult::Err(e)
                                    }
                                }
                            }
                        });

                        match download_result {
                            Ok(file_event) => {
                                let send_result = ack_sender.try_send(MessageResponse::Ack{delivery_tag});

                                match send_result {
                                    Ok(_) => {
                                        debug!("Sent message ack to channel");
                                    },
                                    Err(e) => {
                                        error!("Error sending message ack to channel: {}", e);
                                    }

                                }

                                // Notify about new data from this SFTP source
                                let send_result = sender.try_send(file_event);

                                match send_result {
                                    Ok(_) => {
                                        debug!("Sent SFTP FileEvent to channel");
                                    },
                                    Err(e) => {
                                        error!("Error notifying consumers of new file: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                let send_result = ack_sender.try_send(MessageResponse::Nack{delivery_tag});

                                match send_result {
                                    Ok(_) => {
                                        debug!("Sent message nack to channel");
                                    },
                                    Err(e) => {
                                        error!("Error sending message nack to channel: {}", e);
                                    }

                                }

                                let msg = match e {
                                    retry::Error::<Error>::Operation { error, total_delay: _, tries: _ } => {
                                        let msg_list: Vec<String> = error.iter().map(|sub_err| sub_err.to_string()).collect();
                                        format!("{}", msg_list.join(": "))
                                    },
                                    retry::Error::Internal(int) => {
                                        int
                                    }
                                };

                                error!(
                                    "[E01003] Error downloading '{}': {}",
                                    &command.path, msg
                                );
                            }
                        }
                    },
                    Err(e) => {
                        match e {
                            RecvTimeoutError::Timeout => (),
                            RecvTimeoutError::Disconnected => {
                                error!("[E02005] Channel disconnected");
                                thread::sleep(timeout)
                            }
                        }
                    }
                }
            }

            debug!("SFTP source stream ended");

            Ok(())
        })
    }

    pub fn handle(&mut self, sftp_connection: Arc<RefCell<SftpConnection>>, msg: &SftpDownload) -> Result<FileEvent> {
        let remote_path = Path::new(&msg.path);

        let local_path = self.local_storage.local_path(&self.sftp_source.name, &remote_path, &Path::new("/")).unwrap();

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

        let (copy_result, hash) = {
            let borrow = sftp_connection.borrow();
            
            let open_result = borrow.sftp.open(&remote_path);

            let mut remote_file = match open_result {
                Ok(remote_file) => remote_file,
                Err(e) => {
                    let path_str = remote_path.to_string_lossy();
                    error!("Error opening file '{}': {}", &path_str, e);
                    match e.code() {
                        0 => {
                            return Err(ErrorKind::DisconnectedError.into())
                        },
                        2 => {
                            return Err(ErrorKind::NoSuchFileError.into())
                        },
                        _ => return Err(Error::with_chain(e, "Error opening remote file"))
                    }
                }
            };

            let local_path_parent = local_path.parent().unwrap();

            if !local_path_parent.exists() {
                std::fs::create_dir_all(local_path_parent)
                    .chain_err(||format!("Error creating containing directory '{}'", local_path_parent.to_str().unwrap()))?;

                info!("Created containing directory '{}'", local_path_parent.to_str().unwrap());
            }

            let mut local_file = File::create(&local_path)
                .chain_err(|| format!("Error creating local file '{}'", local_path.to_str().unwrap()))?;

            let mut sha256 = Sha256::new();

            let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

            (
                io::copy(&mut tee_reader, &mut local_file),
                format!("{:x}", sha256.result())
            )
        };

        match copy_result {
            Ok(bytes_copied) => {
                info!(
                    "Downloaded <{}> '{}' {} bytes",
                    self.sftp_source.name, msg.path, bytes_copied
                );

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
                    let unlink_result = sftp_connection.borrow().sftp.unlink(&remote_path);

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
            },
            Err(e) => Err(Error::with_chain(e, "Error copying file")),
        }
    }
}
