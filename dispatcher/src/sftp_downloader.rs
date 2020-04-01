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

use chrono::{Utc, DateTime, NaiveDateTime};

error_chain! {
    errors {
        DisconnectedError
        NoSuchFileError
        ConnectInterrupted
        PersistenceError
    }
}

pub struct SftpDownloader<T>
where
    T: Persistence,
{
    pub sftp_source: settings::SftpSource,
    pub persistence: T,
    pub local_storage: LocalStorage<T>,
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
        sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        local_storage: LocalStorage<T>,
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
                Ok(c) => {
                    debug!("SFTP connection to {} established", sftp_config.address);
                    Arc::new(RefCell::new(c))
                },
                Err(e) => return Err(Error::with_chain(e, "SFTP connect failed"))
            };

            let mut sftp_downloader = SftpDownloader {
                sftp_source: config.clone(),
                persistence: persistence,
                local_storage: local_storage.clone(),
            };

            let timeout = time::Duration::from_millis(500);

            // Take SFTP download commands from the queue until the stop flag is set and
            // the command channel is empty.
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
                                let send_result = sender.send(file_event);

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
                                        msg_list.join(": ").to_string()
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

        let localize_result = self.local_storage.local_path(&self.sftp_source.name, &remote_path, &Path::new("/"));

        let local_path = match localize_result {
            Ok(p) => p,
            Err(e) => {
                return Err(Error::with_chain(e, "Could not localize path"))
            }
        };

        match msg.size {
            Some(size) => {
                debug!(
                    "Downloading <{}> '{}' -> '{}' {} bytes",
                    self.sftp_source.name,
                    msg.path,
                    local_path.to_string_lossy(),
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

        let (copy_result, hash, stat) = {
            let borrow = sftp_connection.borrow();

            let stat_result = borrow.sftp.stat(&remote_path);

            let stat = match stat_result {
                Ok(s) => s,
                Err(e) => {
                    match e.code() {
                        0 => {
                            // unknown error, probably a fault in the SFTP connection
                            return Err(ErrorKind::DisconnectedError.into())
                        },
                        _ => return Err(Error::with_chain(e, "Error retrieving stat for remote file"))
                    }
                }
            };
            
            let open_result = borrow.sftp.open(&remote_path);

            let mut remote_file = match open_result {
                Ok(remote_file) => remote_file,
                Err(e) => {
                    match e.code() {
                        0 => {
                            // unknown error, probably a fault in the SFTP connection
                            return Err(ErrorKind::DisconnectedError.into())
                        },
                        2 => {
                            let delete_result = self.persistence.delete_sftp_download_file(msg.id);

                            match delete_result {
                                Ok(_) => return Err(ErrorKind::NoSuchFileError.into()),
                                Err(e) => return Err(Error::with_chain(e, "Error removing record of non-existent remote file"))
                            }
                        },
                        _ => return Err(Error::with_chain(e, "Error opening remote file"))
                    }
                }
            };

            let local_path_parent = local_path.parent().unwrap();

            if !local_path_parent.exists() {
                std::fs::create_dir_all(local_path_parent)
                    .chain_err(||format!("Error creating containing directory '{}'", local_path_parent.to_string_lossy()))?;

                info!("Created containing directory '{}'", local_path_parent.to_string_lossy());
            }

            let mut local_file = File::create(&local_path)
                .chain_err(|| format!("Error creating local file '{}'", local_path.to_string_lossy()))?;

            let mut sha256 = Sha256::new();

            let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

            (
                io::copy(&mut tee_reader, &mut local_file),
                format!("{:x}", sha256.result()),
                stat
            )
        };

        match copy_result {
            Ok(bytes_copied) => {
                info!(
                    "Downloaded <{}> '{}' {} bytes",
                    self.sftp_source.name, msg.path, bytes_copied
                );

                let file_size = i64::try_from(bytes_copied).unwrap();

				let sec = i64::try_from(stat.mtime.unwrap()).unwrap();
				let nsec = 0;

                let modified = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(sec, nsec), Utc);

                let file_id = self.persistence.insert_file(
                    &self.sftp_source.name, &local_path.to_string_lossy(), &modified, file_size, Some(hash)
                ).unwrap();

                let set_result = self.persistence.set_sftp_download_file(msg.id, file_id);

                match set_result {
                    Ok(_) => {},
                    Err(_) => return Err(ErrorKind::PersistenceError.into()),
                }

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
