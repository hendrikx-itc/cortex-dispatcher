use std::convert::TryFrom;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

extern crate failure;

use tokio::sync::mpsc::UnboundedSender;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use retry::{retry, delay::Fixed, OperationResult};

use crate::event::FileEvent;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;

use cortex_core::sftp_connection::SftpConnectionManager;
use cortex_core::SftpDownload;

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
    	stop: Arc<AtomicBool>,
		receiver: Receiver<SftpDownload>,
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

            let mut sftp_downloader = SftpDownloader {
                sftp_source: config.clone(),
                sftp_connection: conn_mgr,
                persistence: persistence,
                local_storage_path: data_dir.clone(),
            };

			let timeout = time::Duration::from_millis(500);

        	while !stop.load(Ordering::Relaxed) {
				let receive_result = receiver.recv_timeout(timeout);

				match receive_result {
					Ok(command) => {
						let retry_policy = Fixed::from_millis(100).take(3);
						let download_result = retry(retry_policy, || {
							match sftp_downloader.handle(&command) {
								Ok(file_event) => OperationResult::Ok(file_event),
								Err(e) => {
									match e.source {
										SftpErrorSource::SshError(ie) => {
											if ie.code() == 0 {
												let new_err = SftpError::new(SftpErrorSource::SshError(ie), "".to_string());
												OperationResult::Retry(new_err)
											} else {
												let new_err = SftpError::new(SftpErrorSource::SshError(ie), "".to_string());
												OperationResult::Err(new_err)
											}
										}
										SftpErrorSource::IoError(_) => OperationResult::Err(e),
										SftpErrorSource::Other => OperationResult::Err(e)
									}
								}
							}
						});

						match download_result {
							Ok(_) => {
								// Notify about new data from this SFTP source
								let send_result = sender
									.try_send(FileEvent {
										source_name: config.name.clone(),
										path: PathBuf::from(command.path),
									});

								match send_result {
									Ok(_) => (),
									Err(e) => {
										error!("Error sending download notification: {}", e);
									}
								}
							}
							Err(e) => {
								error!(
									"Error downloading {}: {}",
									&command.path, e
								);
							}
						}
					},
					Err(e) => {
						match e {
							RecvTimeoutError::Timeout => (),
							RecvTimeoutError::Disconnected => {
								error!("Channel disconnected")
							}
						}
					}
				}
            }

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
				if e.code() == 0 {
					// Reset the connection on an error, so that it will reconnect next time.
					self.sftp_connection.reset();
				}

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
