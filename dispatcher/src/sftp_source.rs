use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

extern crate failure;

use tokio::sync::mpsc::UnboundedSender;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use retry::{retry, OperationResult, delay::Fixed};

use crate::event::FileEvent;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;

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
        stop: Arc<AtomicBool>,
        receiver: Receiver<SftpDownload>,
        config: settings::SftpSource,
        mut sender: UnboundedSender<FileEvent>,
        data_dir: PathBuf,
        persistence: T,
    ) -> thread::JoinHandle<Result<()>> {
        thread::spawn(move || {
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
                local_storage_path: data_dir.clone(),
            };

            let timeout = time::Duration::from_millis(500);

            while !(stop.load(Ordering::Relaxed) && receiver.is_empty()) {
                let receive_result = receiver.recv_timeout(timeout);

                match receive_result {
                    Ok(command) => {
                        let download_result = retry(Fixed::from_millis(1000), || {
                            match sftp_downloader.handle(sftp_connection.clone(), &command) {
                                Ok(file_event) => OperationResult::Ok(file_event),
                                Err(e) => {
                                    match e {
                                        Error(ErrorKind::DisconnectedError, _) => {
                                            let connect_result = SftpConnection::connect_loop(sftp_config.clone(), stop.clone());

                                            match connect_result {
                                                Ok(c) => {
                                                    sftp_connection.replace(c);
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
                                // Notify about new data from this SFTP source
                                sender
                                    .try_send(file_event)
                                    .map_err(|e| Error::with_chain(e, "Error notifying consumers of new file"))?;
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

            Ok(())
        })
    }

    pub fn handle(&mut self, sftp_connection: Arc<RefCell<SftpConnection>>, msg: &SftpDownload) -> Result<FileEvent> {
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

        let (copy_result, hash) = {
            let borrow = sftp_connection.borrow();
            
            let open_result = borrow.sftp.open(&remote_path);

            let mut remote_file = match open_result {
                Ok(remote_file) => remote_file,
                Err(e) => {
                    match e.code() {
                        0 => return Err(ErrorKind::DisconnectedError.into()),
                        2 => return Err(ErrorKind::NoSuchFileError.into()),
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
                .chain_err(|| format!("Error creating local file {}", local_path.to_str().unwrap()))?;

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
