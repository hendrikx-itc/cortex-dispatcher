use std::convert::TryFrom;
use std::fs::{rename, File};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread, time};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use retry::{delay::Fixed, retry, OperationResult};

use crate::base_types::MessageResponse;
use crate::event::FileEvent;
use crate::local_storage::LocalStorage;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;

use cortex_core::sftp_connection::SftpConfig;
use cortex_core::SftpDownload;

use sha2::{Digest, Sha256};
use tee::TeeReader;

use chrono::{DateTime, NaiveDateTime, Utc};

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
        ack_sender: tokio::sync::mpsc::Sender<MessageResponse>,
        config: settings::SftpSource,
        sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        local_storage: LocalStorage<T>,
        persistence: T,
    ) -> thread::JoinHandle<Result<()>> {
        thread::spawn(move || -> Result<()> {
            proctitle::set_title("sftp_dl");

            let sftp_config = SftpConfig {
                address: config.address.clone(),
                username: config.username.clone(),
                password: config.password.clone(),
                key_file: config.key_file.clone(),
                compress: config.compress,
            };

            let mut session = sftp_config
                .connect_loop(stop.clone())
                .map_err(|e| Error::with_chain(e, "SFTP connect failed"))?;

            let mut sftp = session
                .sftp()
                .map_err(|e| Error::with_chain(e, "SFTP connect failed"))?;

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
                            match sftp_downloader.handle(&sftp, &command) {
                                Ok(file_event) => OperationResult::Ok(file_event),
                                Err(e) => match e {
                                    Error(ErrorKind::DisconnectedError, _) => {
                                        info!("Sftp connection disconnected, reconnecting");
                                        session = match sftp_config.connect_loop(stop.clone()) {
                                            Ok(s) => s,
                                            Err(e) => {
                                                return OperationResult::Err(Error::with_chain(
                                                    e,
                                                    "SFTP connect failed",
                                                ))
                                            }
                                        };

                                        sftp = match session.sftp() {
                                            Ok(s) => s,
                                            Err(e) => {
                                                return OperationResult::Err(Error::with_chain(
                                                    e,
                                                    "SFTP connect failed",
                                                ))
                                            }
                                        };

                                        info!("Sftp connection reconnected");
                                        OperationResult::Retry(e)
                                    }
                                    _ => OperationResult::Err(e),
                                },
                            }
                        });

                        match download_result {
                            Ok(file_event) => {
                                let send_result =
                                    ack_sender.try_send(MessageResponse::Ack { delivery_tag });

                                match send_result {
                                    Ok(_) => {
                                        debug!("Sent message ack to channel");
                                    }
                                    Err(e) => {
                                        error!("Error sending message ack to channel: {}", e);
                                    }
                                }

                                if let Some(f) = file_event {
                                    // Notify about new data from this SFTP source
                                    let send_result = sender.send(f);

                                    match send_result {
                                        Ok(_) => {
                                            debug!("Sent SFTP FileEvent to channel");
                                        }
                                        Err(e) => {
                                            error!("Error notifying consumers of new file: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let send_result =
                                    ack_sender.try_send(MessageResponse::Nack { delivery_tag });

                                match send_result {
                                    Ok(_) => {
                                        debug!("Sent message nack to channel");
                                    }
                                    Err(e) => {
                                        error!("Error sending message nack to channel: {}", e);
                                    }
                                }

                                error!("[E01003] Error downloading '{}': {}", &command.path, e);
                            }
                        }
                    }
                    Err(e) => {
                        match e {
                            RecvTimeoutError::Timeout => (),
                            RecvTimeoutError::Disconnected => {
                                // If the stop flag was set, the other side of the channel was
                                // dropped because of that, otherwise return an error
                                if stop.load(Ordering::Relaxed) {
                                    return Ok(());
                                } else {
                                    error!("[E02005] SFTP download command channel receiver disconnected");

                                    return Err(Error::with_chain(e, "[E02005] SFTP download command channel receiver disconnected"));
                                }
                            }
                        }
                    }
                }
            }

            debug!("SFTP source stream '{}' ended", config.name);

            Ok(())
        })
    }

    pub fn handle(&mut self, sftp: &ssh2::Sftp, msg: &SftpDownload) -> Result<Option<FileEvent>> {
        let remote_path = Path::new(&msg.path);

        let path_prefix = Path::new("");

        let local_path = self
            .local_storage
            .local_path(&self.sftp_source.name, &remote_path, &Path::new("/"))
            .map_err(|e| Error::with_chain(e, "Could not localize path"))?;

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

        let open_result = sftp.open(&remote_path);

        let mut remote_file = match open_result {
            Ok(remote_file) => remote_file,
            Err(e) => {
                match e.code() {
                    ssh2::ErrorCode::Session(_) => {
                        // Probably a fault in the SFTP connection
                        return Err(ErrorKind::DisconnectedError.into());
                    }
                    ssh2::ErrorCode::SFTP(2) => {
                        let delete_result = self.persistence.delete_sftp_download_file(msg.id);

                        match delete_result {
                            Ok(_) => return Err(ErrorKind::NoSuchFileError.into()),
                            Err(e) => {
                                return Err(Error::with_chain(
                                    e,
                                    "Error removing record of non-existent remote file",
                                ))
                            }
                        }
                    }
                    _ => return Err(Error::with_chain(e, "Error opening remote file")),
                }
            }
        };

        let stat = remote_file.stat().map_err(|e| match e.code() {
            ssh2::ErrorCode::Session(_) => {
                // Probably a fault in the SFTP connection
                ErrorKind::DisconnectedError.into()
            }
            _ => Error::with_chain(e, "Error retrieving stat for remote file"),
        })?;

        let mtime = stat.mtime.unwrap_or(0);

        let sec = i64::try_from(mtime)
            .map_err(|e| Error::with_chain(e, "Error converting mtime to i64"))?;
        let nsec = 0;

        let modified =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_opt(sec, nsec).unwrap(), Utc);

        let file_info_result = self
            .local_storage
            .get_file_info(&msg.sftp_source, &remote_path, &path_prefix)
            .map_err(|e| {
                Error::with_chain(e, "Could not get file information from internal storage")
            })?;

        // Opportunity for duplicate check without hash check
        if let Some(file_info) = &file_info_result {
            // See if a deduplication check is configured
            if let settings::Deduplication::Check(check) = &self.sftp_source.deduplication {
                // Only check now if no hash check is required, because that is not calculated
                // yet
                if !check.hash {
                    if check.equal(&file_info, stat.size.unwrap(), modified, None) {
                        // A file with the same name, modified timestamp and/or size was already
                        // downloaded, so assume that it is the same and skip.
                        return Ok(None);
                    }
                }
            }
        }

        if let Some(local_path_parent) = local_path.parent() {
            if !local_path_parent.exists() {
                std::fs::create_dir_all(local_path_parent).map_err(|e| {
                    format!(
                        "Error creating containing directory '{}': {}",
                        local_path_parent.to_string_lossy(),
                        e
                    )
                })?;

                info!(
                    "Created containing directory '{}'",
                    local_path_parent.to_string_lossy()
                );
            }
        }

        // Construct a temporary file name with the extension '.part'
        let mut local_path_part = local_path.as_os_str().to_os_string();
        local_path_part.push(".part");

        let mut local_file_part = File::create(&local_path_part).map_err(|e| {
            format!(
                "Error creating local file part '{}': {}",
                local_path.to_string_lossy(),
                e
            )
        })?;

        let mut sha256 = Sha256::new();

        let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

        let copy_result = io::copy(&mut tee_reader, &mut local_file_part);
        let hash = format!("{:x}", sha256.finalize());

        if let Some(file_info) = &file_info_result {
            // See if a deduplication check is configured
            if let settings::Deduplication::Check(check) = &self.sftp_source.deduplication {
                if check.equal(&file_info, stat.size.unwrap(), modified, Some(hash.clone())) {
                    // A file with the same name, modified timestamp, size and/or hash was already
                    // downloaded, so assume that it is the same and skip.
                    return Ok(None);
                }
            }
        }

        let bytes_copied = copy_result.map_err(|e| Error::with_chain(e, "Error copying file"))?;

        info!(
            "Downloaded <{}> '{}' {} bytes",
            self.sftp_source.name, msg.path, bytes_copied
        );

        // Rename the file to its regular name
        rename(&local_path_part, &local_path)
            .map_err(|e| Error::with_chain(e, "Error renaming part to its regular name"))?;

        let file_size = i64::try_from(bytes_copied)
            .map_err(|e| Error::with_chain(e, "Error converting bytes copied to i64"))?;

        let file_id = match self.persistence.insert_file(
            &self.sftp_source.name,
            &local_path.to_string_lossy(),
            &modified,
            file_size,
            Some(hash.clone()),
        ) {
            Ok(id) => id,
            Err(_) => return Err(ErrorKind::PersistenceError.into()),
        };

        self.persistence
            .set_sftp_download_file(msg.id, file_id)
            .map_err(|e| Error::with_chain(e, "Error updating STFP download information"))?;

        metrics::FILE_DOWNLOAD_COUNTER_VEC
            .with_label_values(&[&self.sftp_source.name])
            .inc();
        metrics::BYTES_DOWNLOADED_COUNTER_VEC
            .with_label_values(&[&self.sftp_source.name])
            .inc_by(bytes_copied as u64);

        if msg.remove {
            let unlink_result = sftp.unlink(&remote_path);

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

        Ok(Some(FileEvent {
            file_id: file_id,
            source_name: self.sftp_source.name.clone(),
            path: local_path,
            hash: hash,
        }))
    }
}
