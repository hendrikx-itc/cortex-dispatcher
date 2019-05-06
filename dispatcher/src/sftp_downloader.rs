use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::thread;

extern crate inotify;

extern crate failure;
extern crate lapin_futures;

use crate::settings;
use crate::metrics;

use cortex_core::sftp_connection::SftpConnection;

use tee::TeeReader;
use sha2::{Sha256, Digest};


pub struct SftpDownloader {
    pub sftp_source: settings::SftpSource,
    pub sftp_connection: SftpConnection,
    pub db_connection: postgres::Connection,
    pub local_storage_path: PathBuf
}

#[derive(Debug)]
pub struct Download {
    path: String,
    size: Option<u64>,
}

impl SftpDownloader {
    pub fn handle(&mut self, msg: Download) -> bool {
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
                error!("Error opening remote file {}: {}", msg.path, e);
                return false;
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
                    error!("Error creating containing directory '{}': {}", local_path_parent.to_str().unwrap(), e);
                    return false;
                }
            }
        }

        let file_create_result = File::create(&local_path);

        let mut local_file = match file_create_result {
            Ok(file) => file,
            Err(e) => {
                error!("Could not create file {}: {}", local_path.to_str().unwrap(), e);
                return false;
            }
        };

        let mut sha256 = Sha256::new();

        let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

        let copy_result = io::copy(&mut tee_reader, &mut local_file);

        match copy_result {
            Ok(bytes_copied) => {
                info!("{} downloaded '{}', {} bytes", self.sftp_source.name, msg.path, bytes_copied);

                let hash = format!("{:x}", sha256.result());

                self.db_connection.execute(
                    "insert into dispatcher.sftp_download (remote, path, size, hash) values ($1, $2, $3, $4)",
                    &[&self.sftp_source.name, &msg.path, &i64::try_from(bytes_copied).unwrap(), &hash]
                ).unwrap();

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
                true
            }
            Err(e) => {
                error!(
                    "{} error downloading '{}': {}",
                    self.sftp_source.name, msg.path, e
                );
                false
            }
        }
    }
}

pub struct SftpDownloadDispatcher {
    pub downloaders_map: HashMap<String, (crossbeam_channel::Sender<Download>, thread::JoinHandle<()>)>,
}

impl SftpDownloadDispatcher {
    pub fn dispatch_download(&mut self, sftp_source: &str, size: Option<u64>, path: String) {
        let result = self.downloaders_map.get(sftp_source);

        match result {
            Some((sender, _)) => {
                let result = sender.send(Download {path, size});
                result.unwrap();
            },
            None => {
                warn!("no SFTP source matching '{}'", sftp_source);
            }
        }
    }
}
