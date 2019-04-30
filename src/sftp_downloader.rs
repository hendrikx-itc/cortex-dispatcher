use std::fs::File;
use std::io;
use std::path::Path;
use std::collections::HashMap;
use std::convert::TryFrom;

extern crate inotify;

extern crate actix;
use actix::prelude::*;
use actix::{Actor};

extern crate failure;
extern crate lapin_futures;

use crate::settings;
use crate::sftp_connection::SftpConnection;
use crate::metrics;

use futures::{future, Future};

use tee::TeeReader;
use sha2::{Sha256, Digest};


pub struct SftpDownloader {
    pub config: settings::SftpSource,
    pub sftp_connection: SftpConnection,
    pub db_connection: postgres::Connection,
    pub local_storage_path: String
}

pub struct Download {
    path: String,
    size: Option<u64>,
}

impl Message for Download {
    type Result = bool;
}

impl Handler<Download> for SftpDownloader {
    type Result = bool;

    fn handle(&mut self, msg: Download, _ctx: &mut SyncContext<Self>) -> Self::Result {
        match msg.size {
            Some(size) => {
                info!(
                    "{} downloading '{}' {} bytes",
                    self.config.name,
                    msg.path,
                    size
                );
            },
            None => {
                info!(
                    "{} downloading '{}' size unknown",
                    self.config.name,
                    msg.path
                );
            }
        }

        let remote_path = Path::new(&msg.path);
        let local_path = Path::new(&self.local_storage_path).join(remote_path.file_name().unwrap());

        let open_result = self.sftp_connection.sftp.open(&remote_path);

        let mut remote_file = match open_result {
            Ok(remote_file) => remote_file,
            Err(e) => {
                error!("Error opening remote file {}: {}", msg.path, e);
                return false;
            }
        };

        let mut local_file = File::create(local_path).unwrap();

        let mut sha256 = Sha256::new();

        let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

        let copy_result = io::copy(&mut tee_reader, &mut local_file);

        match copy_result {
            Ok(bytes_copied) => {
                info!("{} downloaded '{}', {} bytes", self.config.name, msg.path, bytes_copied);

                let hash = format!("{:x}", sha256.result());

                self.db_connection.execute(
                    "insert into dispatcher.sftp_download (remote, path, size, hash) values ($1, $2, $3, $4)",
                    &[&self.config.name, &msg.path, &i64::try_from(bytes_copied).unwrap(), &hash]
                ).unwrap();

                metrics::FILE_DOWNLOAD_COUNTER.inc();
                metrics::BYTES_DOWNLOADED_COUNTER.inc_by(bytes_copied as i64);

                let remove_after_download = true;

                if remove_after_download {
                    let unlink_result = self.sftp_connection.sftp.unlink(&remote_path);
                    
                    match unlink_result {
                        Ok(_) => {
                            info!("{} removed '{}'", self.config.name, msg.path);
                        },
                        Err(e) => {
                            info!("{} error removing '{}': {}", self.config.name, msg.path, e);
                        }
                    }
                }
                true
            }
            Err(e) => {
                error!(
                    "{} error downloading '{}': {}",
                    self.config.name, msg.path, e
                );
                false
            }
        }
    }
}

impl Actor for SftpDownloader {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("SftpDownloader actor started");
    }
}


pub struct SftpDownloadDispatcher {
    pub downloaders_map: HashMap<String, Addr<SftpDownloader>>,
}

impl SftpDownloadDispatcher {
    pub fn dispatch_download(&mut self, sftp_source: &str, size: Option<u64>, path: String) {
        let result = self.downloaders_map.get(sftp_source);

        match result {
            Some(downloader) => {
                let result = downloader.send(Download {path, size});

                Arbiter::spawn(result.then(|_r| {
                    future::result(Ok(()))
                }));
            },
            None => {
                info!("no SFTP source matching '{}'", sftp_source);
            }
        }
    }
}
