use std::convert::TryFrom;
use std::path::Path;
use std::{time};
use std::net::{TcpStream, ToSocketAddrs};

use postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio::fs::File;
use tokio_postgres::Socket;

extern crate failure;

use crate::event::FileEvent;
use crate::metrics;
use crate::persistence::Persistence;
use crate::settings;
use crate::base_types::MessageResponse;
use crate::local_storage::LocalStorage;

use cortex_core::SftpDownload;

use sha2::{Digest, Sha256};
//use tee::TeeReader;

use async_io::Async;
use async_ssh2_lite::{AsyncSession};

use chrono::{Utc, DateTime, NaiveDateTime};

error_chain! {
    errors {
        DisconnectedError
        NoSuchFileError
        ConnectInterrupted
        PersistenceError
    }
}

#[derive(Clone)]
pub struct SftpDownloader<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send + postgres::tls::MakeTlsConnect<postgres::Socket>,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub sftp_source: settings::SftpSource,
    pub persistence: Persistence<T>,
    pub local_storage: LocalStorage<T>,
}

impl<T> SftpDownloader<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send + postgres::tls::MakeTlsConnect<postgres::Socket>,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn start(
        mut receiver: tokio::sync::mpsc::Receiver<(u64, SftpDownload)>,
        ack_sender: tokio::sync::mpsc::Sender<MessageResponse>,
        config: settings::SftpSource,
        sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        local_storage: LocalStorage<T>,
        persistence: Persistence<T>,
    ) {
        let addr = ToSocketAddrs::to_socket_addrs(&config.address).unwrap().next().unwrap();

        let stream = match Async::<TcpStream>::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("Could not connect to SFTP server at {}: {}", &addr, e);
                return
            }
        };

        let mut session = match AsyncSession::new(stream, None) {
            Ok(s) => s,
            Err(e) => {
                error!("Could not setup session to SFTP server at {}: {}", &addr, e);
                return
            }
        };
    
        match session.handshake().await {
            Ok(_) => {},
            Err(e) => {
                error!("Error during handshake with {}: {}", &addr, e);
                return
            }
        }
    
        match session.userauth_agent(&config.username).await {
            Ok(_) => {},
            Err(e) => {
                error!("Error authenticating with {}: {}", &addr, e);
                return
            }
        }
    
        if !session.authenticated() {
            error!("Unknown error authenticating with {}", &addr);
            return
        }

        let sftp_downloader = SftpDownloader {
            sftp_source: config.clone(),
            persistence: persistence,
            local_storage: local_storage.clone(),
        };

        let timeout = time::Duration::from_millis(500);

        // Take SFTP download commands from the channel until it is closed
        while let Some((delivery_tag, command)) = receiver.recv().await {
            let download_result = sftp_downloader.handle(&session, &command).await;

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
                    let send_result = ack_sender.send(MessageResponse::Nack{delivery_tag}).await;

                    match send_result {
                        Ok(_) => {
                            debug!("Sent message nack to channel");
                        },
                        Err(e) => {
                            error!("Error sending message nack to channel: {}", e);
                        }
                    }

                    error!(
                        "[E01003] Error downloading '{}': {}",
                        &command.path, e
                    );
                }
            }
        }
    }

    pub async fn handle<S>(&self, session: &AsyncSession<S>, msg: &SftpDownload) -> Result<FileEvent> {
        let remote_path = Path::new(&msg.path);

        let sftp = session.sftp().await.unwrap();

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
            let open_result = sftp.open(&remote_path).await;

            let mut remote_file = match open_result {
                Ok(remote_file) => remote_file,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::BrokenPipe => {
                            // unknown error, probably a fault in the SFTP connection
                            return Err(ErrorKind::DisconnectedError.into())
                        },
                        std::io::ErrorKind::NotFound => {
                            let delete_result = self.persistence.delete_sftp_download_file(msg.id).await;

                            match delete_result {
                                Ok(_) => return Err(ErrorKind::NoSuchFileError.into()),
                                Err(e) => return Err(Error::with_chain(e, "Error removing record of non-existent remote file"))
                            }
                        },
                    }
                }
            };

            let stat_result = sftp.stat(&remote_path).await;

            let stat = match stat_result {
                Ok(s) => s,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::BrokenPipe => {
                            // unknown error, probably a fault in the SFTP connection
                            return Err(ErrorKind::DisconnectedError.into())
                        },
                        _ => return Err(Error::with_chain(e, "Error retrieving stat for remote file"))
                    }
                }
            };

            if let Some(local_path_parent) = local_path.parent() {
                if !local_path_parent.exists() {
                    std::fs::create_dir_all(local_path_parent)
                        .chain_err(||format!("Error creating containing directory '{}'", local_path_parent.to_string_lossy()))?;
    
                    info!("Created containing directory '{}'", local_path_parent.to_string_lossy());
                }    
            }

            let mut local_file = File::create(&local_path).await
                .map_err(|e| format!("Error creating local file '{}': {}", local_path.to_string_lossy(), e))?;

            let mut sha256 = Sha256::new();

            //let mut tee_reader = TeeReader::new(&mut remote_file, &mut sha256);

            let bytes_copied: u64 = 0;
            let copy_result: std::io::Result<u64> = Ok(0);

            (
                copy_result, //futures_util::io::copy(&mut remote_file, &mut local_file).await;
                format!("{:x}", sha256.finalize()),
                stat
            )
        };

        match copy_result {
            Ok(bytes_copied) => {
                info!(
                    "Downloaded <{}> '{}' {} bytes",
                    self.sftp_source.name, msg.path, bytes_copied
                );

                let file_size = match i64::try_from(bytes_copied) {
                    Ok(size) => size,
                    Err(e) => return Err(Error::with_chain(e, "Error converting bytes copied to i64"))
                };

                let mtime = match stat.mtime {
                    Some(t) => t,
                    None => 0
                };

				let sec = match i64::try_from(mtime) {
                    Ok(s) => s,
                    Err(e) => return Err(Error::with_chain(e, "Error converting mtime to i64"))
                };
				let nsec = 0;

                let modified = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(sec, nsec), Utc);

                let file_id = match self.persistence.insert_file(
                    &self.sftp_source.name, &local_path.to_string_lossy(), &modified, file_size, Some(hash)
                ).await {
                    Ok(id) => id,
                    Err(e) => return Err(ErrorKind::PersistenceError.into())
                };

                let set_result = self.persistence.set_sftp_download_file(msg.id, file_id).await;

                if let Err(_) = set_result {
                    return Err(ErrorKind::PersistenceError.into())
                }

                metrics::FILE_DOWNLOAD_COUNTER_VEC
                    .with_label_values(&[&self.sftp_source.name])
                    .inc();
                metrics::BYTES_DOWNLOADED_COUNTER_VEC
                    .with_label_values(&[&self.sftp_source.name])
                    .inc_by(bytes_copied as u64);

                if msg.remove {
                    let unlink_result = sftp.unlink(&remote_path).await;

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
                    file_id: file_id,
                    source_name: self.sftp_source.name.clone(),
                    path: local_path,
                })
            },
            Err(e) => Err(Error::with_chain(e, "Error copying file")),
        }
    }
}
