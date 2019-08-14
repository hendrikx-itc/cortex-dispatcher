use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::RefCell;

use crossbeam_channel::{Sender, SendTimeoutError};
use log::{debug, error, info};

use retry::{retry, delay::Fixed, OperationResult};

extern crate chrono;
use chrono::prelude::*;

use proctitle;

use cortex_core::sftp_connection::{SftpConfig, SftpConnection};
use cortex_core::SftpDownload;

use crate::metrics;
use crate::settings::SftpSource;


error_chain! {
    errors {
        DisconnectedError
        ConnectInterrupted
    }
}


/// Starts a new thread with an SFTP scanner for the specified source.
///
/// For encountered files to be downloaded, a message is placed on a channel
/// using the provided sender.
///
/// A thread is used instead of an async Tokio future because the library used
/// for the SFTP connection is not thread safe.
pub fn start_scanner(
    stop: Arc<AtomicBool>,
    mut sender: Sender<SftpDownload>,
    db_url: String,
    sftp_source: SftpSource,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        proctitle::set_title(format!("sftp-scanner {}", &sftp_source.name));

        let conn_result = postgres::Connection::connect(db_url, postgres::TlsMode::None);

        let conn = match conn_result {
            Ok(c) => {
                info!("Connected to database");
                c
            }
            Err(e) => {
                error!("Error connecting to database: {}", e);
                ::std::process::exit(2);
            }
        };

        let sftp_config = SftpConfig {
            address: sftp_source.address.clone(),
            username: sftp_source.username.clone(),
            password: sftp_source.password.clone(),
            key_file: sftp_source.key_file.clone(),
            compress: false,
        };

        let connect_result = SftpConnection::connect_loop(sftp_config.clone(), stop.clone());

        let sftp_connection = match connect_result {
            Ok(c) => Arc::new(RefCell::new(c)),
            Err(e) => return Err(Error::with_chain(e, "Error reconnecting SFTP"))
        };

        let scan_interval = time::Duration::from_millis(sftp_source.scan_interval);
        let mut next_scan = time::Instant::now();

        while !stop.load(Ordering::Relaxed) {
            if time::Instant::now() > next_scan {
                // Increase next_scan until it is past now, because it can
                // happen that the process has stalled on SFTP reconnect,
                // causing a number of scheduled scan misses.
                while next_scan < time::Instant::now() {
                    next_scan += scan_interval;
                }

                let scan_start = time::Instant::now();
                info!("Started scanning {}", &sftp_source.name);

                let scan_result = retry(Fixed::from_millis(1000), || {
                    match scan_source(&stop, &sftp_source, sftp_connection.clone(), &conn, &mut sender) {
                        Ok(v) => OperationResult::Ok(v),
                        Err(e) => {
                            match e {
                                Error(ErrorKind::DisconnectedError, _) => {
                                    let connect_result = SftpConnection::connect_loop(sftp_config.clone(), stop.clone());

                                    match connect_result {
                                        Ok(c) => {
                                            sftp_connection.replace(c);
                                            OperationResult::Retry(Error::with_chain(e, "Reconnected SFTP"))
                                        },
                                        Err(er) => {
                                            OperationResult::Err(Error::with_chain(er, "Error reconnecting SFTP"))
                                        }
                                    }
                                },
                                _ => {
                                    let msg = format!("Unexpected error: {}", &e);
                                    OperationResult::Err(Error::with_chain(e, msg))
                                }
                            }
                        }
                    }
                });

                match scan_result {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            "Error scanning {}: {}",
                            &sftp_source.name, e
                        );
                    }
                }

                let scan_end = time::Instant::now();

                let scan_duration = scan_end.duration_since(scan_start);

                info!(
                    "Finished scanning {} in {} ms",
                    &sftp_source.name,
                    scan_duration.as_millis()
                );

                metrics::DIR_SCAN_COUNTER
                    .with_label_values(&[&sftp_source.name])
                    .inc();
                metrics::DIR_SCAN_DURATION
                    .with_label_values(&[&sftp_source.name])
                    .inc_by(scan_duration.as_millis() as i64);
            } else {
                thread::sleep(time::Duration::from_millis(200));
            }
        };

        Ok(())
    })
}

fn scan_source(stop: &Arc<AtomicBool>, sftp_source: &SftpSource, sftp_connection: Arc<RefCell<SftpConnection>>, conn: &postgres::Connection, sender: &mut Sender<SftpDownload>) -> Result<()> {
    scan_directory(stop, sftp_source, &Path::new(&sftp_source.directory), sftp_connection, conn, sender)
}

fn scan_directory(stop: &Arc<AtomicBool>, sftp_source: &SftpSource, directory: &Path, sftp_connection: Arc<RefCell<SftpConnection>>, conn: &postgres::Connection, sender: &mut Sender<SftpDownload>) -> Result<()> {
    debug!("Directory scan started for {}", &directory.to_str().unwrap());

    let read_result = sftp_connection.borrow()
        .sftp
        .readdir(directory);

    let paths = match read_result {
        Ok(paths) => paths,
        Err(e) => {
            if e.code() == 0 {
                return Err(ErrorKind::DisconnectedError.into());
            } else {
                return Err(Error::with_chain(e, "could not read directory"));
            }
        }
    };

    for (path, stat) in paths {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let file_name = path.file_name().unwrap().to_str().unwrap();

        if stat.is_dir() {
            let mut dir = PathBuf::from(directory);
            dir.push(&file_name);
            let scan_result = scan_directory(stop, sftp_source, &dir, sftp_connection.clone(), conn, sender);

            match scan_result {
                Ok(_) => (),
                Err(e) => {
                    match e {
                        Error(ErrorKind::DisconnectedError, _) => return Err(e),
                        _ => ()
                    }
                }
            }
        } else {
            let file_size: u64 = stat.size.unwrap();

            let cast_result = i64::try_from(file_size);

            let file_size_db: i64 = match cast_result {
                Ok(size) => size,
                Err(e) => {
                    error!("Could not convert file size to type that can be stored in database: {}", e);
                    continue;
                }
            };

            let path_str = path.to_str().unwrap().to_string();

            if sftp_source.regex.is_match(file_name) {
                debug!("'{}' - matches", path_str);

                let file_requires_download =
                    if sftp_source.deduplicate {
                        let query_result = conn.query(
                            "select 1 from sftp_scanner.scan where remote = $1 and path = $2 and size = $3",
                            &[&sftp_source.name, &path_str, &file_size_db]
                        );

                        match query_result {
                            Ok(rows) => {
                                if rows.is_empty() {
                                                        
                                    let execute_result = conn.execute(
                                        "insert into sftp_scanner.scan (remote, path, size) values ($1, $2, $3)",
                                        &[&sftp_source.name, &path_str, &file_size_db]
                                    );

                                    match execute_result {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("Error inserting record: {}", e);
                                        }
                                    };
                                }

                                rows.is_empty()
                            },
                            Err(e) => {
                                error!("Error querying database: {}", e);
                                false
                            }
                        }
                    } else {
                        true
                    };

                if file_requires_download {
                    let command = SftpDownload {
                        created: Utc::now(),
                        size: stat.size,
                        sftp_source: sftp_source.name.clone(),
                        path: path_str.clone(),
                        remove: sftp_source.remove
                    };

                    let retry_policy = Fixed::from_millis(100);
                    let send_timeout = time::Duration::from_millis(1000);

                    let send_result = retry(retry_policy, || {
                        let result = sender.send_timeout(command.clone(), send_timeout);

                        match result {
                            Ok(v) => {
                                debug!("Sent message {} on channel", command);
                                OperationResult::Ok(v)
                            },
                            Err(e) => {
                                match e {
                                    SendTimeoutError::Timeout(timeout) => OperationResult::Retry(timeout),
                                    SendTimeoutError::Disconnected(timeout) => OperationResult::Err(timeout)
                                }
                            }
                        }
                    });

                    match send_result {
                        Ok(_) => (),
                        Err(e) => error!("Error sending download message on channel: {:?}", e)
                    }
                } else {
                    debug!("{} already encountered {}", sftp_source.name, path_str);
                }
            } else {
                debug!(" - {} - no match", path_str);
            }
        }
    }

    Ok(())
}
