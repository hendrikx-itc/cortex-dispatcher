use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::{Sender, SendTimeoutError};
use log::{debug, error, info};

use retry::{retry, delay::Fixed, OperationResult};

extern crate chrono;
use chrono::prelude::*;

use proctitle;

use cortex_core::sftp_connection::SftpConnection;
use cortex_core::SftpDownload;

use crate::metrics;
use crate::settings::SftpSource;


error_chain! {
    errors { DisconnectedError }
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
) -> thread::JoinHandle<()> {
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

		let sftp_source_clone = sftp_source.clone();

		let connect = move || {
			loop {
				let conn_result = SftpConnection::new(
					&sftp_source_clone.address.clone(),
					&sftp_source_clone.username.clone(),
					sftp_source_clone.password.clone(),
					sftp_source_clone.key_file.clone(),
					false,
				);

				match conn_result {
					Ok(c) => break c,
					Err(e) => error!("Could not connect: {}", e),
				}

				thread::sleep(time::Duration::from_millis(1000));
			}
		};

        let mut sftp_connection = connect();

        let scan_interval = time::Duration::from_millis(sftp_source.scan_interval);
        let mut next_scan = time::Instant::now();

        while !stop.load(Ordering::Relaxed) {
            if time::Instant::now() > next_scan {
                next_scan += scan_interval;

                let scan_start = time::Instant::now();
                debug!("Started scanning {}", &sftp_source.name);

                let scan_result = scan_source(&stop, &sftp_source, &sftp_connection, &conn, &mut sender);

				match scan_result {
					Ok(_) => (),
					Err(e) => {
						match e {
							Error(ErrorKind::DisconnectedError, _) => {
								sftp_connection = connect();
							},
							_ => ()
						}
					}
				}

                let scan_end = time::Instant::now();

                let scan_duration = scan_end.duration_since(scan_start);

                debug!(
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
        }
    })
}

fn scan_source(stop: &Arc<AtomicBool>, sftp_source: &SftpSource, sftp_connection: &SftpConnection, conn: &postgres::Connection, sender: &mut Sender<SftpDownload>) -> Result<()> {
    scan_directory(stop, sftp_source, &Path::new(&sftp_source.directory), sftp_connection, conn, sender)
}

fn scan_directory(stop: &Arc<AtomicBool>, sftp_source: &SftpSource, directory: &Path, sftp_connection: &SftpConnection, conn: &postgres::Connection, sender: &mut Sender<SftpDownload>) -> Result<()> {
    debug!("Directory scan started for {}", &directory.to_str().unwrap());

    let read_result = sftp_connection
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
            let scan_result = scan_directory(stop, sftp_source, &dir, sftp_connection, conn, sender);

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
                debug!(" - {} - matches!", path_str);

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
                            Ok(v) => OperationResult::Ok(v),
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
