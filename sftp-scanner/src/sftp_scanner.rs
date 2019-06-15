use std::convert::TryFrom;
use std::path::Path;
use std::{thread, time};

use futures::sync::mpsc::Sender;
use log::{debug, error, info};

extern crate chrono;
use chrono::prelude::*;

use cortex_core::sftp_connection::SftpConnection;
use cortex_core::SftpDownload;

use crate::metrics;
use crate::settings::SftpSource;

/// Starts a new thread with an SFTP scanner for the specified source.
///
/// For encountered files to be downloaded, a message is placed on a channel
/// using the provided sender.
///
/// A thread is used instead of an async Tokio future because the library used
/// for the SFTP connection is not thread safe.
pub fn start_scanner(
    mut sender: Sender<SftpDownload>,
    db_url: String,
    sftp_source: SftpSource,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
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

        let sftp_connection = loop {
            let conn_result = SftpConnection::new(
                &sftp_source.address.clone(),
                &sftp_source.username.clone(),
                sftp_source.password.clone(),
                false,
            );

            match conn_result {
                Ok(c) => break c,
                Err(e) => error!("Could not connect: {}", e),
            }

            thread::sleep(time::Duration::from_millis(1000));
        };

        loop {
            let scan_start = time::Instant::now();
            debug!("Started scanning {}", &sftp_source.name);

            scan_directory(&sftp_source, &sftp_connection, &conn, &mut sender);

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

            thread::sleep(time::Duration::from_millis(sftp_source.scan_interval));
        }
    })
}

fn scan_directory(sftp_source: &SftpSource, sftp_connection: &SftpConnection, conn: &postgres::Connection, sender: &mut Sender<SftpDownload>) {
    let read_result = sftp_connection
        .sftp
        .readdir(Path::new(&sftp_source.directory));

    let paths = match read_result {
        Ok(paths) => paths,
        Err(e) => {
            error!("Could not read directory {}: {}", &sftp_source.directory, e);
            return;
        }
    };

    for (path, stat) in paths {
        let file_name = path.file_name().unwrap().to_str().unwrap();

        let file_size: u64 = stat.size.unwrap();

        let cast_result = i64::try_from(file_size);

        let file_size_db: i64 = match cast_result {
            Ok(size) => size,
            Err(e) => {
                error!("Could not convert file size to type that can be stored in database: {}", e);
                return;
            }
        };

        let path_str = path.to_str().unwrap().to_string();

        if sftp_source.regex.is_match(file_name) {
            debug!(" - {} - matches!", path_str);

            let query_result = conn.query(
                "select 1 from sftp_scanner.scan where remote = $1 and path = $2 and size = $3",
                &[&sftp_source.name, &path_str, &file_size_db]
            );

            let file_is_new = match query_result {
                Ok(rows) => rows.is_empty(),
                Err(e) => {
                    error!("Error querying database: {}", e);
                    true
                }
            };

            if file_is_new {
                let command = SftpDownload {
                    created: Utc::now(),
                    size: stat.size,
                    sftp_source: sftp_source.name.clone(),
                    path: path_str.clone(),
                };

                sender.try_send(command).unwrap();

                let execute_result = conn.execute(
                    "insert into sftp_scanner.scan (remote, path, size) values ($1, $2, $3)",
                    &[&sftp_source.name, &path_str, &file_size_db]
                );

                match execute_result {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error inserting record: {}", e);
                    }
                }
            } else {
                debug!("{} already encountered {}", sftp_source.name, path_str);
            }
        } else {
            debug!(" - {} - no match", path_str);
        }
    }
}