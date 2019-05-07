use std::path::Path;
use std::{thread, time};
use std::time::Duration;
use std::convert::TryFrom;

use crate::lapin::channel::{BasicProperties, BasicPublishOptions};
use crate::lapin::client::ConnectionOptions;
use env_logger;
use failure::Error;
use futures::Future;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Sender, Receiver};
use lapin_futures as lapin;
use log::{info, error, debug};
use tokio;
use tokio::net::TcpStream;
use serde_json;

extern crate config;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate prometheus;

#[macro_use]
extern crate lazy_static;

extern crate postgres;

extern crate chrono;
use chrono::prelude::*;

extern crate cortex_core;
use cortex_core::SftpDownload;

mod cmd;
mod settings;
mod metrics;

use settings::{Settings, SftpSource};
use cortex_core::sftp_connection::SftpConnection;

fn main() {
    let matches = cmd::app().get_matches();

    let mut env_logger_builder = env_logger::builder();

    if matches.is_present("service") {
        env_logger_builder.default_format_timestamp(false);
    }

    env_logger_builder.init();

    let config_file = matches
        .value_of("config")
        .unwrap_or("/etc/cortex/sftp-scanner.yaml");

    let settings = load_settings(&config_file);

	let (cmd_sender, cmd_receiver) = channel(4096);

    let scanner_threads: Vec<thread::JoinHandle<()>> = settings.sftp_sources.clone().into_iter().map(|sftp_source| {
        start_scanner(cmd_sender.clone(), settings.postgresql.url.clone(), sftp_source)
    }).collect();

    let metrics_collector_join_handle = start_metrics_collector(
        settings.prometheus.push_gateway.clone(),
        settings.prometheus.push_interval
    );

    let future = channel_to_amqp(
        cmd_receiver, settings.command_queue.address
    );

    tokio::run(future);

    for scanner_thread in scanner_threads {
        let res = scanner_thread.join();

        match res {
            Ok(()) => {
                info!("scanner thread stopped");
            },
            Err(e) => {
                error!("scanner thread stopped with error: {:?}", e)
            }
        }
    }

    let res = metrics_collector_join_handle.join();

    match res {
        Ok(()) => {
            info!("metrics collector thread stopped")
        },
        Err(e) => {
            error!("metrics collector thread stopped with error: {:?}", e)
        }
    }
}

fn start_scanner(mut sender: Sender<SftpDownload>, db_url: String, sftp_source: SftpSource) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let conn_result = postgres::Connection::connect(db_url, postgres::TlsMode::None);

        let conn = match conn_result {
            Ok(c) => {
                info!("Connected to database");
                c
            },
            Err(e) => {
                error!("Error connecting to database: {}", e);
                ::std::process::exit(2);
            }
        };

        let conn_result = SftpConnection::new(
            &sftp_source.address.clone(),
            &sftp_source.username.clone(),
            false
        );

        let sftp_connection = conn_result.unwrap();

        loop {
            let scan_start = time::Instant::now();
            debug!("Started scanning {}", &sftp_source.name);

            let read_result = sftp_connection.sftp.readdir(Path::new(&sftp_source.directory));

            let paths = match read_result {
                Ok(paths) => paths,
                Err(e) => {
                    error!("Could not read directory {}: {}", &sftp_source.directory, e);
                    break;
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
                        break;
                    }
                };

                let path_str = path.to_str().unwrap().to_string();

                if sftp_source.regex.is_match(file_name) {
                    debug!(" - {} - matches!", path_str);

                    let rows = conn.query(
                        "select 1 from sftp_scanner.scan where remote = $1 and path = $2 and size = $3",
                        &[&sftp_source.name, &path_str, &file_size_db]
                    ).unwrap();

                    if rows.is_empty() {
                        let command = SftpDownload {
                            created: Utc::now(),
                            size: stat.size,
                            sftp_source: sftp_source.name.clone(),
                            path: path_str.clone()
                        };

                        sender.try_send(command).unwrap();

                        let execute_result = conn.execute(
                            "insert into sftp_scanner.scan (remote, path, size) values ($1, $2, $3)",
                            &[&sftp_source.name, &path_str, &file_size_db]
                        );

                        match execute_result {
                            Ok(_) => {},
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

            let scan_end = time::Instant::now();

            let scan_duration = scan_end.duration_since(scan_start);

            debug!("Finished scanning {} in {} ms", &sftp_source.name, scan_duration.as_millis());

            metrics::DIR_SCAN_COUNTER.with_label_values(&[&sftp_source.name]).inc();
            metrics::DIR_SCAN_DURATION.with_label_values(&[&sftp_source.name]).inc_by(scan_duration.as_millis() as i64);

            thread::sleep(time::Duration::from_millis(sftp_source.scan_interval));
        }
    })
}

/// Connects a channel receiver to an AMQP queue.
fn channel_to_amqp(receiver: Receiver<SftpDownload>, addr: std::net::SocketAddr) -> impl Future<Item = (), Error = ()> + Send + 'static {
    connect_channel(&addr)
        .map(move |channel| {
            let stream = receiver.for_each(move |cmd| {
                let command_str = serde_json::to_string(&cmd).unwrap();

                let exchange = "amq.direct";

                let routing_key = format!("source.{}", &cmd.sftp_source);

                let future = channel.basic_publish(
                    exchange,
                    &routing_key,
                    command_str.as_bytes().to_vec(),
                    BasicPublishOptions::default(),
                    BasicProperties::default(),
                ).and_then(move |_request_result| {
                    debug!("Command sent: {}", cmd);
                    // No confirmation/ack is expected
                    Ok(())
                }).map_err(|e| {
                    error!("Error sending command: {:?}", e);
                });

                tokio::spawn(future);

                Ok(())
            });

            tokio::spawn(stream);
        })
        .and_then(|_| {
            Ok(())
        })
        .map_err(|e| {
            error!("Error: {:?}", e)
        })
}

fn connect_channel(addr: &std::net::SocketAddr) -> impl Future<Item = lapin::channel::Channel<TcpStream>, Error = Error> + Send + 'static {
    TcpStream::connect(addr)
        .map_err(Error::from)
        .and_then(|stream| {
            debug!("TcpStream connected");

            lapin::client::Client::connect(stream, ConnectionOptions::default())
                .map_err(Error::from)
        })
        .and_then(|(client, heartbeat)| {
            tokio::spawn(heartbeat.map_err(|_e| ()));

            client.create_channel().map_err(Error::from)
        })
}

fn load_settings(config_file: &str) -> Settings {
    info!("Loading configuration from file {}", config_file);

    let mut settings = config::Config::new();

    let merge_result = settings
        .merge(config::File::new(config_file, config::FileFormat::Yaml));

    match merge_result {
        Ok(_config) => {
            info!("Configuration loaded from file {}", config_file);
        },
        Err(e) => {
            error!("Error loading configuration: {}", e);
            ::std::process::exit(1);
        }
    }

    let into_result = settings.try_into();

    let settings: Settings = match into_result {
        Ok(s) => s,
        Err(e) => {
            error!("Error loading configuration: {}", e);
            ::std::process::exit(1);
        }
    };

    info!("Configuration loaded");

    settings
}

fn start_metrics_collector(address: String, push_interval: u64) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(push_interval));

            let metric_families = prometheus::gather();
            let push_result = prometheus::push_metrics(
                "cortex-sftp-scanner",
                labels! {},
                &address,
                metric_families,
                Some(prometheus::BasicAuthentication {
                    username: "user".to_owned(),
                    password: "pass".to_owned(),
                }),
            );
            
            match push_result {
                Ok(_) => {
                    debug!("Pushed metrics to Prometheus Gateway");
                },
                Err(e) => {
                    error!("Error pushing metrics to Prometheus Gateway: {}", e);
                }
            }
        }
    })
}
