use std::path::Path;
use std::{thread, time};
use std::time::Duration;
use std::fmt;
use std::convert::TryFrom;

use crate::lapin::channel::{BasicProperties, BasicPublishOptions, QueueDeclareOptions};
use crate::lapin::client::ConnectionOptions;
use crate::lapin::types::FieldTable;
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

mod cmd;
mod settings;
mod sftp_connection;
mod metrics;

use settings::{Settings, SftpSource};
use sftp_connection::SftpConnection;


/// The set of commands that can be consumed from the command queue
#[derive(Debug, Deserialize, Clone, Serialize)]
enum Command {
    SftpDownload { created: DateTime<Utc>, sftp_source: String, path: String },
    HttpDownload { created: DateTime<Utc>, url: String }
}

impl fmt::Display for Command {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
            Command::SftpDownload { created, sftp_source, path } => {
                write!(f, "SftpDownload({}, {}, {})", created, sftp_source, path)
            },
            Command::HttpDownload { created, url } => {
                write!(f, "HttpDownload({}, {})", created, url)
            }
		}
    }
}

fn main() {
    env_logger::init();

    let matches = cmd::app().get_matches();

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
        cmd_receiver, settings.command_queue.address, settings.command_queue.queue_name
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

fn start_scanner(mut sender: Sender<Command>, db_url: String, sftp_source: SftpSource) -> thread::JoinHandle<()> {
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

        let conn_result = SftpConnection::new(&sftp_source.address.clone(), &sftp_source.username.clone());

        let sftp_connection = conn_result.unwrap();

        loop {
            info!("{} scanning remote directory '{}'", &sftp_source.name, &sftp_source.directory);

            let result = sftp_connection.sftp.readdir(Path::new(&sftp_source.directory));

            let paths = result.unwrap();

            for (path, stat) in paths {
                let file_name = path.file_name().unwrap().to_str().unwrap();

                let cast_result = i64::try_from(stat.size.unwrap());

                let file_size: i64 = match cast_result {
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
                        &[&sftp_source.name, &path_str, &file_size]
                    ).unwrap();

                    if rows.is_empty() {
                        let command = Command::SftpDownload {
                            created: Utc::now(),
                            sftp_source: sftp_source.name.clone(),
                            path: path_str.clone()
                        };

                        sender.try_send(command).unwrap();

                        conn.execute(
                            "insert into sftp_scanner.scan (remote, path, size) values ($1, $2, $3)",
                            &[&sftp_source.name, &path_str, &file_size]
                        ).unwrap();
                    } else {
                        debug!("{} already encountered {}", sftp_source.name, path_str);
                    }
                } else {
                    debug!(" - {} - no match", path_str);
                }
            }

            metrics::DIR_SCAN_COUNTER.inc();

            thread::sleep(time::Duration::from_millis(sftp_source.scan_interval));
        }
    })
}

/// Connects a channel receiver to an AMQP queue.
fn channel_to_amqp(receiver: Receiver<Command>, addr: std::net::SocketAddr, queue_name: String) -> impl Future<Item = (), Error = ()> + Send + 'static {
    connect_queue(&addr, queue_name)
        .map(move |(channel, queue)| {
            info!("declared queue {}", queue.name());

            let stream = receiver.for_each(move |cmd| {
                let command_str = serde_json::to_string(&cmd).unwrap();

                let future = channel.basic_publish(
                    "",
                    &queue.name(),
                    command_str.as_bytes().to_vec(),
                    BasicPublishOptions::default(),
                    BasicProperties::default(),
                ).and_then(move |request_result| {
                    info!("command sent: {}", cmd);

                    match request_result {
                        Some(request_id) => {
                            debug!("confirmed: {}", request_id);
                        },
                        None => {
                            debug!("not confirmed/nacked");
                        }
                    }
                    Ok(())
                }).map_err(|e| {
                    info!("error sending command: {:?}", e);
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

fn connect_queue(addr: &std::net::SocketAddr, queue_name: String) -> impl Future<Item = (lapin::channel::Channel<TcpStream>, lapin::queue::Queue), Error = Error> + Send + 'static {
    connect_channel(&addr)
        .and_then(move |channel| {
            info!("created channel with id: {}", channel.id);

            // we using a "move" closure to reuse the channel
            // once the queue is declared. We could also clone
            // the channel
            channel
                .queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::new())
                .map(|queue| {(channel, queue)})
                .map_err(Error::from)
        })
}

fn connect_channel(addr: &std::net::SocketAddr) -> impl Future<Item = lapin::channel::Channel<TcpStream>, Error = Error> + Send + 'static {
    TcpStream::connect(addr)
        .map_err(Error::from)
        .and_then(|stream| {
            info!("TcpStream connected");

            lapin::client::Client::connect(stream, ConnectionOptions::default())
                .map_err(Error::from)
        })
        .and_then(|(client, heartbeat)| {
            tokio::spawn(heartbeat.map_err(|_e| ()));

            // create_channel returns a future that is resolved
            // once the channel is successfully created
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
