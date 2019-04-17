use std::path::Path;
use std::{thread, time};

use crate::lapin::channel::{BasicProperties, BasicPublishOptions, QueueDeclareOptions};
use crate::lapin::client::ConnectionOptions;
use crate::lapin::types::FieldTable;
use env_logger;
use failure::Error;
use futures::Future;
use futures::stream::Stream;
use futures::sync::mpsc::channel;
use lapin_futures as lapin;
use log::{info, error};
use tokio;
use tokio::net::TcpStream;
use serde_json;

extern crate config;

#[macro_use]
extern crate serde_derive;

extern crate chrono;
use chrono::prelude::*;

mod cmd;
mod settings;
mod sftp_connection;

use settings::Settings;
use sftp_connection::SftpConnection;


/// The set of commands that can be consumed from the command queue
#[derive(Debug, Deserialize, Clone, Serialize)]
enum Command {
    SftpDownload { created: DateTime<Utc>, sftp_source: String, path: String },
    HttpDownload { created: DateTime<Utc>, url: String }
}

fn main() {
    env_logger::init();

    let matches = cmd::app().get_matches();

    let config_file = matches
        .value_of("config")
        .unwrap_or("/etc/cortex/sftp-scanner.yaml");

    let mut settings = config::Config::new();

    info!("Loading configuration from file {}", config_file);

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

    let settings: Settings = settings.try_into().unwrap();

    info!("Configuration loaded");

	let (sender, receiver) = channel(4096);

    let scanner_threads: Vec<thread::JoinHandle<()>> = settings.sftp_sources.clone().into_iter().map(|sftp_source| {
		let mut sender_l = sender.clone();
		thread::spawn(move || {
			let conn_result = SftpConnection::new(&sftp_source.address.clone(), &sftp_source.username.clone());

			let sftp_connection = conn_result.unwrap();

			loop {
				info!("{} scanning remote directory '{}'", &sftp_source.name, &sftp_source.directory);

				let result = sftp_connection.sftp.readdir(Path::new(&sftp_source.directory));

				let paths = result.unwrap();

				for (path, stat) in paths {
					let file_name = path.file_name().unwrap().to_str().unwrap();

					let path_str = path.to_str().unwrap().to_string();

					if sftp_source.regex.is_match(file_name) {
						info!(" - {} - matches!", path_str);
						let command = Command::SftpDownload {
							created: Utc::now(),
							sftp_source: sftp_source.name.clone(),
							path: path_str
						};

						sender_l.try_send(command).unwrap();
					} else {
						info!(" - {} - no match", path_str);
					}
				}

				thread::sleep(time::Duration::from_millis(1000));
			}
		})
    }).collect();

    let addr = settings.command_queue.address.parse().unwrap();

    let future = TcpStream::connect(&addr)
        .map_err(Error::from)
        .and_then(|stream| {
            info!("TcpStream connected");

            lapin::client::Client::connect(stream, ConnectionOptions::default())
                .map_err(Error::from)
        })
        .and_then(|(client, _ /* heartbeat */)| {
            // create_channel returns a future that is resolved
            // once the channel is successfully created
            client.create_channel().map_err(Error::from)
        })
        .and_then(|channel| {
            let id = channel.id;
            info!("created channel with id: {}", id);

            let queue_name = settings.command_queue.queue_name;

            // we using a "move" closure to reuse the channel
            // once the queue is declared. We could also clone
            // the channel
            channel
                .queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::new())
                .map(move |queue| {
                    info!("channel {} declared queue {}", id, queue.name());

					let stream = receiver.for_each(move |cmd| {
						let command_str = serde_json::to_string(&cmd).unwrap();

						let future = channel.basic_publish(
							"",
							&queue.name(),
							command_str.as_bytes().to_vec(),
							BasicPublishOptions::default(),
							BasicProperties::default(),
						).and_then(|request_result| {
							info!("command sent");
							match request_result {
								Some(request_id) => {
									info!("confirmed: {}", request_id);
								},
								None => {
									info!("not confirmed/nacked");
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
                .map_err(Error::from)
        })
        .and_then(|_| {
            Ok(())
        })
        .map_err(|e| {
            error!("Error: {:?}", e)
        });

    tokio::run(future);
}
