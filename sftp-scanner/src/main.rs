use crate::lapin::channel::{BasicProperties, BasicPublishOptions, QueueDeclareOptions};
use crate::lapin::client::ConnectionOptions;
use crate::lapin::types::FieldTable;
use env_logger;
use failure::Error;
use futures::future::Future;
use lapin_futures as lapin;
use log::{info, error};
use tokio;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use serde_json;

extern crate config;

#[macro_use]
extern crate serde_derive;

mod cmd;
mod settings;

use settings::Settings;

/// The set of commands that can be consumed from the command queue
#[derive(Debug, Deserialize, Clone, Serialize)]
enum Command {
    SftpDownload { sftp_source: String, path: String },
    HttpDownload { url: String }
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

    let addr = settings.command_queue.address.parse().unwrap();

    Runtime::new()
        .unwrap()
        .block_on_all(
            TcpStream::connect(&addr)
                .map_err(Error::from)
                .and_then(|stream| {
                    // connect() returns a future of an AMQP Client
                    // that resolves once the handshake is done
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
                        .and_then(move |_| {
                            info!("channel {} declared queue {}", id, queue_name);

                            let command = Command::SftpDownload { sftp_source: "test".to_string(), path: "test_data/foo.txt".to_string() };

                            let command_str = serde_json::to_string(&command).unwrap();

                            channel.basic_publish(
                                "",
                                &queue_name,
                                command_str.as_bytes().to_vec(),
                                BasicPublishOptions::default(),
                                BasicProperties::default(),
                            )
                        })
                        .map_err(Error::from)
                }),
        )
        .expect("runtime failure");
}
