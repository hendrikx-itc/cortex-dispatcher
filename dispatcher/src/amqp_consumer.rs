use std::net::SocketAddr;
use std::thread;

use lapin_futures::channel::{BasicConsumeOptions, QueueDeclareOptions};
use lapin_futures::client::ConnectionOptions;
use lapin_futures::types::FieldTable;
use lapin_futures as lapin;

use failure::Error;
use log::info;
use tokio;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::current_thread::block_on_all;
use actix::prelude::*;


use serde_json;

use crate::command_handler::CommandHandler;

use cortex_core::Command;


trait CommandDispatch {
    fn dispatch(&mut self, target: &mut CommandHandler) -> Box<Future<Item = bool, Error = Error>>;
}

impl CommandDispatch for Command {
    fn dispatch(&mut self, target: &mut CommandHandler) -> Box<Future<Item = bool, Error = Error>> {
        match self {
            Command::SftpDownload { created, size, sftp_source, path } => {
                info!("dispatch SftpDownload created at {}", created);

                target.sftp_download(sftp_source.clone(), *size, path.clone())
            },
            Command::HttpDownload { created, size, url } => {
                info!("dispatch HttpDownload created at {}", created);

                target.http_download(*size, url.clone())
            }
        }
    }
}

/// Starts a new thread running the command consumer
pub fn start_consumer(addr: String, queue_name: String, mut command_handler: CommandHandler) -> thread::JoinHandle<()> {
    let builder = thread::Builder::new()
        .name("amqp_listener".into());

    builder.spawn(move || {
        let addr: SocketAddr = addr.parse().unwrap();

        block_on_all(
            TcpStream::connect(&addr).map_err(Error::from).and_then(|stream| {

                // connect() returns a future of an AMQP Client
                // that resolves once the handshake is done
                lapin::client::Client::connect(stream, ConnectionOptions::default()).map_err(Error::from)
            }).and_then(|(client, heartbeat)| {
                // The heartbeat future should be run in a dedicated thread so that nothing can prevent it from
                // dispatching events on time.
                // If we ran it as part of the "main" chain of futures, we might end up not sending
                // some heartbeats if we don't poll often enough (because of some blocking task or such).
                tokio::spawn(heartbeat.map_err(|_| ()));

                // create_channel returns a future that is resolved
                // once the channel is successfully created
                client.create_channel().map_err(Error::from)
            }).and_then(|channel| {
                let id = channel.id;
                info!("created channel with id: {}", id);

                let ch = channel.clone();

                channel.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::new()).and_then(move |queue| {
                    info!("channel {} declared queue {}", id, queue_name);

                    let consume_options = BasicConsumeOptions {
                        no_local: false,
                        no_ack: false,
                        exclusive: true,
                        no_wait: false,
                        ticket: 0
                    };

                    // basic_consume returns a future of a message
                    // stream. Any time a message arrives for this consumer,
                    // the for_each method would be called
                    channel.basic_consume(&queue, "my_consumer", consume_options, FieldTable::new())
                }).and_then(|stream| {
                    info!("got consumer stream");

                    stream.for_each(move |message| {
                        let deserialize_result: serde_json::Result<Command> = serde_json::from_slice(message.data.as_slice());

                        match deserialize_result {
                            Ok(mut command) => {
                                let local_ch = ch.clone();
                                let local_tag = message.delivery_tag.clone();

                                let future_dispatch = command
                                    .dispatch(&mut command_handler)
                                    .then(move |r| {
                                        match r {
                                            Ok(ret) => {
                                                info!("Ok: {} Sending Ack...", ret);
                                                local_ch.basic_ack(local_tag, false);

                                            },
                                            Err(e) => {
                                                error!("Error: {} Sending Nack...", e);
                                                local_ch.basic_nack(local_tag, false, false);
                                            }
                                        }

                                        future::result(Ok(()))
                                    });

                                info!("Spawning dispatch future");

                                Arbiter::spawn(future_dispatch);
                            },
                            Err(e) => error!("Error deserializing command: {}", e)
                        };

                        future::ok(())
                    })
                }).map_err(Error::from)
            })
        ).expect("runtime failure");
    }).unwrap()
}
