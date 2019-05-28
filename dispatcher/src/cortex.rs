use std::thread;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
extern crate inotify;

extern crate failure;
use failure::Error;

extern crate lapin_futures;
use lapin_futures::client::ConnectionOptions;

extern crate tokio_executor;
use tokio_executor::enter;
use tokio::net::TcpStream;
use tokio::prelude::Stream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use futures::future::Future;

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use crate::directory_source::start_directory_sources;
use crate::settings;
use crate::sftp_source::SftpDownloader;
use crate::base_types::{Source, Target, Connection, RabbitMQNotify};
use crate::metrics_collector::metrics_collector;
use crate::directory_target::to_stream;
use crate::http_server::start_http_server;
use crate::persistence::{Persistence, PostgresPersistence};
use crate::event::FileEvent;
use crate::base_types::{Notify, CortexConfig};

fn stream_consuming_future(stream: Box<futures::Stream< Item = FileEvent, Error = () > + Send>) -> Box<futures::Future< Item = (), Error = () > + Send> {
    Box::new(
        stream.for_each(|_| {futures::future::ok(())})
    )
}

fn connect_channel(addr: &std::net::SocketAddr) -> impl Future<Item = lapin_futures::channel::Channel<TcpStream>, Error = Error> + Send + 'static {
    TcpStream::connect(addr)
        .map_err(Error::from)
        .and_then(|stream| {
            debug!("TcpStream connected");

            lapin_futures::client::Client::connect(stream, ConnectionOptions::default())
                .map_err(Error::from)
        })
        .and_then(|(client, heartbeat)| {
            tokio::spawn(heartbeat.map_err(|_e| ()));

            client.create_channel().map_err(Error::from)
        })
}


pub fn run(settings: settings::Settings) {
    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let mut targets: HashMap<String, Arc<Target>> = HashMap::new();

    settings.directory_targets.iter().for_each(|target_conf| {
        let (sender, receiver) = unbounded_channel();

        let target_stream = to_stream(target_conf, receiver);

        let target_stream = match &target_conf.notify {
            Some(conf) => {
                match conf {
                    settings::Notify::RabbitMQ(notify_conf) => {
                        let amqp_channel = connect_channel(&notify_conf.address);

                        let message_template = notify_conf.message_template.clone();
                        let exchange = notify_conf.exchange.clone();
                        let routing_key = notify_conf.routing_key.clone();

                        Box::new(amqp_channel.map_err(|e| { error!("Error connecting channel: {}", e); }).and_then(|channel| {
                            let notify = RabbitMQNotify {
                                message_template: message_template,
                                channel: channel,
                                exchange: exchange,
                                routing_key: routing_key
                            };

                            stream_consuming_future(notify.and_then_notify(target_stream))
                        }))
                    }
                }
            },
            None => stream_consuming_future(Box::new(target_stream))
        };

        runtime.spawn(target_stream);

        let target = Arc::new(Target {
            name: target_conf.name.clone(),
            sender: sender
        });

        targets.insert(target_conf.name.clone(), target);
    });

    let prometheus_push_conf = settings.prometheus_push.clone();

    if let Some(conf) = prometheus_push_conf {
        runtime.spawn(metrics_collector(
            conf.gateway.clone(),
            conf.interval,
        ));

        info!("Prometheus metrics push collector configured");
    };

    let (directory_sources_join_handle, mut directory_sources) = start_directory_sources(settings.directory_sources.clone());

    let connections: Vec<Connection> = settings.connections.iter().map(|conn_conf| {
        let target = targets.get(&conn_conf.target).unwrap().clone();

        Connection {
            source_name: conn_conf.source.clone(),
            target: target,
            filter: conn_conf.filter.clone()
        }
    }).collect();

    let connection_manager = PostgresConnectionManager::new(
        settings.postgresql.url.parse().unwrap(),
        NoTls,
    );

    let persistence = PostgresPersistence::new(connection_manager);

    let mut source_sender_pairs: Vec<(settings::SftpSource, UnboundedSender<FileEvent>)> = Vec::new();

    let mut sources = Vec::new();

    settings.sftp_sources.iter().for_each(|sftp_source| {
        let (sender, receiver) = unbounded_channel();

        source_sender_pairs.push((sftp_source.clone(), sender));
        sources.push(Source {name: sftp_source.name.clone(), receiver: receiver});
    });

    let (sftp_join_handles, connect_future) = connect_sftp_downloaders(
        settings.command_queue.address,
        source_sender_pairs,
        settings.storage.directory.clone(),
        persistence
    );

    runtime.spawn(connect_future);

    sources.append(&mut directory_sources);

    let local_event_dispatchers = sources.into_iter().map(|source| {
        // Filter connections belonging to this source
        let source_connections: Vec<Connection> = connections
            .iter().filter(|c| c.source_name == source.name).cloned().collect();

        let source_name = source.name.clone();

        source.receiver.map_err(|_| ()).for_each(move |file_event| {
            info!("FileEvent from {}: {}", &source_name, file_event.path.to_str().unwrap());

            source_connections.deref().iter().filter(|c| c.filter.event_matches(&file_event)).for_each(|c| {
                let mut s = c.target.sender.clone();
                s.try_send(file_event.clone()).unwrap();
            });

            futures::future::ok(())
        })
    });

    for p in local_event_dispatchers {
        runtime.spawn(p);
    }

    let cortex_config = CortexConfig {
        sftp_sources: Arc::new(Mutex::new(settings.sftp_sources))
    };

    let static_content_path = settings.http_server.static_content_path.clone();

    let web_server_join_handle = start_http_server(settings.http_server.address, static_content_path, cortex_config);

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    wait_for(directory_sources_join_handle, "directory sources");

    wait_for(web_server_join_handle, "http server");

    let l = Arc::try_unwrap(sftp_join_handles).expect("Cannot unwrap Arc");
    let jhs = l.into_inner().expect("Cannot unlock Mutex");

    jhs.into_iter().for_each(|jh| {
        wait_for(jh, "sftp download");
    });
}

fn wait_for(join_handle: thread::JoinHandle<()>, thread_name: &str) {
    let join_result = join_handle.join();

    match join_result {
        Ok(()) => {
            info!("{} thread stopped", thread_name);
        },
        Err(e) => {
            error!("{} thread stopped with error: {:?}", thread_name, e);
        }
    }
}

/// Connect to RabbitMQ and when the connection is made, start all SFTP
/// downloaders that consume commands from it.
fn connect_sftp_downloaders<T>(
        rabbitmq_address: std::net::SocketAddr,
        sftp_sources: Vec<(settings::SftpSource, UnboundedSender<FileEvent>)>,
        storage_directory: std::path::PathBuf,
        persistence: T
    ) -> (Arc<Mutex<Vec<thread::JoinHandle<()>>>>, impl Future<Item=(), Error=()>) where T: Persistence, T: Send, T: Clone, T: 'static {
    let join_handles = Arc::new(Mutex::new(Vec::new()));
    let join_handles_result = join_handles.clone();

    let stream = TcpStream::connect(&rabbitmq_address).map_err(failure::Error::from).and_then(|stream| {
        lapin_futures::client::Client::connect(stream, ConnectionOptions::default()).map_err(failure::Error::from)
    }).and_then(move |(client, heartbeat)| {
        tokio::spawn(heartbeat.map_err(|e| {
            error!("Error sending heartbeat: {}", e);
        }));

        for (sftp_source, sender) in sftp_sources {
            let mut jhs = join_handles.lock().unwrap();

            jhs.push(SftpDownloader::start(
                client.clone(),
                sftp_source.clone(),
                sender,
                storage_directory.clone(),
                persistence.clone()
            ));
        }

        futures::future::ok(())
    }).map_err(|e| {
        error!("{}", e);
    });

    (join_handles_result, stream)
}
