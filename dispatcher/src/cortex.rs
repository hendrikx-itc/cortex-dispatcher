use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::thread;

#[cfg(target_os = "linux")]
extern crate inotify;

extern crate failure;
use failure::Error;

extern crate lapin_futures;
use lapin_futures::{ConnectionProperties};

extern crate tokio_executor;
use tokio::prelude::Stream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_executor::enter;
use tokio::sync::oneshot;

use futures::future::{Future, IntoFuture};

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use signal_hook;
use signal_hook::iterator::Signals;

use crate::base_types::{Connection, RabbitMQNotify, Source, Target, CortexConfig, Notify, ControlCommand};

#[cfg(target_os = "linux")]
use crate::directory_source::start_directory_sources;

use crate::directory_target::to_stream;
use crate::event::FileEvent;
use crate::http_server::start_http_server;
use crate::metrics_collector::metrics_collector;
use crate::persistence::{Persistence, PostgresPersistence};
use crate::settings;
use crate::sftp_source::SftpDownloader;

fn stream_consuming_future(
    stream: Box<futures::Stream<Item = FileEvent, Error = ()> + Send>,
) -> Box<futures::Future<Item = (), Error = ()> + Send> {
    Box::new(stream.for_each(|_| futures::future::ok(())))
}

fn connect_channel(
    addr: &str,
) -> impl Future<Item = lapin_futures::Channel, Error = Error> + Send + 'static {
    lapin_futures::Client::connect(
        addr,
        ConnectionProperties::default(),
    )
    .map_err(Error::from)
    .and_then(|client| client.create_channel().map_err(Error::from))
}

pub fn run(settings: settings::Settings) {
    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let mut targets: HashMap<String, Arc<Target>> = HashMap::new();

    // Should hold all oneshot channel senders for stopping various async parts.
    let mut stop_senders = Vec::new();

    settings.directory_targets.iter().for_each(|target_conf| {
        let (sender, receiver) = unbounded_channel();

        let target_stream = to_stream(target_conf, receiver);

        let target_stream = match &target_conf.notify {
            Some(conf) => match conf {
                settings::Notify::RabbitMQ(notify_conf) => {
                    let amqp_channel = connect_channel(&notify_conf.address);

                    let message_template = notify_conf.message_template.clone();
                    let exchange = notify_conf.exchange.clone();
                    let routing_key = notify_conf.routing_key.clone();

                    let address = notify_conf.address.clone();

                    Box::new(
                        amqp_channel
                            .map_err(move |e| {
                                error!("Error connecting to AMQP channel {}: {}", address, e);
                            })
                            .and_then(|channel| {
                                let notify = RabbitMQNotify {
                                    message_template: message_template,
                                    channel: channel,
                                    exchange: exchange,
                                    routing_key: routing_key,
                                };

                                stream_consuming_future(notify.and_then_notify(target_stream))
                            }),
                    )
                }
            },
            None => stream_consuming_future(Box::new(target_stream)),
        };

        let (stop_sender, stop_receiver) = oneshot::channel::<ControlCommand>();

        stop_senders.push(stop_sender);

        let stoppable_stream = target_stream.into_future().select2(stop_receiver.into_future());

        runtime.spawn(stoppable_stream.map(|_result| debug!("End directory target stream")).map_err(|_e| error!("Error: ")));

        let target = Arc::new(Target {
            name: target_conf.name.clone(),
            sender: sender,
        });

        targets.insert(target_conf.name.clone(), target);
    });

    let prometheus_push_conf = settings.prometheus_push.clone();

    if let Some(conf) = prometheus_push_conf {
        let (stop_sender, stop_receiver) = oneshot::channel::<ControlCommand>();

        stop_senders.push(stop_sender);

        let metrics_coll = metrics_collector(conf.gateway.clone(), conf.interval);

        let stoppable_metrics_coll = metrics_coll.select2(stop_receiver.into_future());

        runtime.spawn(stoppable_metrics_coll.map(|_result| debug!("End metrics stream")).map_err(|_e| error!("Error: ")));

        info!("Prometheus metrics push collector configured");
    };

    #[cfg(target_os = "linux")]
    let (directory_sources_join_handle, inotify_stop_sender, mut directory_sources) =
        start_directory_sources(settings.directory_sources.clone());

    stop_senders.push(inotify_stop_sender);

    let connections: Vec<Connection> = settings
        .connections
        .iter()
        .map(|conn_conf| {
            let target = targets.get(&conn_conf.target).unwrap().clone();

            Connection {
                source_name: conn_conf.source.clone(),
                target: target,
                filter: conn_conf.filter.clone(),
            }
        })
        .collect();

    let connection_manager =
        PostgresConnectionManager::new(settings.postgresql.url.parse().unwrap(), NoTls);

    let persistence = PostgresPersistence::new(connection_manager);

    let mut source_sender_pairs: Vec<(oneshot::Receiver<ControlCommand>, settings::SftpSource, UnboundedSender<FileEvent>)> =
        Vec::new();

    let mut sources = Vec::new();

    settings.sftp_sources.iter().for_each(|sftp_source| {
        let (sender, receiver) = unbounded_channel();

        let (command_sender, command_receiver) = oneshot::channel::<ControlCommand>();

        stop_senders.push(command_sender);

        source_sender_pairs.push((command_receiver, sftp_source.clone(), sender));
        sources.push(Source {
            name: sftp_source.name.clone(),
            receiver: receiver,
        });
    });

    let (sftp_join_handles, connect_future) = connect_sftp_downloaders(
        &settings.command_queue.address,
        source_sender_pairs,
        settings.storage.directory.clone(),
        persistence,
    );

    runtime.spawn(connect_future);

    #[cfg(target_os = "linux")]
    sources.append(&mut directory_sources);

    let local_event_dispatchers = sources.into_iter().map(|source| {
        // Filter connections belonging to this source
        let source_connections: Vec<Connection> = connections
            .iter()
            .filter(|c| c.source_name == source.name)
            .cloned()
            .collect();

        let source_name = source.name.clone();

        source.receiver.map_err(|_| ()).for_each(move |file_event| {
            debug!(
                "FileEvent from {}: {}",
                &source_name,
                file_event.path.to_str().unwrap()
            );

            source_connections
                .deref()
                .iter()
                .filter(|c| c.filter.event_matches(&file_event))
                .for_each(|c| {
                    let mut s = c.target.sender.clone();
                    s.try_send(file_event.clone()).unwrap();
                });

            futures::future::ok(())
        })
    });

    for local_event_dispatcher in local_event_dispatchers {
        let (stop_sender, stop_receiver) = oneshot::channel::<ControlCommand>();

        stop_senders.push(stop_sender);

        let stoppable_dispatcher = local_event_dispatcher.select2(stop_receiver.into_future());

        runtime.spawn(stoppable_dispatcher.map(|_result| debug!("End connection stream")).map_err(|_e| error!("Error: ")));
    }

    let cortex_config = CortexConfig {
        sftp_sources: Arc::new(Mutex::new(settings.sftp_sources)),
        directory_targets: Arc::new(Mutex::new(settings.directory_targets)),
        connections: Arc::new(Mutex::new(settings.connections)),
    };

    let static_content_path = settings.http_server.static_content_path.clone();

    let (web_server_join_handle, actix_system, actix_http_server) = start_http_server(
        settings.http_server.address,
        static_content_path,
        cortex_config,
    );

    let signals = Signals::new(&[
        signal_hook::SIGHUP,
        signal_hook::SIGTERM,
        signal_hook::SIGINT,
        signal_hook::SIGQUIT,
    ]).unwrap();

    let signal_stream = signals.into_async().unwrap().into_future();

    runtime.spawn(
        signal_stream
            .map(move |sig| {
                info!("signal: {}", sig.0.unwrap());

                for sender in stop_senders {
                    sender.send(ControlCommand::ShutDown).unwrap();
                }

                tokio::spawn(actix_http_server.stop(true));
                actix_system.stop();
            })
            .map_err(|e| panic!("{}", e.0))
    );

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    #[cfg(target_os = "linux")]
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
        }
        Err(e) => {
            error!("{} thread stopped with error: {:?}", thread_name, e);
        }
    }
}

/// Connect to RabbitMQ and when the connection is made, start all SFTP
/// downloaders that consume commands from it.
fn connect_sftp_downloaders<T>(
    rabbitmq_address: &str,
    sftp_sources: Vec<(oneshot::Receiver<ControlCommand>, settings::SftpSource, UnboundedSender<FileEvent>)>,
    storage_directory: std::path::PathBuf,
    persistence: T,
) -> (
    Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    impl Future<Item = (), Error = ()>,
)
where
    T: Persistence,
    T: Send,
    T: Clone,
    T: 'static,
{
    let join_handles = Arc::new(Mutex::new(Vec::new()));
    let join_handles_result = join_handles.clone();

    let stream = lapin_futures::Client::connect(
        &rabbitmq_address,
        ConnectionProperties::default(),
    )
    .map_err(failure::Error::from)
    .and_then(move |client| {
        for (stop_receiver, sftp_source, sender) in sftp_sources {
            let mut jhs = join_handles.lock().unwrap();

            jhs.push(SftpDownloader::start(
                stop_receiver,
                client.clone(),
                sftp_source.clone(),
                sender,
                storage_directory.clone(),
                persistence.clone(),
            ));
        }

        futures::future::ok(())
    })
    .map_err(|e| {
        error!("{}", e);
    });

    (join_handles_result, stream)
}
