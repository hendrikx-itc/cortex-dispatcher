use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "linux")]
extern crate inotify;

extern crate failure;
use failure::Error;

extern crate lapin_futures;
use lapin;

extern crate tokio_executor;
use tokio::prelude::Stream;
use tokio::sync::mpsc::unbounded_channel;
use tokio_executor::enter;
use tokio::sync::oneshot;

use futures::future::{Future, IntoFuture};

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use signal_hook;
use signal_hook::iterator::Signals;

use crossbeam_channel::bounded;

use cortex_core::wait_for;

use crate::base_types::{Connection, RabbitMQNotify, Source, Target, CortexConfig, Notify};

#[cfg(target_os = "linux")]
use crate::directory_source::start_directory_sources;

use crate::directory_target::to_stream;
use crate::event::FileEvent;
use crate::http_server::start_http_server;
use crate::metrics_collector::metrics_collector;
use crate::persistence::PostgresPersistence;
use crate::settings;
use crate::sftp_source::SftpDownloader;
use crate::sftp_command_consumer::SftpCommandConsumer;

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
        lapin::ConnectionProperties::default(),
    )
    .map_err(Error::from)
    .and_then(|client| client.create_channel().map_err(Error::from))
}

pub fn run(settings: settings::Settings) {
    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let mut targets: HashMap<String, Arc<Target>> = HashMap::new();

    // Will hold all functions that stop components of the SFTP scannner
    let mut stop_commands: Vec<Box<dyn FnOnce() -> () + Send + 'static>> = Vec::new();

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

        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        stop_commands.push(Box::new(move || {
            stop_sender.send(()).unwrap();
        }));

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
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        stop_commands.push(Box::new(move || {
            stop_sender.send(()).unwrap();
        }));

        let metrics_coll = metrics_collector(conf.gateway.clone(), conf.interval);

        let stoppable_metrics_coll = metrics_coll.select2(stop_receiver.into_future());

        runtime.spawn(stoppable_metrics_coll.map(|_result| debug!("End metrics stream")).map_err(|_e| error!("Error: ")));

        info!("Prometheus metrics push collector configured");
    };

    #[cfg(target_os = "linux")]
    let (directory_sources_join_handle, inotify_stop_sender, mut directory_sources) =
        start_directory_sources(settings.directory_sources.clone());

    stop_commands.push(Box::new(move || {
        inotify_stop_sender.send(()).unwrap();
    }));

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

    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    stop_commands.push(Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    }));

    let mut command_consumers = Vec::new();

    let mut sources = Vec::new();
    let mut sftp_join_handles = Vec::new();

    let amqp_conn = lapin::Connection::connect(&settings.command_queue.address, lapin::ConnectionProperties::default()).wait().expect("connection error");

    settings.sftp_sources.iter().for_each(|sftp_source| {
        let (sender, receiver) = unbounded_channel();

        let (s, r) = bounded(10);

        command_consumers.push(
            SftpCommandConsumer::start(stop.clone(), amqp_conn.clone(), sftp_source.clone(), s)
        );

        for n in 0..sftp_source.thread_count {
            sftp_join_handles.push(SftpDownloader::start(
                stop.clone(),
                r.clone(),
                sftp_source.clone(),
                sender.clone(),
                settings.storage.directory.clone(),
                persistence.clone(),
            ));

            info!("Started {} download thread {}", &sftp_source.name, n + 1);
        }

        sources.push(Source {
            name: sftp_source.name.clone(),
            receiver: receiver,
        });
    });

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
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        stop_commands.push(Box::new(move || {
            match stop_sender.send(()) {
                Ok(_) => (),
                Err(e) => error!("Error sending stop signal: {:?}", e)
            }
        }));

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

    stop_commands.push(Box::new(move || {
        tokio::spawn(actix_http_server.stop(true));
    }));

    stop_commands.push(Box::new(move || {
        actix_system.stop();
    }));

    runtime.spawn(setup_signal_handler(stop_commands));

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    #[cfg(target_os = "linux")]
    wait_for(directory_sources_join_handle, "directory sources");

    wait_for(web_server_join_handle, "http server");

    sftp_join_handles.into_iter().for_each(|jh| {
        wait_for(jh, "sftp download");
    });
}

fn setup_signal_handler(stop_commands: Vec<Box<dyn FnOnce() -> () + Send + 'static>>) -> impl Future<Item = (), Error = ()> + Send + 'static {
    let signals = Signals::new(&[
        signal_hook::SIGHUP,
        signal_hook::SIGTERM,
        signal_hook::SIGINT,
        signal_hook::SIGQUIT,
    ]).unwrap();

    let signal_stream = signals.into_async().unwrap().into_future();

    signal_stream
        .map(move |sig| {
            info!("signal: {}", sig.0.unwrap());

            for stop_command in stop_commands {
                stop_command();
            }
        })
        .map_err(|e| panic!("{}", e.0))
}
