use std::thread;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "linux")]
extern crate inotify;

use failure::{Error, err_msg};

extern crate lapin_futures;
use lapin_futures::{ConnectionProperties};

extern crate tokio_executor;
use tokio::prelude::Stream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_executor::enter;
use tokio::sync::oneshot;

use futures::future::{Future, ok};

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use signal_hook;
use signal_hook::iterator::Signals;

use crossbeam_channel::{bounded, Sender, Receiver};

use cortex_core::{wait_for, SftpDownload, StopCmd};

use crate::base_types::{Connection, RabbitMQNotify, Target, CortexConfig, Notify, Source};

#[cfg(target_os = "linux")]
use crate::directory_source::{start_directory_sources, start_directory_sweep, start_local_intake_thread};

use crate::directory_target::to_stream;
use crate::event::{FileEvent, EventDispatcher};
use crate::http_server::start_http_server;
use crate::metrics_collector::metrics_collector;
use crate::persistence::PostgresPersistence;
use crate::settings;
use crate::sftp_downloader;
use crate::sftp_command_consumer;
use crate::base_types::MessageResponse;
use crate::local_storage::LocalStorage;

fn stream_consuming_future(
    stream: Box<dyn futures::Stream<Item = FileEvent, Error = ()> + Send>,
) -> Box<dyn futures::Future<Item = (), Error = ()> + Send> {
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

struct Stop {
    stop_commands: Vec<StopCmd>
}

impl std::fmt::Debug for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stop")
    }
}

impl Stop {
    fn new() -> Stop {
        Stop {
            stop_commands: Vec::new()
        }
    }

    fn stop(self) {
        for stop_command in self.stop_commands {
            stop_command();
        }
    }

    fn add_command(&mut self, cmd: StopCmd) {
        self.stop_commands.push(cmd);
    }

    fn make_stoppable_stream<S>(&mut self, stream: S, name: String) -> impl Future<Item=(), Error=()>
        where S: Future<Item=(), Error=()> {
        let (stop_sender, _stop_receiver) = oneshot::channel::<()>();

        self.stop_commands.push(Box::new(move || {
            match stop_sender.send(()) {
                Ok(_) => (),
                Err(e) => error!("[E02009] Error sending stop signal: {:?}", e)
            }
        }));

        stream
            // select2(stop_receiver.into_future())
            .map(|_| debug!("End stream"))
            .map_err(move |_| error!("[E02010] Error in stoppable stream {}", &name))
    }
}

pub fn run(settings: settings::Settings) {
    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    // List of targets with their file event channels
    let mut targets: HashMap<String, Arc<Target>> = HashMap::new();

    // List of sources with their file event channels
    let sources: Arc<Mutex<Vec<Source>>> = Arc::new(Mutex::new(Vec::new()));

    let connections: Arc<Mutex<Vec<Connection>>> = Arc::new(Mutex::new(Vec::new()));

    // Stop orchestrator
    let stop: Arc<Mutex<Stop>> = Arc::new(Mutex::new(Stop::new()));

    settings.directory_targets.iter().for_each(|target_conf| {
        let (stoppable_stream, stop_cmd, target) = setup_directory_target(target_conf);

        stop.lock().unwrap().add_command(stop_cmd);

        runtime.spawn(stoppable_stream);

        targets.insert(target_conf.name.clone(), target);
    });

    let prometheus_push_conf = settings.prometheus_push.clone();

    if let Some(conf) = prometheus_push_conf {
        let metrics_coll = metrics_collector(conf.gateway.clone(), conf.interval);

        runtime.spawn(stop.lock().unwrap().make_stoppable_stream(metrics_coll, String::from("metrics collector")));

        info!("Prometheus metrics push collector configured");
    };

    let connection_manager =
        PostgresConnectionManager::new(settings.postgresql.url.parse().unwrap(), NoTls);

    let persistence = PostgresPersistence::new(connection_manager);

    let local_storage = LocalStorage::new(&settings.storage.directory, persistence.clone());

    let (local_intake_sender, local_intake_receiver) = std::sync::mpsc::channel();

    let mut senders: HashMap<String, UnboundedSender<FileEvent>> = HashMap::new();

    settings.directory_sources.iter().for_each(|directory_source| { 
        let (sender, receiver) = unbounded_channel();

        let guard = sources.lock();
        
        guard.unwrap().push(
            Source {
                name: directory_source.name.clone(),
                receiver: receiver
            }
        );

        senders.insert(directory_source.name.clone(), sender);
    });

    let event_dispatcher = EventDispatcher {
        senders: senders
    };

    start_local_intake_thread(local_intake_receiver, event_dispatcher, local_storage.clone());

    #[cfg(target_os = "linux")]
    let (directory_sources_join_handle, inotify_stop_cmd) =
        start_directory_sources(settings.directory_sources.clone(), local_intake_sender.clone());

    #[cfg(target_os = "linux")]
    {
        stop.lock().unwrap().add_command(inotify_stop_cmd);
    }

    let (directory_sweep_join_handle, sweep_stop_cmd) = start_directory_sweep(
        settings.directory_sources.clone(),
        local_intake_sender.clone(),
        settings.scan_interval
    );

    settings
        .connections
        .iter()
        .for_each(|conn_conf| {
            let target = targets.get(&conn_conf.target).unwrap().clone();

            connections.lock().unwrap().push(
                Connection {
                    source_name: conn_conf.source.clone(),
                    target: target,
                    filter: conn_conf.filter.clone(),
                }
            );
        });

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();

    stop.lock().unwrap().add_command(Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    }));

    let sftp_join_handles: Arc<Mutex<Vec<thread::JoinHandle<std::result::Result<(), sftp_downloader::Error>>>>> = Arc::new(Mutex::new(Vec::new()));

    struct SftpSourceChannels {
        pub sftp_source: settings::SftpSource,
        pub cmd_sender: Sender<(u64, SftpDownload)>,
        pub cmd_receiver: Receiver<(u64, SftpDownload)>,
        pub file_event_sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        pub file_event_receiver: tokio::sync::mpsc::UnboundedReceiver<FileEvent>,
        pub stop_receiver: oneshot::Receiver<()>
    }

    let sftp_source_channels: Vec<SftpSourceChannels> = settings.sftp_sources.iter().map(|sftp_source| {
        let (cmd_sender, cmd_receiver) = bounded::<(u64, SftpDownload)>(1000);
        let (file_event_sender, file_event_receiver) = unbounded_channel();
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        stop.lock().unwrap().add_command(Box::new(move || {
            match stop_sender.send(()) {
                Ok(_) => (),
                Err(e) => error!("[E02008] Error sending stop signal: {:?}", e)
            }
        }));

        SftpSourceChannels {
            sftp_source: sftp_source.clone(),
            cmd_sender: cmd_sender,
            cmd_receiver: cmd_receiver,
            file_event_sender: file_event_sender,
            file_event_receiver: file_event_receiver,
            stop_receiver: stop_receiver
        }
    }).collect();

    let jhs = sftp_join_handles.clone();
    let th_sources = Arc::clone(&sources);

    let stop_clone = Arc::clone(&stop);

    let connect_future = lapin_futures::Client::connect(
        &settings.command_queue.address,
        ConnectionProperties::default(),
    )
    .map_err(failure::Error::from)
    .and_then(move |client| {
        sftp_source_channels.into_iter().for_each(|channels| {
            let (ack_sender, ack_receiver) = tokio::sync::mpsc::channel::<MessageResponse>(100);

            for n in 0..channels.sftp_source.thread_count {
                let join_handle = sftp_downloader::SftpDownloader::start(
                    stop_flag.clone(),
                    channels.cmd_receiver.clone(),
                    ack_sender.clone(),
                    channels.sftp_source.clone(),
                    channels.file_event_sender.clone(),
                    local_storage.clone(),
                    persistence.clone(),
                );

                let guard = jhs.lock();

                guard.unwrap().push(join_handle);

                info!("Started {} download thread {}", &channels.sftp_source.name, n + 1);
            }

            th_sources.lock().unwrap().push(Source {
                name: channels.sftp_source.name.clone(),
                receiver: channels.file_event_receiver
            });

            let name = channels.sftp_source.name.clone();

            let stream = sftp_command_consumer::start(client.clone(), channels.sftp_source.name.clone(), ack_receiver, channels.cmd_sender.clone())
                //.select2(channels.stop_receiver.into_future())
                .map(|_| debug!("End SFTP command stream"))
                .map_err(move |_| error!("[E02007] Error in AMQP stream '{}'", &name));

            debug!("Spawning AMQP stream");
            tokio::spawn(stream);
        });

        ok(())
    })
    .and_then(move |_| {
        Arc::try_unwrap(sources).expect("still users of sources").into_inner().unwrap().into_iter().for_each(|source| {
            // Filter connections belonging to this source
            let source_connections: Vec<Connection> = connections.lock().unwrap()
                .iter()
                .filter(|c| c.source_name == source.name)
                .cloned()
                .collect();

            let source_name = source.name.clone();

            let stream_future = source.receiver.map_err(Error::from).for_each(move |file_event| {
                debug!(
                    "FileEvent from {}: {}",
                    &source_name,
                    file_event.path.to_string_lossy()
                );

                source_connections
                    .deref()
                    .iter()
                    .filter(|c| {
                        match &c.filter {
                            Some(f) => f.file_matches(&file_event.path),
                            None => true
                        }
                    })
                    .for_each(|c| {
                        let mut s = c.target.sender.clone();
                        s.try_send(file_event.clone()).unwrap();
                    });

                futures::future::ok(())
            }).map_err(|_| ());

            debug!("Spawing local event dispatcher for {}", &source.name);

            tokio::spawn(stop_clone.lock().unwrap().make_stoppable_stream(stream_future, String::from("local event connection")));
        });

        ok(())
    })
    .map_err(|_| ());

    runtime.spawn(connect_future);

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

    stop.lock().unwrap().add_command(Box::new(move || {
        tokio::spawn(actix_http_server.stop(true));
    }));

    stop.lock().unwrap().add_command(Box::new(move || {
        actix_system.stop();
    }));

    let spawn_signal_handler = futures::future::poll_fn(move || -> futures::Poll<(), Error> {
        let s_stop = Arc::clone(&stop);

        let unwrap_result = Arc::try_unwrap(s_stop);

        match unwrap_result {
            Ok(unwrapped) => {
                tokio::spawn(setup_signal_handler(unwrapped.into_inner().unwrap()));
                Ok(futures::Async::Ready(()))
            },
            Err(_) => Err(err_msg("Could not unwrap Arc"))
        }
    }).map_err(|_| ());

    runtime.spawn(spawn_signal_handler);

    // Wait until all tasks have finished
    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    #[cfg(target_os = "linux")]
    wait_for(directory_sources_join_handle, "directory sources");

    wait_for(web_server_join_handle, "http server");

    Arc::try_unwrap(sftp_join_handles).expect("still users of handles").into_inner().unwrap().into_iter().for_each(|jh| {
        wait_for(jh, "sftp download");
    });
}

fn setup_signal_handler(stop: Stop) -> impl Future<Item = (), Error = ()> + Send + 'static {
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

            stop.stop();
        })
        .map_err(|e| panic!("{}", e.0))
}

fn setup_directory_target(target_conf: &settings::DirectoryTarget) -> (impl Future<Item=(), Error=()>, StopCmd, Arc<Target>) {
    let (sender, receiver) = unbounded_channel();

    let target_stream = to_stream(target_conf, receiver);

    let target_stream = match &target_conf.notify {
        Some(conf) => match conf {
            settings::Notify::RabbitMQ(notify_conf) => {
                debug!("Connecting notifier to directory target stream");

                let amqp_channel = connect_channel(&notify_conf.address);

                let message_template = notify_conf.message_template.clone();
                let exchange = notify_conf.exchange.clone();
                let routing_key = notify_conf.routing_key.clone();

                let address = notify_conf.address.clone();

                Box::new(
                    amqp_channel
                        .map_err(move |e| {
                            error!("[E01008] Error connecting to AMQP channel {}: {}", address, e);
                        })
                        .and_then(|channel| {
                            debug!("Notifying on AMQP queue");

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

    let name = target_conf.name.clone();

    let stoppable_stream = target_stream
        //.select2(stop_receiver.into_future())
        .map(|_result| debug!("End directory target stream"))
        .map_err(move |_| error!("[E02006] Error in directory target stream '{}'", &name));

    let stop_cmd = Box::new(move || {
        stop_sender.send(()).unwrap();
    });

    let target = Arc::new(Target {
        name: target_conf.name.clone(),
        sender: sender,
    });

    (stoppable_stream, stop_cmd, target)
}

