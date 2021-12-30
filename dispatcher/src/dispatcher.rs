use futures::future::join_all;
use std::collections::HashMap;
use std::iter::Iterator;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

#[cfg(target_os = "linux")]
extern crate inotify;

use failure::{err_msg, Error};

extern crate lapin;
use lapin::ConnectionProperties;

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;

use futures::stream::StreamExt;

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use signal_hook_tokio::Signals;

use crossbeam_channel::{bounded, Receiver, Sender};

use cortex_core::{wait_for, SftpDownload, StopCmd};

use crate::base_types::{Connection, RabbitMQNotify, Source, Target};

#[cfg(target_os = "linux")]
use crate::directory_source::start_directory_sources;
use crate::directory_source::{start_directory_sweep, start_local_intake_thread};

use crate::base_types::MessageResponse;
use crate::directory_target::handle_file_event;
use crate::event::{EventDispatcher, FileEvent};
use crate::local_storage::LocalStorage;
use crate::persistence;
use crate::persistence::{PostgresAsyncPersistence, PostgresPersistence};
use crate::settings;
use crate::sftp_command_consumer;
use crate::sftp_downloader;

pub struct Stop {
    stop_commands: Vec<StopCmd>,
}

impl std::fmt::Debug for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stop")
    }
}

impl Stop {
    fn new() -> Stop {
        Stop {
            stop_commands: Vec::new(),
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
}

pub async fn target_directory_handler<T>(
    tokio_persistence: PostgresAsyncPersistence<T>,
    settings: settings::Settings,
    stop: Arc<Mutex<Stop>>,
    targets: Arc<Mutex<HashMap<String, Arc<Target>>>>,
) where
    T: postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
{
    settings.directory_targets.iter().for_each(|target_conf| {
        let persistence = tokio_persistence.clone();
        let (sender, mut receiver) = unbounded_channel::<FileEvent>();

        let c_target_conf = target_conf.clone();
        let d_target_conf = target_conf.clone();

        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        match c_target_conf.notify {
            Some(conf) => match conf {
                settings::Notify::RabbitMQ(notify_conf) => {
                    let fut = async move {
                        debug!("Connecting notifier to directory target stream");

                        let connect_result = lapin::Connection::connect(
                            &notify_conf.address,
                            lapin::ConnectionProperties::default(),
                        )
                        .await;

                        let connection = match connect_result {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Error connecting to AMQP server: {}", e);
                                return;
                            }
                        };

                        let amqp_channel_result = connection.create_channel().await;

                        let amqp_channel = match amqp_channel_result {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Error creating AMQP channel: {}", e);
                                return;
                            }
                        };

                        let notify = RabbitMQNotify {
                            message_template: notify_conf.message_template.clone(),
                            exchange: notify_conf.exchange.clone(),
                            routing_key: notify_conf.routing_key.clone(),
                        };

                        let routing_key = notify_conf.routing_key.clone();

                        while let Some(file_event) = receiver.recv().await {
                            match handle_file_event(&d_target_conf, file_event, persistence.clone())
                                .await
                            {
                                Ok(result_event) => {
                                    debug!("Notifying with AMQP routing key {}", &routing_key);

                                    notify.notify(&amqp_channel, result_event).await;
                                }
                                Err(e) => {
                                    error!("Error handling event for directory target: {}", &e);
                                }
                            }
                        }
                    };

                    tokio::spawn(async move {
                        tokio::select!(
                            _a = fut => (),
                            _b = stop_receiver => ()
                        )
                    })
                }
            },
            None => {
                let fut = async move {
                    while let Some(file_event) = receiver.recv().await {
                        if let Err(e) =
                            handle_file_event(&d_target_conf, file_event, persistence.clone()).await
                        {
                            error!("Error handling event for directory target: {}", &e);
                        }
                    }
                };

                tokio::spawn(async move {
                    tokio::select!(
                        _a = fut => (),
                        _b = stop_receiver => ()
                    )
                })
            }
        };

        let stop_cmd_name = c_target_conf.name.clone();

        let stop_cmd = Box::new(move || {
            let send_result = stop_sender.send(());

            match send_result {
                Ok(_) => debug!(
                    "Stop command sent for directory target '{}'",
                    &stop_cmd_name
                ),
                Err(e) => debug!(
                    "Error sending stop command for directory target '{}': {:?}",
                    &stop_cmd_name, e
                ),
            }
        });

        let target = Arc::new(Target {
            name: c_target_conf.name.clone(),
            sender: sender,
        });

        match stop.lock() {
            Ok(mut guard) => {
                guard.add_command(stop_cmd);
            }
            Err(e) => {
                error!(
                    "Could not lock on Stop struct for adding stop command: {}",
                    e
                );
            }
        };

        match targets.lock() {
            Ok(mut guard) => {
                guard.insert(target_conf.name.clone(), target);
            }
            Err(e) => error!(
                "Could not get lock on targets hash for adding Target: {}",
                e
            ),
        }
    });
}

type SftpJoinHandle = thread::JoinHandle<std::result::Result<(), sftp_downloader::Error>>;

struct SftpSourceSend {
    pub sftp_source: settings::SftpSource,
    pub cmd_sender: Sender<(u64, SftpDownload)>,
    pub cmd_receiver: Receiver<(u64, SftpDownload)>,
    pub file_event_sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
    pub stop_receiver: oneshot::Receiver<()>,
}

async fn sftp_sources_handler<T: 'static>(
    settings: settings::Settings,
    sftp_join_handles: Arc<Mutex<Vec<SftpJoinHandle>>>,
    sftp_source_senders: Vec<SftpSourceSend>,
    stop_flag: Arc<AtomicBool>,
    local_storage: LocalStorage<T>,
    persistence: T,
) -> Result<(), sftp_command_consumer::ConsumeError>
where
    T: persistence::Persistence + Clone + Sync + Send,
{
    debug!(
        "Connecting to AMQP service at {}",
        &settings.command_queue.address
    );

    let amqp_client = lapin::Connection::connect(
        &settings.command_queue.address,
        ConnectionProperties::default(),
    )
    .await?;

    debug!("Connected to AMQP service");

    let mut stream_join_handles: Vec<
        tokio::task::JoinHandle<Result<(), sftp_command_consumer::ConsumeError>>,
    > = Vec::new();

    for channels in sftp_source_senders {
        let (ack_sender, ack_receiver) = tokio::sync::mpsc::channel::<MessageResponse>(100);

        for n in 0..channels.sftp_source.thread_count {
            debug!(
                "Starting SFTP download thread '{}'",
                &channels.sftp_source.name
            );

            let join_handle = sftp_downloader::SftpDownloader::start(
                stop_flag.clone(),
                channels.cmd_receiver.clone(),
                ack_sender.clone(),
                channels.sftp_source.clone(),
                channels.file_event_sender.clone(),
                local_storage.clone(),
                persistence.clone(),
            );

            let guard = sftp_join_handles.lock();

            guard.unwrap().push(join_handle);

            info!(
                "Started SFTP download thread '{}' ({})",
                &channels.sftp_source.name,
                n + 1
            );
        }

        let amqp_channel = amqp_client.create_channel().await?;

        debug!("Spawning AMQP stream task '{}'", &channels.sftp_source.name);

        let consume_future = sftp_command_consumer::start(
            amqp_channel,
            channels.sftp_source.name.clone(),
            ack_receiver,
            channels.cmd_sender.clone(),
        );

        stream_join_handles.push(tokio::spawn(async {
            tokio::select!(
                a = consume_future => a,
                _b = channels.stop_receiver => {
                    debug!("Interrupted SFTP command consumer stream '{}'", &channels.sftp_source.name);
                    Ok(())
                }
            )
        }));
    }

    // Await on futures so that the AMQP connection does not get destroyed.
    let _stream_results = join_all(stream_join_handles).await;

    Ok::<(), sftp_command_consumer::ConsumeError>(())
}

/// Start the streams that dispatch messages from sources to targets
///
/// All connections from the same source are bundled into one stream that
/// dispatches to all targets of those connections, because there is only one
/// receiver per source.
pub fn start_dispatch_streams(
    sources: Vec<Source>,
    connections: Vec<Connection>,
) -> Vec<Option<tokio::task::JoinHandle<Result<(), ()>>>> {
    sources
        .into_iter()
        .map(
            |source| -> Option<tokio::task::JoinHandle<Result<(), ()>>> {
                // Filter connections to this source
                let source_connections: Vec<Connection> = connections
                    .iter()
                    .filter(|c| c.source_name == source.name)
                    .cloned()
                    .collect();

                debug!(
                    "Spawing local event dispatcher task for source '{}'",
                    &source.name
                );

                Some(tokio::spawn(dispatch_stream(source, source_connections)))
            },
        )
        .collect()
}

pub async fn run(settings: settings::Settings) -> Result<(), Error> {
    // List of targets with their file event channels
    let targets: Arc<Mutex<HashMap<String, Arc<Target>>>> = Arc::new(Mutex::new(HashMap::new()));

    // List of sources with their file event channels
    let mut sources: Vec<Source> = Vec::new();

    // Stop orchestrator
    let stop: Arc<Mutex<Stop>> = Arc::new(Mutex::new(Stop::new()));

    let postgres_config: postgres::Config = settings.postgresql.url.parse()?;

    let connection_manager = PostgresConnectionManager::new(postgres_config, NoTls);

    let postgres_config: tokio_postgres::Config = settings.postgresql.url.parse()?;

    let tokio_connection_manager =
        bb8_postgres::PostgresConnectionManager::new(postgres_config, tokio_postgres::NoTls);

    let tokio_persistence = PostgresAsyncPersistence::new(tokio_connection_manager).await;

    tokio::spawn(target_directory_handler(
        tokio_persistence,
        settings.clone(),
        stop.clone(),
        targets.clone(),
    ));

    let persistence = PostgresPersistence::new(connection_manager).map_err(|e| err_msg(e))?;

    let local_storage = LocalStorage::new(&settings.storage.directory, persistence.clone());

    let (local_intake_sender, local_intake_receiver) = std::sync::mpsc::channel();

    let mut senders: HashMap<String, UnboundedSender<FileEvent>> = HashMap::new();

    settings
        .directory_sources
        .iter()
        .for_each(|directory_source| {
            let (sender, receiver) = unbounded_channel();

            sources.push(Source {
                name: directory_source.name.clone(),
                receiver: receiver,
            });

            senders.insert(directory_source.name.clone(), sender);
        });

    let event_dispatcher = EventDispatcher { senders: senders };

    // Create a lookup table for directory sources that can be used by the intake
    // thread
    let directory_source_map: HashMap<String, settings::DirectorySource> = (&settings
        .directory_sources)
        .into_iter()
        .map(|d| (d.name.clone(), d.clone()))
        .collect();

    let (local_intake_handle, local_intake_stop_cmd) = start_local_intake_thread(
        local_intake_receiver,
        event_dispatcher,
        local_storage.clone(),
        directory_source_map,
    );

    match stop.lock() {
        Ok(mut guard) => guard.add_command(local_intake_stop_cmd),
        Err(e) => error!(
            "Could not lock the Stop Arc for adding the directory source stop command: {}",
            e
        ),
    }

    #[cfg(target_os = "linux")]
    let (directory_sources_join_handle, inotify_stop_cmd) = start_directory_sources(
        settings.directory_sources.clone(),
        local_intake_sender.clone(),
    );

    #[cfg(target_os = "linux")]
    match stop.lock() {
        Ok(mut guard) => guard.add_command(inotify_stop_cmd),
        Err(e) => error!(
            "Could not lock the Stop Arc for adding the inotify stop command: {}",
            e
        ),
    }

    let (directory_sweep_join_handle, sweep_stop_cmd) = start_directory_sweep(
        settings.directory_sources.clone(),
        local_intake_sender,
        settings.scan_interval,
    );

    match stop.lock() {
        Ok(mut guard) => guard.add_command(sweep_stop_cmd),
        Err(e) => error!(
            "Could not lock the Stop Arc for adding the sweep stop command: {}",
            e
        ),
    }

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();

    match stop.lock() {
        Ok(mut guard) => guard.add_command(Box::new(move || {
            stop_clone.swap(true, Ordering::Relaxed);
        })),
        Err(e) => error!(
            "Could not lock the Stop Arc for adding stop flag setting stop command: {}",
            e
        ),
    }

    let sftp_join_handles: Arc<Mutex<Vec<SftpJoinHandle>>> = Arc::new(Mutex::new(Vec::new()));

    let (sftp_source_senders, mut sftp_sources): (Vec<SftpSourceSend>, Vec<Source>) = settings
        .sftp_sources
        .iter()
        .map(|sftp_source| {
            let (cmd_sender, cmd_receiver) = bounded::<(u64, SftpDownload)>(10);
            let (file_event_sender, file_event_receiver) = unbounded_channel();
            let (stop_sender, stop_receiver) = oneshot::channel::<()>();

            stop.lock()
                .unwrap()
                .add_command(Box::new(move || match stop_sender.send(()) {
                    Ok(_) => (),
                    Err(e) => error!("[E02008] Error sending stop signal: {:?}", e),
                }));

            let sftp_source_send = SftpSourceSend {
                sftp_source: sftp_source.clone(),
                cmd_sender: cmd_sender,
                cmd_receiver: cmd_receiver,
                file_event_sender: file_event_sender,
                stop_receiver: stop_receiver,
            };

            let source = Source {
                name: sftp_source.name.clone(),
                receiver: file_event_receiver,
            };

            (sftp_source_send, source)
        })
        .unzip();

    sources.append(&mut sftp_sources);

    let _sftp_sources_join_handle = tokio::spawn(sftp_sources_handler(
        settings.clone(),
        sftp_join_handles.clone(),
        sftp_source_senders,
        stop_flag,
        local_storage,
        persistence,
    ));

    let connections = settings
        .connections
        .iter()
        .filter_map(|conn_conf| -> Option<Connection> {
            let target = match targets.lock() {
                Ok(guard) => match guard.get(&conn_conf.target) {
                    Some(target) => target.clone(),
                    None => {
                        error!("No target found matching name '{}'", &conn_conf.target);
                        return None;
                    }
                },
                Err(e) => {
                    error!("Could not lock the targets Arc for getting a target: {}", e);
                    return None;
                }
            };

            Some(Connection {
                source_name: conn_conf.source.clone(),
                target: target,
                filter: conn_conf.filter.clone(),
            })
        })
        .collect();

    // Start the streams that dispatch messages from sources to targets
    let _stream_join_handles = start_dispatch_streams(sources, connections);

    let signals = Signals::new(&[
        signal_hook::consts::signal::SIGHUP,
        signal_hook::consts::signal::SIGTERM,
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGQUIT,
    ])?;

    let signal_handler_join_handle = tokio::spawn(async move {
        let mut signals = signals.fuse();

        while let Some(signal) = signals.next().await {
            match signal {
                signal_hook::consts::signal::SIGHUP => {
                    // Reload configuration
                    // Reopen the log file
                }
                signal_hook::consts::signal::SIGTERM
                | signal_hook::consts::signal::SIGINT
                | signal_hook::consts::signal::SIGQUIT => {
                    // Shutdown the system;
                    match Arc::try_unwrap(stop.clone()) {
                        Ok(mutex) => {
                            let l_stop = mutex.into_inner().unwrap();

                            l_stop.stop();
                        }
                        Err(_) => {
                            error!("Could not get mutex from Arc, not able to stop all tasks");
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    });

    // Wait until all tasks have finished
    let _result = signal_handler_join_handle.await;

    info!("Tokio runtime shutdown");

    #[cfg(target_os = "linux")]
    wait_for(directory_sources_join_handle, "directory sources");

    wait_for(local_intake_handle, "local intake");

    wait_for(directory_sweep_join_handle, "directory sweep");

    Arc::try_unwrap(sftp_join_handles)
        .expect("still users of handles")
        .into_inner()
        .unwrap()
        .into_iter()
        .for_each(|jh| {
            wait_for(jh, "sftp download");
        });

    Ok(())
}

async fn dispatch_stream(mut source: Source, connections: Vec<Connection>) -> Result<(), ()> {
    while let Some(file_event) = source.receiver.recv().await {
        debug!(
            "FileEvent for {} connections, from {}: {}",
            connections.len(),
            &source.name,
            file_event.path.to_string_lossy()
        );

        connections
            .deref()
            .iter()
            .filter(|c| match &c.filter {
                Some(f) => f.file_matches(&file_event.path),
                None => true,
            })
            .for_each(|c| {
                info!("Sending FileEvent to target {}", &c.target.name);

                let send_result = c.target.sender.send(file_event.clone());

                match send_result {
                    Ok(_) => (),
                    Err(e) => {
                        // Could not send file event to target
                        // TODO: Implement retry mechanism
                        error!("Could not send event to target handler: {}", e);
                    }
                }
            });
    }

    debug!("End of dispatch stream '{}'", &source.name);

    Ok(())
}
