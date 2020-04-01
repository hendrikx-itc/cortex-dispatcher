use std::thread;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "linux")]
extern crate inotify;

use failure::{Error, err_msg};

extern crate lapin;
use lapin::{ConnectionProperties};

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tokio::stream::StreamExt;

use futures::task::Context;
use futures_util::compat::Compat01As03;

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use signal_hook;
use signal_hook::iterator::Signals;

use crossbeam_channel::{bounded, Sender, Receiver};

use cortex_core::{wait_for, SftpDownload, StopCmd};

use crate::base_types::{Connection, RabbitMQNotify, Target, CortexConfig, Notify, Source};

use crate::directory_source::{start_directory_sweep, start_local_intake_thread};
#[cfg(target_os = "linux")]
use crate::directory_source::start_directory_sources;

use crate::directory_target::to_stream;
use crate::event::{FileEvent, EventDispatcher};
use crate::http_server::start_http_server;
use crate::persistence::PostgresPersistence;
use crate::settings;
use crate::sftp_downloader;
use crate::sftp_command_consumer;
use crate::base_types::MessageResponse;
use crate::local_storage::LocalStorage;

fn stream_consuming_future(
    mut stream: impl futures::Stream<Item = FileEvent> + Send + std::marker::Unpin,
) -> impl futures::Future<Output=()> + Send {
    async move {
        while let Some(_) = tokio::stream::StreamExt::next(&mut stream).await {
        }
    }
}

async fn connect_channel(
    addr: &str,
) -> Result<lapin::Channel, lapin::Error> {
    let connect_result = lapin::Connection::connect(
        addr,
        lapin::ConnectionProperties::default(),
    ).await;

    let connection = connect_result.unwrap();

    connection.create_channel().await
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
}

pub fn run(settings: settings::Settings) -> Result<(), Error> {
    let mut runtime = tokio::runtime::Runtime::new()?;

    // List of targets with their file event channels
    let mut targets: HashMap<String, Arc<Target>> = HashMap::new();

    // List of sources with their file event channels
    let mut sources: Vec<Source> = Vec::new();

    let connections: Arc<Mutex<Vec<Connection>>> = Arc::new(Mutex::new(Vec::new()));

    // Stop orchestrator
    let stop: Arc<Mutex<Stop>> = Arc::new(Mutex::new(Stop::new()));

    settings.directory_targets.iter().for_each(|target_conf| {
        let (mut stream, stop_cmd, target) = setup_directory_target(target_conf);

        stop.lock().unwrap().add_command(stop_cmd);

        runtime.spawn(async move {
            while let Some(file_event) = stream.next().await {

            }
        });

        targets.insert(target_conf.name.clone(), target);
    });

    let connection_manager =
        PostgresConnectionManager::new(settings.postgresql.url.parse().unwrap(), NoTls);

    let persistence = PostgresPersistence::new(connection_manager);

    let local_storage = LocalStorage::new(&settings.storage.directory, persistence.clone());

    let (local_intake_sender, local_intake_receiver) = std::sync::mpsc::channel();

    let mut senders: HashMap<String, UnboundedSender<FileEvent>> = HashMap::new();

    settings.directory_sources.iter().for_each(|directory_source| { 
        let (sender, receiver) = unbounded_channel();
        
        sources.push(
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

    let (local_intake_handle, local_intake_stop_cmd) = start_local_intake_thread(local_intake_receiver, event_dispatcher, local_storage.clone());

    stop.lock().unwrap().add_command(local_intake_stop_cmd);

    #[cfg(target_os = "linux")]
    let (directory_sources_join_handle, inotify_stop_cmd) =
        start_directory_sources(settings.directory_sources.clone(), local_intake_sender.clone());

    #[cfg(target_os = "linux")]
    stop.lock().unwrap().add_command(inotify_stop_cmd);

    let (directory_sweep_join_handle, sweep_stop_cmd) = start_directory_sweep(
        settings.directory_sources.clone(),
        local_intake_sender,
        settings.scan_interval
    );

    stop.lock().unwrap().add_command(sweep_stop_cmd);

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

    struct SftpSourceSend {
        pub sftp_source: settings::SftpSource,
        pub cmd_sender: Sender<(u64, SftpDownload)>,
        pub cmd_receiver: Receiver<(u64, SftpDownload)>,
        pub file_event_sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        pub stop_receiver: oneshot::Receiver<()>
    }

    let mut sftp_source_senders: Vec<SftpSourceSend> = Vec::new();    
    
    settings.sftp_sources.iter().for_each(|sftp_source| {
        let (cmd_sender, cmd_receiver) = bounded::<(u64, SftpDownload)>(1000);
        let (file_event_sender, file_event_receiver) = unbounded_channel();
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        stop.lock().unwrap().add_command(Box::new(move || {
            match stop_sender.send(()) {
                Ok(_) => (),
                Err(e) => error!("[E02008] Error sending stop signal: {:?}", e)
            }
        }));

        sftp_source_senders.push(
            SftpSourceSend {
                sftp_source: sftp_source.clone(),
                cmd_sender: cmd_sender,
                cmd_receiver: cmd_receiver,
                file_event_sender: file_event_sender,
                stop_receiver: stop_receiver,
            }
        );

        sources.push(Source {
            name: sftp_source.name.clone(),
            receiver: file_event_receiver
        });    
    });

    let jhs = sftp_join_handles.clone();

    let l_settings = settings.clone();

    let sftp_sources_join_handle = runtime.spawn(async move {
        let conn_result = lapin::Connection::connect(
            &l_settings.command_queue.address,
            ConnectionProperties::default(),
        ).await;

        let client = conn_result.unwrap();

        sftp_source_senders.into_iter().for_each(|channels| {
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

            let stream = sftp_command_consumer::start(
                client.clone(),
                channels.sftp_source.name.clone(),
                ack_receiver, channels.cmd_sender.clone()
            );

            debug!("Spawning AMQP stream");
            tokio::spawn(stream);
        });

        sources.into_iter().for_each(|mut source| {
            // Filter connections belonging to this source
            let source_connections: Vec<Connection> = connections.lock().unwrap()
                .iter()
                .filter(|c| c.source_name == source.name)
                .cloned()
                .collect();

            let source_name = source.name.clone();

            let stream_future = async move {
                while let Some(file_event) = tokio::stream::StreamExt::next(&mut source.receiver).await {
                    debug!(
                        "FileEvent from {}: {}",
                        &source.name,
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
                            let s = c.target.sender.clone();
                            let send_result = s.send(file_event.clone());
    
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
            };

            debug!("Spawing local event dispatcher for {}", &source_name);

            tokio::spawn(stream_future);
        });
    });

    let (web_server_join_handle, actix_system, actix_http_server) = start_http_server(
        settings.http_server.address,
    );

    stop.lock().unwrap().add_command(Box::new(move || {
        tokio::spawn(actix_http_server.stop(true));
    }));

    stop.lock().unwrap().add_command(Box::new(move || {
        actix_system.stop();
    }));

    let spawn_signal_handler = futures::future::poll_fn(move |_cx: &mut Context<'_>| -> futures::task::Poll<Result<(), Error>> {
        let s_stop = Arc::clone(&stop);

        let unwrap_result = Arc::try_unwrap(s_stop);

        match unwrap_result {
            Ok(unwrapped) => {
                tokio::spawn(setup_signal_handler(unwrapped.into_inner().unwrap()));
                futures::task::Poll::Ready(Ok(()))
            },
            Err(_) => {
                futures::task::Poll::Ready(Err(err_msg("Could not unwrap Arc")))
            }
        }
    });

    runtime.spawn(spawn_signal_handler);

    // Wait until all tasks have finished
    runtime
        .block_on(sftp_sources_join_handle)
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    #[cfg(target_os = "linux")]
    wait_for(directory_sources_join_handle, "directory sources");

    wait_for(web_server_join_handle, "http server");

    wait_for(local_intake_handle, "local intake");

    wait_for(directory_sweep_join_handle, "directory sweep");

    Arc::try_unwrap(sftp_join_handles).expect("still users of handles").into_inner().unwrap().into_iter().for_each(|jh| {
        wait_for(jh, "sftp download");
    });

    Ok(())
}

fn setup_signal_handler(stop: Stop) -> impl futures::future::Future<Output=()> + Send + 'static {
    let signals = Signals::new(&[
        signal_hook::SIGHUP,
        signal_hook::SIGTERM,
        signal_hook::SIGINT,
        signal_hook::SIGQUIT,
    ]).unwrap();

    async {
        let mut signal_stream = Compat01As03::new(signals.into_async().unwrap());

        while let Ok(signal) = tokio::stream::StreamExt::try_next(&mut signal_stream).await {
            if let Some(s) = signal {
                info!("signal: {}", s);
                //local_stop.stop();  
            }
        }
    }
}

fn setup_directory_target(target_conf: &settings::DirectoryTarget) -> (impl tokio::stream::Stream<Item=FileEvent>, StopCmd, Arc<Target>) {
    let (sender, receiver) = unbounded_channel();

    let target_stream = to_stream(target_conf, receiver);

    // let target_stream = match &target_conf.notify {
    //     Some(conf) => match conf {
    //         settings::Notify::RabbitMQ(notify_conf) => async {
    //             debug!("Connecting notifier to directory target stream");

    //             let amqp_channel_result = connect_channel(&notify_conf.address).await;
    //             let amqp_channel = amqp_channel_result.unwrap();

    //             let message_template = notify_conf.message_template.clone();
    //             let exchange = notify_conf.exchange.clone();
    //             let routing_key = notify_conf.routing_key.clone();

    //             let address = notify_conf.address.clone();

    //             debug!("Notifying on AMQP queue");

    //             let notify = RabbitMQNotify {
    //                 message_template: message_template,
    //                 channel: amqp_channel,
    //                 exchange: exchange,
    //                 routing_key: routing_key,
    //             };

    //             stream_consuming_future(notify.and_then_notify(target_stream))
    //         }
    //     },
    //     None => stream_consuming_future(target_stream),
    // };

    let (stop_sender, _stop_receiver) = oneshot::channel::<()>();

    let name = target_conf.name.clone();

    let stop_cmd = Box::new(move || {
        stop_sender.send(()).unwrap();
    });

    let target = Arc::new(Target {
        name: target_conf.name.clone(),
        sender: sender,
    });

    (target_stream, stop_cmd, target)
}

