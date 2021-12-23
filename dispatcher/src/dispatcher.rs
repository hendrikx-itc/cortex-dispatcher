use futures::future::join_all;
use std::thread;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::iter::Iterator;

#[cfg(target_os = "linux")]
extern crate inotify;

use failure::{Error, err_msg};

extern crate lapin;
use lapin::{ConnectionProperties};

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;

use futures::stream::StreamExt;

use postgres::NoTls;

use signal_hook_tokio::Signals;

use cortex_core::{wait_for, SftpDownload, StopCmd};

use crate::base_types::{Connection, RabbitMQNotify, Target, Source};

use crate::directory_source::{start_directory_sweep, start_local_intake_loop};
#[cfg(target_os = "linux")]
use crate::directory_source::start_directory_sources;

use crate::directory_target::handle_file_event;
use crate::event::{FileEvent, EventDispatcher};
use crate::persistence::{Persistence};
use crate::settings;
use crate::sftp_downloader;
use crate::sftp_command_consumer;
use crate::base_types::MessageResponse;
use crate::local_storage::LocalStorage;

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

pub async fn run(settings: settings::Settings) -> Result<(), Error> {
    // List of targets with their file event channels
    let targets: Arc<Mutex<HashMap<String, Arc<Target>>>> = Arc::new(Mutex::new(HashMap::new()));

    // List of sources with their file event channels
    let mut sources: Vec<Source> = Vec::new();

    let connections: Arc<Mutex<Vec<Connection>>> = Arc::new(Mutex::new(Vec::new()));

    // Stop orchestrator
    let stop: Arc<Mutex<Stop>> = Arc::new(Mutex::new(Stop::new()));

    let connection_manager =
        bb8_postgres::PostgresConnectionManager::new(settings.postgresql.url.parse().unwrap(), NoTls);

    let t_settings = settings.clone();

    let directory_target_stop = stop.clone();

    let directory_target_targets = targets.clone();

    let persistence = Persistence::new(connection_manager)
        .await
        .map_err(|e| err_msg(format!("Error initializing persistence: {}", e)) )?;

    tokio::spawn(async move {
        let persistence = persistence.clone();

        t_settings.directory_targets.iter().for_each(|target_conf| {
            let persistence = persistence.clone();
            let (sender, mut receiver) = unbounded_channel();

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
                            ).await;
                        
                            let connection = match connect_result {
                                Ok(c) => c,
                                Err(e) => {
                                    error!("Error connecting to AMQP server: {}", e);
                                    return
                                }
                            };
                        
                            let amqp_channel_result = connection.create_channel().await;

                            let amqp_channel = match amqp_channel_result {
                                Ok(c) => c,
                                Err(e) => {
                                    error!("Error creating AMQP channel: {}", e);
                                    return
                                }
                            };
                                        
                            let notify = RabbitMQNotify {
                                message_template: notify_conf.message_template.clone(),
                                //channel: amqp_channel,
                                exchange: notify_conf.exchange.clone(),
                                routing_key: notify_conf.routing_key.clone(),
                            };

                            let routing_key = notify_conf.routing_key.clone();
        
                            while let Some(file_event) = receiver.recv().await {        
                                match handle_file_event(&d_target_conf, file_event, persistence.clone()).await {
                                    Ok(result_event) => {
                                        debug!("Notifying with AMQP routing key {}", &routing_key);
        
                                        notify.notify(&amqp_channel, result_event).await;        
                                    },
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
                            if let Err(e) = handle_file_event(&d_target_conf, file_event, persistence.clone()).await {
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
                },
            };
        
            let stop_cmd_name = c_target_conf.name.clone();
        
            let stop_cmd = Box::new(move || {
                let send_result = stop_sender.send(());
        
                match send_result {
                    Ok(_) => debug!("Stop command sent for directory target '{}'", &stop_cmd_name),
                    Err(e) => debug!("Error sending stop command for directory target '{}': {:?}", &stop_cmd_name, e)
                }
            });
        
            let target = Arc::new(Target {
                name: c_target_conf.name.clone(),
                sender: sender,
            });
                    
            directory_target_stop.lock().unwrap().add_command(stop_cmd);
        
            directory_target_targets.lock().unwrap().insert(target_conf.name.clone(), target);
        });
    });

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

    tokio::spawn(start_local_intake_loop(local_intake_receiver, event_dispatcher, local_storage.clone()));

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
            let target = targets.lock().unwrap().get(&conn_conf.target).unwrap().clone();

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

    type SftpJoinHandle = thread::JoinHandle<std::result::Result<(), sftp_downloader::Error>>;

    let sftp_join_handles: Arc<Mutex<Vec<SftpJoinHandle>>> = Arc::new(Mutex::new(Vec::new()));

    struct SftpSourceSend {
        pub sftp_source: settings::SftpSource,
        pub cmd_sender: tokio::sync::mpsc::Sender<(u64, SftpDownload)>,
        pub cmd_receiver: tokio::sync::mpsc::Receiver<(u64, SftpDownload)>,
        pub file_event_sender: tokio::sync::mpsc::UnboundedSender<FileEvent>,
        pub stop_receiver: oneshot::Receiver<()>
    }

    let (sftp_source_senders, mut sftp_sources): (Vec<SftpSourceSend>, Vec<Source>) = settings.sftp_sources.iter().map(|sftp_source| {
        let (cmd_sender, cmd_receiver) = tokio::sync::mpsc::channel::<(u64, SftpDownload)>(10);
        let (file_event_sender, file_event_receiver) = unbounded_channel();
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();

        stop.lock().unwrap().add_command(Box::new(move || {
            match stop_sender.send(()) {
                Ok(_) => (),
                Err(e) => error!("[E02008] Error sending stop signal: {:?}", e)
            }
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
            receiver: file_event_receiver
        };    

        (sftp_source_send, source)
    }).unzip();

    sources.append(&mut sftp_sources);

    let jhs = sftp_join_handles.clone();

    let l_settings = settings.clone();

    let _sftp_sources_join_handle = tokio::spawn(async move {
        debug!("Connecting to AMQP service at {}", &l_settings.command_queue.address);
        
        let amqp_client = lapin::Connection::connect(
            &l_settings.command_queue.address,
            ConnectionProperties::default(),
        ).await?;

        debug!("Connected to AMQP service");

        let mut stream_join_handles: Vec<tokio::task::JoinHandle<Result<(), sftp_command_consumer::ConsumeError>>> = Vec::new();
        
        for channels in sftp_source_senders {
            let (ack_sender, ack_receiver) = tokio::sync::mpsc::channel::<MessageResponse>(100);

            for n in 0..channels.sftp_source.thread_count {
                debug!("Starting SFTP download thread '{}'", &channels.sftp_source.name);

                let downloader = sftp_downloader::SftpDownloader::start(
                    channels.cmd_receiver,
                    ack_sender.clone(),
                    channels.sftp_source.clone(),
                    channels.file_event_sender.clone(),
                    local_storage.clone(),
                    persistence.clone(),
                );

                tokio::spawn(downloader);

                info!("Started SFTP download thread '{}' ({})", &channels.sftp_source.name, n + 1);
            }

            let amqp_channel = amqp_client.create_channel().await?;

            debug!("Spawning AMQP stream task '{}'", &channels.sftp_source.name);

            let consume_future = sftp_command_consumer::start(
                amqp_channel,
                channels.sftp_source.name.clone(),
                ack_receiver, channels.cmd_sender.clone()
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

        let dispatcher_join_handles: Vec<tokio::task::JoinHandle<Result<(), ()>>> = sources.into_iter().map(|source| -> tokio::task::JoinHandle<Result<(), ()>> {
            // Filter connections to this source
            let source_connections: Vec<Connection> = connections.lock().unwrap()
                .iter()
                .filter(|c| c.source_name == source.name)
                .cloned()
                .collect();

            debug!("Spawing local event dispatcher task for {}", &source.name);

            tokio::spawn(dispatch_stream(source, source_connections))
        }).collect();

        // Await on futures so that the AMQP connection does not get destroyed.
        let stream_results = join_all(stream_join_handles).await;

        Ok::<(), sftp_command_consumer::ConsumeError>(())
    });

    let signals = Signals::new(&[
        signal_hook::consts::signal::SIGHUP,
        signal_hook::consts::signal::SIGTERM,
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGQUIT,
    ]).unwrap();

    let signal_handler_join_handle = tokio::spawn(async move {
        let mut signals = signals.fuse();

        while let Some(signal) = signals.next().await {
            let l_stop = Arc::try_unwrap(stop.clone()).unwrap().into_inner().unwrap();

            match signal {
                signal_hook::consts::signal::SIGHUP => {
                    // Reload configuration
                    // Reopen the log file
                }
                signal_hook::consts::signal::SIGTERM | signal_hook::consts::signal::SIGINT | signal_hook::consts::signal::SIGQUIT => {
                    // Shutdown the system;
                    l_stop.stop();
                },
                _ => unreachable!(),
            }
        }
    });

    // Wait until all tasks have finished
    let _result = signal_handler_join_handle.await;

    info!("Tokio runtime shutdown");

    #[cfg(target_os = "linux")]
    wait_for(directory_sources_join_handle, "directory sources");

    wait_for(directory_sweep_join_handle, "directory sweep");

    Arc::try_unwrap(sftp_join_handles).expect("still users of handles").into_inner().unwrap().into_iter().for_each(|jh| {
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
            .filter(|c| {
                match &c.filter {
                    Some(f) => f.file_matches(&file_event.path),
                    None => true
                }
            })
            .for_each(|c| {
                info!("Sending FileEvent to target {}", &c.target.name);

                //let s = c.target.sender.clone();
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
