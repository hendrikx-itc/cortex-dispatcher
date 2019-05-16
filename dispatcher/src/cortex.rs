use std::thread;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
extern crate inotify;

extern crate failure;
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
use crate::base_types::Source;
use crate::metrics_collector::metrics_collector;
use crate::directory_target::DirectoryTarget;
use crate::http_server::start_http_server;
use crate::persistence::{Persistence, PostgresPersistence};
use crate::event::FileEvent;


pub fn run(settings: settings::Settings) {
    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let directory_targets: Arc<HashMap<String, DirectoryTarget>> = Arc::new(settings.directory_targets.iter().map(|target| {
        (target.name.clone(), DirectoryTarget::from_settings(target, &mut runtime))
    }).collect());

    let prometheus_push_conf = settings.prometheus_push.clone();

    match prometheus_push_conf {
        Some(conf) => {
            runtime.spawn(metrics_collector(
                conf.gateway.clone(),
                conf.interval,
            ));

            info!("Prometheus metrics push collector configured");
        },
        None => {}
    };

    let (directory_sources_join_handle, mut directory_sources) = start_directory_sources(settings.directory_sources.clone());

    let connections = settings.connections.clone();

    let connection_manager = PostgresConnectionManager::new(
        settings.postgresql.url.parse().unwrap(),
        NoTls,
    );

    let persistence = PostgresPersistence::new(connection_manager);

    let mut source_sender_pairs: Vec<(settings::SftpSource, UnboundedSender<FileEvent>)> = Vec::new();
    let mut sources: Vec<Source> = Vec::new();
    
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

    let local_event_dispatchers = sources.into_iter().map(|directory_source| {
        // Filter connections belonging to this source
        let source_connections: Vec<settings::Connection> = connections
            .iter().filter(|c| c.source == directory_source.name).cloned().collect();

        let targets = directory_targets.clone();

        let source_name = directory_source.name.clone();

        directory_source.receiver.map_err(|_| ()).for_each(move |file_event| {
            info!("FileEvent from {}: {}", &source_name, file_event.path.to_str().unwrap());

            source_connections.deref().iter().filter(|c| c.filter.event_matches(&file_event)).for_each(|c| {
                let target = targets.get(&c.target).unwrap();

                let mut s = target.sender.clone();
                s.try_send(file_event.clone()).unwrap();
            });

            futures::future::ok(())
        })
    });

    for p in local_event_dispatchers {
        runtime.spawn(p);
    }

    let web_server_join_handle = start_http_server(settings.http_server.address);

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
