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

use futures::future::Future;

use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;

use crate::directory_source::start_directory_sources;
use crate::settings;
use crate::sftp_source::{SftpDownloader};
use crate::metrics_collector::metrics_collector;
use crate::directory_target::DirectoryTarget;
use crate::base_types::Source;
use crate::http_server::start_http_server;
use crate::persistence;


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

    let (directory_sources_join_handle, directory_sources) = start_directory_sources(settings.directory_sources.clone());

    let connections = settings.connections.clone();

    let local_event_dispatchers = directory_sources.into_iter().map(|directory_source| {
        let c = connections.clone();
        let targets = directory_targets.clone();

        let source_name = directory_source.name.clone();

        directory_source.receiver.map_err(|_| ()).for_each(move |file_event| {
            info!("FileEvent from {}: {}", &source_name, file_event.path.to_str().unwrap());

            c.deref().iter().filter(|c| c.filter.event_matches(&file_event)).for_each(|c| {
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

    let (sftp_join_handles, connect_future) = connect_sftp_downloaders(
        settings.command_queue.address,
        settings.sftp_sources.clone(),
        settings.storage.directory.clone(),
        settings.postgresql.url.clone()
    );

    runtime.spawn(connect_future);

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
fn connect_sftp_downloaders(
        rabbitmq_address: std::net::SocketAddr,
        sftp_sources: Vec<settings::SftpSource>,
        storage_directory: std::path::PathBuf,
        postgresql_url: String
    ) -> (Arc<Mutex<Vec<thread::JoinHandle<()>>>>, impl Future<Item=(), Error=()>) {
    let join_handles = Arc::new(Mutex::new(Vec::new()));
    let join_handles_result = join_handles.clone();

    let connection_manager = PostgresConnectionManager::new(
        postgresql_url.parse().unwrap(),
        NoTls,
    );

    let persistence = persistence::PostgresPersistence::new(connection_manager);

    let stream = TcpStream::connect(&rabbitmq_address).map_err(failure::Error::from).and_then(|stream| {
        lapin_futures::client::Client::connect(stream, ConnectionOptions::default()).map_err(failure::Error::from)
    }).and_then(move |(client, heartbeat)| {
        tokio::spawn(heartbeat.map_err(|e| {
            error!("Error sending heartbeat: {}", e);
        }));

        for config in sftp_sources {
            let (join_handle, sftp_source) = SftpDownloader::start(
                client.clone(),
                config.clone(),
                storage_directory.clone(),
                persistence.clone()
            );

            {
                let mut jhs = join_handles.lock().unwrap();

                jhs.push(join_handle);
            }

            let sftp_source_name = config.name.clone();

            let process_events = sftp_source.events()
                .map_err(|e| {
                    error!("{}", e);
                })
                .for_each(move |file_event| {
                    info!("FileEvent from {}: {}", &sftp_source_name, file_event.path.to_str().unwrap());

                    futures::future::ok(())
                });

            tokio::spawn(process_events);

            debug!("Spawned MQ message handling stream");
        }

        futures::future::ok(())
    }).map_err(|e| {
        error!("{}", e);
    });

    (join_handles_result, stream)
}
