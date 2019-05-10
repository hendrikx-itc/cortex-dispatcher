use std::ops::Deref;
use std::sync::Arc;
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

use crate::local_source::start_local_source_handler;
use crate::settings;
use crate::sftp_downloader::{SftpDownloader};
use crate::metrics_collector::metrics_collector;
use crate::directory_target::DirectoryTarget;


pub fn run(settings: settings::Settings) {
    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let directory_targets: Arc<HashMap<String, DirectoryTarget>> = Arc::new(settings.directory_targets.iter().map(|target| {
        (target.name.clone(), DirectoryTarget::from_settings(target, &mut runtime))
    }).collect());

    runtime.spawn(metrics_collector(
        settings.prometheus.push_gateway.clone(),
        settings.prometheus.push_interval,
    ));

    let (local_source_handler_join_handle, local_directory_receivers) = start_local_source_handler(settings.directory_sources.clone());

    let connections = settings.connections.clone();

    for (source_name, receiver) in local_directory_receivers {
        let c = connections.clone();
        let targets = directory_targets.clone();

        let process_events = receiver.map_err(|_| ()).for_each(move |file_event| {
            info!("FileEvent for {}: {}", &source_name, file_event.path.to_str().unwrap());

            c.deref().iter().filter(|c| c.filter.event_matches(&file_event)).for_each(|c| {
                let target = targets.get(&c.target).unwrap();

                let mut s = target.sender.clone();
                s.try_send(file_event.clone()).unwrap();
            });

            futures::future::ok(())
        });

        runtime.spawn(process_events);
    }

    // Connect to RabbitMQ and when the connection is made, start all SFTP
    // downloaders that consume commands from it.
    let connect_future = TcpStream::connect(&settings.command_queue.address).map_err(failure::Error::from).and_then(|stream| {
        lapin_futures::client::Client::connect(stream, ConnectionOptions::default()).map_err(failure::Error::from)
    }).and_then(|(client, heartbeat)| {
        tokio::spawn(heartbeat.map_err(|e| {
            error!("Error sending heartbeat: {}", e);
        }));

        for sftp_source in settings.sftp_sources {
            let (join_handle, receiver) = SftpDownloader::start(
                client.clone(),
                sftp_source.clone(),
                settings.storage.directory.clone(),
                settings.postgresql.url.clone()
            );

            let process_events = receiver.map_err(|_| ()).for_each(move |file_event| {
                info!("FileEvent for {}: {}", &sftp_source.name, file_event.path.to_str().unwrap());

                futures::future::ok(())
            });

            tokio::spawn(process_events);
        }

        futures::future::ok(())
    }).map_err(|_| ());

    runtime.spawn(connect_future);

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    local_source_handler_join_handle.join().unwrap();
}

