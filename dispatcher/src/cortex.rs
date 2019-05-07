use std::thread;

extern crate inotify;

extern crate failure;
extern crate lapin_futures;
use lapin_futures::client::ConnectionOptions;

extern crate tokio_executor;
use tokio_executor::enter;
use tokio::net::TcpStream;

use futures::future::Future;

use crate::local_source::start_local_source_handler;
use crate::settings;
use crate::sftp_downloader::{SftpDownloader};
use crate::metrics_collector::metrics_collector;


pub fn run(settings: settings::Settings) {
    let (local_source_handler_join_handle, receivers) = start_local_source_handler(settings.directory_sources.clone());

    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.spawn(metrics_collector(
        settings.prometheus.push_gateway.clone(),
        settings.prometheus.push_interval,
    ));

    let connect_future = TcpStream::connect(&settings.command_queue.address).map_err(failure::Error::from).and_then(|stream| {
        lapin_futures::client::Client::connect(stream, ConnectionOptions::default()).map_err(failure::Error::from)
    }).and_then(move |(client, heartbeat)| {
        tokio::spawn(heartbeat.map_err(|e| {
            error!("Error sending heartbeat: {}", e);
        }));

        let join_handles: Vec<thread::JoinHandle<()>> = settings.sftp_sources.iter().map(|sftp_source| {
            SftpDownloader::start(
                client.clone(),
                sftp_source.clone(),
                settings.storage.directory.clone(),
                settings.postgresql.url.clone()
            )
        }).collect();

        futures::future::ok(())
    }).map_err(|_| ());
    
    runtime.spawn(connect_future);

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    local_source_handler_join_handle.join().unwrap();
}

