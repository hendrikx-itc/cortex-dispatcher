use std::collections::HashMap;
use std::path::PathBuf;
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

fn start_sftp_downloaders(
    amqp_client: lapin_futures::client::Client<TcpStream>,
    sftp_sources: Vec<settings::SftpSource>,
    data_dir: PathBuf,
    db_url: String,
) -> HashMap<String, thread::JoinHandle<()>> {
    sftp_sources
        .iter()
        .map(|sftp_source| {
            (
                sftp_source.name.clone(),
                SftpDownloader::start(
                    amqp_client.clone(),
                    sftp_source.clone().clone(),
                    data_dir.clone(),
                    db_url.clone(),
                ),
            )
        })
        .collect()
}

pub fn run(settings: settings::Settings) {
    let local_source_handler_join_handle =
        start_local_source_handler(settings.directory_sources.clone());

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

        start_sftp_downloaders(
            client,
            settings.sftp_sources.clone(),
            settings.storage.directory.clone(),
            settings.postgresql.url.clone(),
        );

        futures::future::ok(())
    }).map_err(|_| ());
    
    runtime.spawn(connect_future);

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    info!("Tokio runtime shutdown");

    local_source_handler_join_handle.join().unwrap();
}
