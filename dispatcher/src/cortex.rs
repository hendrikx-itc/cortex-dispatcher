use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::path::{PathBuf};

extern crate inotify;

extern crate actix;
use actix::prelude::*;
use actix::{Addr};

extern crate failure;
extern crate lapin_futures;

use crate::amqp_consumer::start_consumer;
use crate::settings;
use crate::command_handler::CommandHandler;
use crate::sftp_downloader::{SftpDownloader, SftpDownloadDispatcher};
use crate::local_source::start_local_source_handler;

use cortex_core::sftp_connection::SftpConnection;

use prometheus;

fn start_sftp_downloader(sftp_source: settings::SftpSource, data_dir: PathBuf, db_url: String) -> Addr<SftpDownloader> {
    SyncArbiter::start(sftp_source.thread_count, move || {
        let conn = loop {
            let conn_result = SftpConnection::new(
                &sftp_source.address.clone(),
                &sftp_source.username.clone(),
                sftp_source.compress
            );

            match conn_result {
                Ok(c) => break c,
                Err(e) => error!("Could not connect: {}", e)
            }

            thread::sleep(Duration::from_millis(1000));
        };

        let db_conn_result = postgres::Connection::connect(db_url.clone(), postgres::TlsMode::None);

        let db_conn = match db_conn_result {
            Ok(c) => {
                info!("Connected to database");
                c
            },
            Err(e) => {
                error!("Error connecting to database: {}", e);
                ::std::process::exit(2);
            }
        };

        SftpDownloader {
            sftp_source: sftp_source.clone(),
            sftp_connection: conn,
            db_connection: db_conn,
            local_storage_path: data_dir.clone()
        }
    })
}

fn start_sftp_downloaders(sftp_sources: Vec<settings::SftpSource>, data_dir: PathBuf, db_url: String) -> HashMap<String, Addr<SftpDownloader>> {
    sftp_sources
        .iter()
        .map(|sftp_source| {
            (sftp_source.name.clone(), start_sftp_downloader(sftp_source.clone().clone(), data_dir.clone(), db_url.clone()))
        })
        .collect()
}

pub fn run(settings: settings::Settings) {
    let system = actix::System::new("cortex");

    let downloaders_map = start_sftp_downloaders(
        settings.sftp_sources.clone(),
        settings.storage.directory.clone(),
        settings.postgresql.url.clone()
    );

    let sftp_download_dispatcher = SftpDownloadDispatcher { downloaders_map };

    let local_source_handler_join_handle = start_local_source_handler(settings.directory_sources.clone());

    let command_handler = CommandHandler { sftp_download_dispatcher };

    let metrics_collector_join_handle = start_metrics_collector(
        settings.prometheus.push_gateway.clone(),
        settings.prometheus.push_interval
    );

    let consumer_join_handle = start_consumer(
        settings.command_queue.address.clone(),
        settings.command_queue.queue_name.clone(),
        command_handler
    );

    system.run();

    metrics_collector_join_handle.join().unwrap();
    consumer_join_handle.join().unwrap();
    local_source_handler_join_handle.join().unwrap();
}

fn start_metrics_collector(address: String, push_interval: u64) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(push_interval));

            let metric_families = prometheus::gather();
            let push_result = prometheus::push_metrics(
                "cortex-dispatcher",
                labels! {},
                &address,
                metric_families,
                Some(prometheus::BasicAuthentication {
                    username: "user".to_owned(),
                    password: "pass".to_owned(),
                }),
            );
            
            match push_result {
                Ok(_) => {
                    debug!("Pushed metrics to Prometheus Gateway");
                },
                Err(e) => {
                    error!("Error pushing metrics to Prometheus Gateway: {}", e);
                }
            }
        }
    })
}
