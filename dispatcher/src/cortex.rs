use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::path::{PathBuf};

extern crate inotify;

use crossbeam_channel::unbounded;

extern crate failure;
extern crate lapin_futures;

use tokio::prelude::*;
use tokio::timer::Interval;

extern crate tokio_executor;
use tokio_executor::enter;

use crate::amqp_consumer::setup_consumer;
use crate::settings;
use crate::command_handler::CommandHandler;
use crate::sftp_downloader::{SftpDownloader, SftpDownloadDispatcher, Download};
use crate::local_source::start_local_source_handler;

use cortex_core::sftp_connection::SftpConnection;

use prometheus;

fn start_sftp_downloader(sftp_source: settings::SftpSource, data_dir: PathBuf, db_url: String) -> (crossbeam_channel::Sender<Download>, thread::JoinHandle<()>) {
    let (s, r) = unbounded();

    let join_handle = thread::spawn(move || {
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

        let mut sftp_downloader = SftpDownloader {
            sftp_source: sftp_source.clone(),
            sftp_connection: conn,
            db_connection: db_conn,
            local_storage_path: data_dir.clone()
        };

        loop {
            let recv_result = r.recv();

            match recv_result {
                Ok(msg) => {
                    info!("{:?}", msg);

                    sftp_downloader.handle(msg);
                },
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
    });

    (s, join_handle)
}

fn start_sftp_downloaders(sftp_sources: Vec<settings::SftpSource>, data_dir: PathBuf, db_url: String) -> HashMap<String, (crossbeam_channel::Sender<Download>, thread::JoinHandle<()>)> {
    sftp_sources
        .iter()
        .map(|sftp_source| {
            (
                sftp_source.name.clone(),
                start_sftp_downloader(sftp_source.clone().clone(), data_dir.clone(), db_url.clone())
            )
        })
        .collect()
}

pub fn run(settings: settings::Settings) {
    let downloaders_map = start_sftp_downloaders(
        settings.sftp_sources.clone(),
        settings.storage.directory.clone(),
        settings.postgresql.url.clone()
    );

    let sftp_download_dispatcher = SftpDownloadDispatcher { downloaders_map };

    let local_source_handler_join_handle = start_local_source_handler(settings.directory_sources.clone());

    let command_handler = CommandHandler { sftp_download_dispatcher };

    let mut entered = enter().expect("failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    //runtime.spawn(metrics_collector(
    //    settings.prometheus.push_gateway.clone(),
    //    settings.prometheus.push_interval
    //));

    runtime.spawn(setup_consumer(
        settings.command_queue.address,
        settings.command_queue.queue_name.clone(),
        command_handler
    ));

    entered.block_on(runtime.shutdown_on_idle()).expect("shutdown cannot error");

    info!("Tokio Runtime shutdown");

    local_source_handler_join_handle.join().unwrap();
}

fn metrics_collector(address: String, push_interval: u64) -> impl Future< Item = (), Error = () > {
    Interval::new_interval(Duration::from_millis(push_interval)).for_each(move |_| {
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
        };

        future::ok(())
    }).map_err(|e| {
        error!("{}", e);
        ()
    })
}
