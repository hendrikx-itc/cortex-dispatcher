use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::lapin::{BasicProperties, ConnectionProperties};
use crate::lapin::options::BasicPublishOptions;
use env_logger;
use failure::Error;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver};
use futures::{Future, IntoFuture};
use lapin_futures as lapin;
use log::{debug, error, info};
use serde_json;
use tokio;
use tokio_executor::enter;

use signal_hook;
use signal_hook::iterator::Signals;

extern crate config;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate prometheus;

#[macro_use]
extern crate lazy_static;

extern crate chrono;
extern crate postgres;
extern crate serde_yaml;

extern crate cortex_core;
use cortex_core::SftpDownload;

mod cmd;
mod http_server;
mod metrics;
mod settings;
mod sftp_scanner;

use settings::Settings;

fn main() {
    let matches = cmd::app().get_matches();

    let mut env_logger_builder = env_logger::builder();

    // When run as a service no timestamps are logged, we expect the service manager to append
    // timestamps to the logs.
    if matches.is_present("service") {
        env_logger_builder.default_format_timestamp(false);
    }

    env_logger_builder.init();

    if matches.is_present("sample_config") {
        print!(
            "{}\n",
            serde_yaml::to_string(&settings::Settings::default()).unwrap()
        );
        ::std::process::exit(0);
    }

    let config_file = matches
        .value_of("config")
        .unwrap_or("/etc/cortex/sftp-scanner.yaml");

    let settings = load_settings(&config_file);

    let mut entered = enter().expect("Failed to claim thread");
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    // Will hold all functions that stop components of the SFTP scannner
    let mut stop_commands: Vec<Box<dyn FnOnce() -> () + Send + 'static>> = Vec::new();

    // Setup the channel that connects to the RabbitMQ queue for SFTP download commands.
    let (cmd_sender, cmd_receiver) = channel(4096);

    let stop = Arc::new(AtomicBool::new(false));

    let stop_clone = stop.clone();

    stop_commands.push(Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    }));

    // Start every configured scanner in it's own thread and have them send commands to the
    // command channel.
    let scanner_threads: Vec<thread::JoinHandle<()>> = settings
        .sftp_sources
        .clone()
        .into_iter()
        .map(|sftp_source| {
            sftp_scanner::start_scanner(
                stop.clone(),
                cmd_sender.clone(),
                settings.postgresql.url.clone(),
                sftp_source,
            )
        })
        .collect();

    let metrics_collector_join_handle = match settings.prometheus_push {
        Some(conf) => {
            let join_handle = start_metrics_collector(conf.gateway.clone(), conf.interval);

            info!("Metrics collector thread started");

            Some(join_handle)
        }
        None => Option::None,
    };

    // Start the built in web server that currently only serves metrics.
    let (web_server_join_handle, actix_system, actix_http_server) = http_server::start_http_server(settings.http_server.address);

    stop_commands.push(Box::new(move || {
        tokio::spawn(actix_http_server.stop(true));
    }));

    stop_commands.push(Box::new(move || {
        actix_system.stop();
    }));

    let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();

    stop_commands.push(Box::new(move || {
        stop_sender.send(()).unwrap();
    }));

    // Use a stream to connect the command channel to the AMQP queue.
    let future = channel_to_amqp(stop_receiver, cmd_receiver, &settings.command_queue.address);

    runtime.spawn(setup_signal_handler(stop_commands));

    runtime.spawn(future);

    entered
        .block_on(runtime.shutdown_on_idle())
        .expect("Shutdown cannot error");

    for scanner_thread in scanner_threads {
        let res = scanner_thread.join();

        match res {
            Ok(()) => {
                info!("scanner thread stopped");
            }
            Err(e) => error!("scanner thread stopped with error: {:?}", e),
        }
    }

    let res = web_server_join_handle.join();

    match res {
        Ok(()) => info!("http server thread stopped"),
        Err(e) => error!("http server thread stopped with error: {:?}", e),
    }

    if let Some(join_handle) = metrics_collector_join_handle {
        let res = join_handle.join();

        match res {
            Ok(()) => info!("metrics collector thread stopped"),
            Err(e) => error!("metrics collector thread stopped with error: {:?}", e),
        }
    }
}

fn setup_signal_handler(stop_commands: Vec<Box<dyn FnOnce() -> () + Send + 'static>>) -> impl Future<Item = (), Error = ()> + Send + 'static {
    let signals = Signals::new(&[
        signal_hook::SIGHUP,
        signal_hook::SIGTERM,
        signal_hook::SIGINT,
        signal_hook::SIGQUIT,
    ]).unwrap();

    let signal_stream = signals.into_async().unwrap().into_future();

    signal_stream
        .map(move |sig| {
            info!("signal: {}", sig.0.unwrap());

            for stop_command in stop_commands {
                stop_command();
            }
        })
        .map_err(|e| panic!("{}", e.0))
}

/// Connects a channel receiver to an AMQP queue.
fn channel_to_amqp(
    stop_receiver: tokio::sync::oneshot::Receiver<()>,
    receiver: Receiver<SftpDownload>,
    addr: &str,
) -> impl Future<Item = (), Error = ()> + Send + 'static {
    connect_channel(&addr)
        .map(move |channel| {
            let stream = receiver.for_each(move |cmd| {
                let command_str = serde_json::to_string(&cmd).unwrap();

                let exchange = "amq.direct";

                let routing_key = format!("source.{}", &cmd.sftp_source);

                let future = channel
                    .basic_publish(
                        exchange,
                        &routing_key,
                        command_str.as_bytes().to_vec(),
                        BasicPublishOptions::default(),
                        BasicProperties::default(),
                    )
                    .and_then(move |_request_result| {
                        debug!("Command sent: {}", cmd);
                        // No confirmation/ack is expected
                        Ok(())
                    })
                    .map_err(|e| {
                        error!("Error sending command: {:?}", e);
                    });

                tokio::spawn(future);

                Ok(())
            });

            let stoppable_stream = stream.into_future().select2(stop_receiver.into_future());

            tokio::spawn(stoppable_stream.map(|_result| debug!("End amqp command stream")).map_err(|_e| error!("Error: ")));
        })
        .and_then(|_| Ok(()))
        .map_err(|e| error!("Error: {:?}", e))
}

fn connect_channel(
    addr: &str,
) -> impl Future<Item = lapin::Channel, Error = Error> + Send + 'static {
    lapin::Client::connect(addr, ConnectionProperties::default())
        .map_err(Error::from)
        .and_then(|client| {
            client.create_channel().map_err(Error::from)
        })
}

fn load_settings(config_file: &str) -> Settings {
    info!("Loading configuration from file {}", config_file);

    let mut settings = config::Config::new();

    let merge_result = settings.merge(config::File::new(config_file, config::FileFormat::Yaml));

    match merge_result {
        Ok(_config) => {
            info!("Configuration loaded from file {}", config_file);
        }
        Err(e) => {
            error!("Error loading configuration: {}", e);
            ::std::process::exit(1);
        }
    }

    let into_result = settings.try_into();

    let settings: Settings = match into_result {
        Ok(s) => s,
        Err(e) => {
            error!("Error loading configuration: {}", e);
            ::std::process::exit(1);
        }
    };

    info!("Configuration loaded");

    settings
}

fn start_metrics_collector(address: String, push_interval: u64) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(push_interval));

        let metric_families = prometheus::gather();
        let push_result = prometheus::push_metrics(
            "cortex-sftp-scanner",
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
            }
            Err(e) => {
                error!("Error pushing metrics to Prometheus Gateway: {}", e);
            }
        }
    })
}
