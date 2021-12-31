use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use futures::stream::TryStreamExt;
use futures_util::compat::Compat01As03;
use log::{error, info};

use crossbeam_channel::bounded;

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
extern crate proctitle;
extern crate serde_yaml;

#[macro_use]
extern crate error_chain;

extern crate cortex_core;

use cortex_core::wait_for;

mod amqp_sender;
mod cmd;
mod http_server;
mod metrics;
mod settings;
mod sftp_scanner;

use sftp_scanner::Error;

// We'll put our errors in an `errors` module, and other modules in
// this crate will `use errors::*;` to get access to everything
// `error_chain!` creates.
mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {}
}

use settings::Settings;

fn main() {
    let matches = cmd::app().get_matches();

    let mut env_logger_builder = env_logger::builder();

    // When run as a service no timestamps are logged, we expect the service manager
    // to append timestamps to the logs.
    if matches.is_present("service") {
        env_logger_builder
            .format(|buf, record| writeln!(buf, "{}  {}", record.level(), record.args()));
    }

    env_logger_builder.init();

    if matches.is_present("sample_config") {
        println!(
            "{}",
            serde_yaml::to_string(&settings::Settings::default()).unwrap()
        );
        ::std::process::exit(0);
    }

    let config_file = matches
        .value_of("config")
        .unwrap_or("/etc/cortex/sftp-scanner.yaml");

    let settings = load_settings(&config_file);

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    // Will hold all functions that stop components of the SFTP scannner
    let mut stop_commands: Vec<Box<dyn FnOnce() -> () + Send + 'static>> = Vec::new();

    // Setup the channel that connects to the RabbitMQ queue for SFTP download
    // commands.
    let (cmd_sender, cmd_receiver) = bounded(4096);

    let stop = Arc::new(AtomicBool::new(false));

    let stop_clone = stop.clone();

    stop_commands.push(Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    }));

    // Start every configured scanner in it's own thread and have them send commands
    // to the command channel.
    let scanner_threads: Vec<(String, thread::JoinHandle<Result<(), Error>>)> = settings
        .sftp_sources
        .clone()
        .into_iter()
        .map(|sftp_source| {
            let name = sftp_source.name.clone();

            let join_handle = sftp_scanner::start_scanner(
                stop.clone(),
                cmd_sender.clone(),
                settings.postgresql.url.clone(),
                sftp_source,
            );

            (name, join_handle)
        })
        .collect();

    // Start the built in web server that currently only serves metrics.
    let (web_server_join_handle, actix_system, actix_http_server) =
        http_server::start_http_server(settings.http_server.address);

    stop_commands.push(Box::new(move || {
        tokio::spawn(actix_http_server.stop(true));
    }));

    stop_commands.push(Box::new(move || {
        actix_system.stop();
    }));

    let amqp_sender_join_handle =
        amqp_sender::start_sender(stop, cmd_receiver, settings.command_queue.address);

    let signal_handler_join_handle = runtime.spawn(setup_signal_handler(stop_commands));

    runtime
        .block_on(signal_handler_join_handle)
        .expect("Shutdown cannot error");

    for (source_name, scanner_thread) in scanner_threads {
        info!("Waiting for scanner thread '{}' to stop", &source_name);

        wait_for(scanner_thread, "Scanner");
    }

    wait_for(web_server_join_handle, "Http server");
    wait_for(amqp_sender_join_handle, "AMQP sender");
}

fn setup_signal_handler(
    stop_commands: Vec<Box<dyn FnOnce() -> () + Send + 'static>>,
) -> impl futures::future::Future<Output = ()> + Send + 'static {
    let signals = Signals::new(&[
        signal_hook::SIGHUP,
        signal_hook::SIGTERM,
        signal_hook::SIGINT,
        signal_hook::SIGQUIT,
    ])
    .unwrap();

    let mut signal_stream = Compat01As03::new(signals.into_async().unwrap());

    async move {
        while let Ok(signal) = signal_stream.try_next().await {
            if let Some(s) = signal {
                info!("signal: {}", s);
            }
        }

        for stop_command in stop_commands {
            stop_command();
        }
    }
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
