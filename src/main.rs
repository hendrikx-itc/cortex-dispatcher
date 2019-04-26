extern crate clap;
extern crate config;

#[macro_use]
extern crate log;
extern crate env_logger;

mod settings;
mod cmd;
mod cortex;
mod amqp_consumer;
mod command_handler;
mod sftp_downloader;
mod sftp_connection;
mod local_source;
mod metrics;

use settings::Settings;
use cortex::Cortex;

#[macro_use]
extern crate serde_derive;

extern crate chrono;
extern crate tee;
extern crate sha2;

#[macro_use]
extern crate prometheus;

#[macro_use]
extern crate lazy_static;

fn main() {
    env_logger::builder()
        .default_format_timestamp(false)
        .init();

    let matches = cmd::app().get_matches();

    let config_file = matches
        .value_of("config")
        .unwrap_or("/etc/cortex/cortex.yaml");

    let mut settings = config::Config::new();

    info!("Loading configuration");

    let merge_result = settings
        .merge(config::File::new(config_file, config::FileFormat::Yaml));

    match merge_result {
        Ok(_config) => {
            info!("Configuration loaded from file {}", config_file);
        },
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

    Cortex::new(settings).run();
}
