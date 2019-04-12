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

use settings::Settings;
use cortex::Cortex;

#[macro_use]
extern crate serde_derive;

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

    settings
        .merge(config::File::new(config_file, config::FileFormat::Yaml))
        .expect("Could not read config");

    let settings: Settings = settings.try_into().unwrap();

    info!("Configuration loaded");

    Cortex::new(settings).run();
}
