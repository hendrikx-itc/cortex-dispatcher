use std::io::Write;

use config;

use env_logger;
use log::{error, info};

mod base_types;
mod directory_source;
mod directory_target;
mod dispatcher;
mod event;
mod local_storage;
mod metrics;
mod persistence;
mod settings;
mod sftp_command_consumer;
mod sftp_downloader;

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,

    /// Show example config
    #[arg(short, long)]
    example_config: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut env_logger_builder = env_logger::builder();

    env_logger_builder.format(|buf, record| writeln!(buf, "{}  {}", record.level(), record.args()));

    env_logger_builder.init();

    if args.example_config {
        println!(
            "{}",
            serde_yaml::to_string(&settings::Settings::default()).unwrap()
        );
        ::std::process::exit(0);
    }

    let config_file = args.config.unwrap_or("/etc/cortex/cortex.yaml".into());

    info!("Loading configuration");

    let merge_result = config::Config::builder()
        .add_source(config::File::new(&config_file, config::FileFormat::Yaml))
        .build();

    let settings = match merge_result {
        Ok(config) => {
            info!("Configuration loaded from file {}", config_file);

            config.try_deserialize().unwrap()
        }
        Err(e) => {
            error!("Error merging configuration: {}", e);
            ::std::process::exit(1);
        }
    };

    info!("Configuration loaded");

    match dispatcher::run(settings).await {
        Ok(_) => (),
        Err(e) => error!("{}", e),
    }
}
