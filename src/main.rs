extern crate clap;
extern crate config;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate cortex_dispatcher;

use cortex_dispatcher::Settings;

mod cmd;

fn main() {
    env_logger::init();

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

    cortex_dispatcher::run(settings);
}
