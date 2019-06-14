use clap::{crate_authors, crate_description, crate_version, App, Arg};

pub fn app() -> App<'static, 'static> {
    App::new("Cortex")
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("CONFIG_FILE")
                .help("Specify config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("sample_config")
                .long("sample-config")
                .help("Show example configuration file"),
        )
        .arg(
            Arg::with_name("service")
                .long("service")
                .help("Run in service mode"),
        )
}
