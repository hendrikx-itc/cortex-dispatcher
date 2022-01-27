use clap::{crate_authors, crate_description, crate_name, crate_version, App, Arg};

pub fn app() -> App<'static> {
    App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("CONFIG_FILE")
                .help("Specify config file")
                .takes_value(true),
        )
        .arg(
            Arg::new("sample_config")
                .long("sample-config")
                .help("Show example configuration file"),
        )
        .arg(
            Arg::new("service")
                .long("service")
                .help("Run in service mode"),
        )
}
