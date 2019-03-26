use clap::{App, Arg, crate_version, crate_description, crate_authors};

pub fn app() -> App<'static, 'static> {
    App::new("Cortex")
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("config")
                .short("c")
                .value_name("CONFIG_FILE")
                .help("Specify config file")
                .takes_value(true),
        )
}
