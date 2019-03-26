use clap::{App, Arg};

pub fn app() -> App<'static, 'static> {
    let app = App::new("Cortex")
        .version("0.1.0")
        .author("Hendrikx ITC <info@hendrikx-itc.nl>")
        .arg(
            Arg::with_name("config")
                .short("c")
                .value_name("CONFIG_FILE")
                .help("Specify config file")
                .takes_value(true),
        );

    app
}
