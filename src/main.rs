//#![cfg(feature = "yaml")]

extern crate config;
extern crate inotify;
extern crate regex;
extern crate serde_regex;

#[macro_use]
extern crate serde_derive;

use std::fs;
use std::path::Path;
use std::collections::HashMap;

use inotify::{
    EventMask,
    WatchMask,
    Inotify
};

use regex::Regex;

#[derive(Debug, Deserialize)]
struct DataTarget {
    #[serde(with = "serde_regex")]
    regex: Regex,
    directory: String
}

#[derive(Debug, Deserialize)]
struct DataSource {
    directory: String,
    targets: Vec<DataTarget>
}

#[derive(Debug, Deserialize)]
struct Settings {
    sources: Vec<DataSource>
}

fn main() {
    let mut settings = config::Config::new();

    settings.merge(
        config::File::new("cortex", config::FileFormat::Yaml)
    ).expect("Could not read config");

    let s: Settings = settings.try_into().unwrap();

    let mut inotify = Inotify::init()
        .expect("Failed to initialize inotify");

    let mut watch_mapping: HashMap<inotify::WatchDescriptor, DataSource> = HashMap::new();

    for data_source in s.sources {
        let source_directory_str = data_source.directory.clone();
        let source_directory = Path::new(&source_directory_str);

        let watch = inotify
            .add_watch(
                source_directory,
                WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO
            )
            .expect("Failed to add inotify watch");

        watch_mapping.insert(watch, data_source);
    }

    let mut buffer = [0u8; 4096];

    loop {
        let events = inotify
            .read_events_blocking(&mut buffer)
            .expect("Failed to read inotify events");

        for event in events {
            if event.mask.contains(EventMask::CLOSE_WRITE) | event.mask.contains(EventMask::MOVED_TO) {
                println!("File detected: {:?}", event.name.expect("could not decode event name"));

                let name = event.name.expect("Could not decode name");

                let data_source = watch_mapping.get(&event.wd).unwrap();

                for data_target in &data_source.targets {
                    if data_target.regex.is_match(name.to_str().unwrap()) {
                        let source_path = Path::new(&data_source.directory).join(name);
                        let target_path = Path::new(&data_target.directory).join(name);

                        let rename_result = fs::rename(&source_path, &target_path);

                        match rename_result {
                            Err(e) => println!("Error moving {:?} -> {:?}: {:?}", source_path, target_path, e),
                            Ok(_o) => println!("Moved {:?} -> {:?}", source_path, target_path)
                        }
                    }
                }
            }
        }
    }
}
