use std::collections::HashMap;
use std::path::Path;
use std::fs;

mod settings;

pub use settings::Settings;

extern crate inotify;

use inotify::{
    EventMask,
    WatchMask,
    Inotify
};

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

pub fn run(settings: settings::Settings) -> () {
    let mut inotify = Inotify::init()
        .expect("Failed to initialize inotify");

    let mut watch_mapping: HashMap<inotify::WatchDescriptor, settings::DirectorySource> = HashMap::new();

    for directory_source in settings.directory_sources {
        info!("Directory source: {}", directory_source.name);
        let source_directory_str = directory_source.directory.clone();
        let source_directory = Path::new(&source_directory_str);

        let watch = inotify
            .add_watch(
                source_directory,
                WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO
            )
            .expect("Failed to add inotify watch");

        watch_mapping.insert(watch, directory_source);
    }

    for sftp_source in settings.sftp_sources {
        info!("SFTP source: {}", sftp_source.name);
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
