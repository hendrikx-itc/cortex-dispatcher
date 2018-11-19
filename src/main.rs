extern crate inotify;

use std::env;
use std::fs;
use std::convert::AsRef;
use std::path::Path;

use inotify::{
    EventMask,
    WatchMask,
    Inotify
};

fn main() {
    let mut inotify = Inotify::init()
        .expect("Failed to initialize inotify");

    let current_dir = env::current_dir()
        .expect("Failed to determine current directory");

    let curr_path: &Path = current_dir.as_path();

    inotify
        .add_watch(
            curr_path,
            WatchMask::CLOSE_WRITE
        )
        .expect("Failed to add inotify watch");

    let mut buffer = [0u8; 4096];

    let target_dir = curr_path.join("target_dir");

    loop {
        let events = inotify
            .read_events_blocking(&mut buffer)
            .expect("Failed to read inotify events");

        for event in events {
            if event.mask.contains(EventMask::CLOSE_WRITE) {
                println!("File created: {:?}", event.name.expect("could not decode event name"));

                let name = event.name.expect("Could not decode name");

                let target_path = target_dir.join(name);

                println!("Move to: {:?}", target_path);

                fs::rename(name, target_path);
            }
        }
    }
}
