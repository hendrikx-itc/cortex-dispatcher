use std::collections::HashMap;
use std::path::Path;
use std::thread;

extern crate inotify;

use inotify::{Inotify, WatchMask};

use futures::future::Future;
use futures::stream::Stream;

extern crate failure;
extern crate lapin_futures;

use crate::settings;

use tokio::runtime::current_thread::Runtime;

struct FileSystemEvent {
    file_name: String,
    source: settings::DirectorySource
}

pub fn start_local_source_handler(directory_sources: Vec<settings::DirectorySource>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let init_result = Inotify::init();

        let inotify = match init_result {
            Ok(i) => i,
            Err(e) => panic!("Could not initialize inotify: {}", e),
        };

        info!("Inotify initialized");

        let mut runtime = Runtime::new().unwrap();

        let local_source = event_stream_handler(directory_sources, inotify);

        runtime.spawn(local_source);

        runtime.run().unwrap();
    })
}

pub fn event_stream_handler(sources: Vec<settings::DirectorySource>, inotify: Inotify) -> impl Future<Item = (), Error = ()> {
    event_stream(sources, inotify)
        .map_err(|e| {
            error!("Error: {}", e);
        })
        .for_each(|filesystem_event| {
            info!("Filesystem event: {}", filesystem_event.file_name);

            let source_path = Path::new(&filesystem_event.source.directory).join(&filesystem_event.file_name);

            for target in filesystem_event.source.targets {
                if target.regex.is_match(&filesystem_event.file_name) {
                    let target_path = std::path::Path::new(&target.directory).join(&filesystem_event.file_name);

                    let move_result = std::fs::rename(&source_path, &target_path);

                    match move_result {
                        Ok(_) => {
                            info!("moved {} -> {}", &source_path.to_str().unwrap(), &target_path.to_str().unwrap());
                        },
                        Err(e) => {
                            error!("error moving {} -> {}: {}", &source_path.to_str().unwrap(), &target_path.to_str().unwrap(), e);
                        }
                    }
                }
            }

            Ok(())
        })
}

fn event_stream(sources: Vec<settings::DirectorySource>, mut inotify: Inotify) -> impl Stream<Item = FileSystemEvent, Error = std::io::Error> {
    let watch_mapping: HashMap<inotify::WatchDescriptor, settings::DirectorySource> = sources
        .iter()
        .map(|directory_source| {
            info!("Directory source: {}", directory_source.name);
            let source_directory_str = directory_source.directory.clone();
            let source_directory = Path::new(&source_directory_str);

            let watch = inotify
                .add_watch(
                    source_directory,
                    WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO,
                )
                .expect("Failed to add inotify watch");

            info!("Added watch on {}", source_directory_str);

            (watch, directory_source.clone())
        })
        .collect();

    let buffer: Vec<u8> = vec![0; 1024];

    inotify.event_stream(buffer).map(
        move |event: inotify::Event<std::ffi::OsString>| -> FileSystemEvent {
            let name = event.name.expect("Could not decode name");

            let data_source = &watch_mapping[&event.wd];

            FileSystemEvent {
                file_name: name.to_str().unwrap().to_string(),
                source: data_source.clone()
            }
        }
    )
}