use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::path::PathBuf;

extern crate inotify;

use inotify::{Inotify, WatchMask};

use futures::future::Future;
use futures::stream::Stream;

extern crate failure;
extern crate lapin_futures;

use crate::settings;

use tokio::runtime::current_thread::Runtime;
use crossbeam_channel::{Sender, Receiver, unbounded};

#[derive(Debug)]
pub struct FileEvent {
    source_name: String,
    path: PathBuf
}

#[derive(Clone)]
struct LocalSource {
    directory_source: settings::DirectorySource,
    sender: Sender<FileEvent>,
    receiver: Receiver<FileEvent>
}

pub fn start_local_source_handler(directory_sources: Vec<settings::DirectorySource>) -> (thread::JoinHandle<()>, Vec<(String, Receiver<FileEvent>)>) {
    let source_channels: Vec<LocalSource> = directory_sources.iter().map(|directory_source| {
        let (sender, receiver) = unbounded();

        LocalSource {
            directory_source: directory_source.clone(),
            sender: sender,
            receiver: receiver
        }
    }).collect();

    let local_sources: Vec<LocalSource> = source_channels.iter().map(|sc| sc.clone()).collect();

    let join_handle = thread::spawn(move || {
        let init_result = Inotify::init();

        let mut inotify = match init_result {
            Ok(i) => i,
            Err(e) => panic!("Could not initialize inotify: {}", e),
        };

        info!("Inotify initialized");

        let mut runtime = Runtime::new().unwrap();

        let watch_mapping: HashMap<inotify::WatchDescriptor, LocalSource> = source_channels 
            .iter()
            .map(|local_source| -> Option<(inotify::WatchDescriptor, LocalSource)> {
                info!("Directory source: {}", local_source.directory_source.name);
                let source_directory_str = local_source.directory_source.directory.clone();
                let source_directory = Path::new(&source_directory_str);

                let watch_result = inotify
                    .add_watch(
                        source_directory,
                        WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO,
                    );

                match watch_result {
                    Ok(w) => {
                        info!("Added watch on {}", source_directory_str);
                        let l: LocalSource = local_source.to_owned();
                        Some((w, l))
                    },
                    Err(e) => {
                        error!("Failed to add inotify watch on '{}': {}", source_directory_str, e);
                        None
                    }
                }
            })
            .filter_map(|w| w)
            .collect();

        let buffer: Vec<u8> = vec![0; 1024];

        let stream = inotify.event_stream(buffer).for_each(
            move |event: inotify::Event<std::ffi::OsString>| {
                let name = event.name.expect("Could not decode name");

                let local_source = &watch_mapping[&event.wd];

                let file_name = name.to_str().unwrap().to_string();

                let source_path = Path::new(&local_source.directory_source.directory).join(&file_name);

                let file_event = FileEvent {
                    source_name: local_source.directory_source.name.clone(),
                    path: source_path.clone()
                };

                debug!("Sending FileEvent: {:?}", &file_event);

                local_source.sender.send(file_event).unwrap();

                Ok(())
            }
        ).map_err(|e| {
            error!("{}", e);
        });

        runtime.spawn(stream);

        runtime.run().unwrap();
    });

    let source_receivers = local_sources.iter().map(|local_source| {
        (local_source.directory_source.name.clone(), local_source.receiver.clone())
    }).collect();

    (join_handle, source_receivers)
}

