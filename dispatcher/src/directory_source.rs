#![cfg(target_os = "linux")]
use std::collections::HashMap;
use std::path::Path;
use std::thread;

extern crate inotify;

use inotify::{Inotify, WatchMask};

use futures::future::Future;
use futures::stream::Stream;

extern crate failure;
extern crate lapin_futures;

use crate::base_types::Source;
use crate::event::FileEvent;
use crate::settings;

use tokio::runtime::current_thread::Runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub fn start_directory_sources(
    directory_sources: Vec<settings::DirectorySource>,
) -> (thread::JoinHandle<()>, Vec<Source>) {
    let init_result = Inotify::init();

    let mut inotify = match init_result {
        Ok(i) => i,
        Err(e) => panic!("Could not initialize inotify: {}", e),
    };

    info!("Inotify initialized");

    let mut watch_mapping: HashMap<
        inotify::WatchDescriptor,
        (settings::DirectorySource, UnboundedSender<FileEvent>),
    > = HashMap::new();
    let mut result_sources: Vec<(String, UnboundedReceiver<FileEvent>)> = Vec::new();

    directory_sources.iter().for_each(|directory_source| {
        info!("Directory source: {}", directory_source.name);
        let (sender, receiver) = unbounded_channel();

        result_sources.push((directory_source.name.clone(), receiver));

        let watch_result = inotify.add_watch(
            Path::new(&directory_source.directory),
            WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO,
        );

        match watch_result {
            Ok(w) => {
                info!(
                    "Added watch on {}",
                    &directory_source.directory.to_str().unwrap()
                );
                watch_mapping.insert(w, (directory_source.clone(), sender));
            }
            Err(e) => {
                error!(
                    "Failed to add inotify watch on '{}': {}",
                    &directory_source.directory.to_str().unwrap(),
                    e
                );
            }
        };
    });

    let join_handle = thread::spawn(move || {
        let mut runtime = Runtime::new().unwrap();

        let buffer: Vec<u8> = vec![0; 1024];

        let stream = inotify
            .event_stream(buffer)
            .for_each(move |event: inotify::Event<std::ffi::OsString>| {
                let name = event.name.expect("Could not decode name");

                let (directory_source, sender) = watch_mapping.get_mut(&event.wd).unwrap();

                let file_name = name.to_str().unwrap().to_string();

                let source_path = directory_source.directory.join(&file_name);

                let file_event = FileEvent {
                    source_name: directory_source.name.clone(),
                    path: source_path.clone(),
                };

                debug!("Sending FileEvent: {:?}", &file_event);

                sender.try_send(file_event).unwrap();

                Ok(())
            })
            .map_err(|e| {
                error!("{}", e);
            });

        runtime.spawn(stream);

        runtime.run().unwrap();
    });

    (
        join_handle,
        result_sources
            .into_iter()
            .map(move |(name, receiver)| Source {
                name: name,
                receiver: receiver,
            })
            .collect(),
    )
}
