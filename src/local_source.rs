use std::collections::HashMap;
use std::io;
use std::path::Path;

extern crate inotify;

extern crate actix;
use actix::prelude::*;
use actix::{Actor, Context};

use inotify::{Inotify, WatchMask};

use futures::stream::Stream;

extern crate failure;
extern crate lapin_futures;

use crate::settings;


struct FileSystemEvent {
    path: String,
}

impl Message for FileSystemEvent {
    type Result = bool;
}

pub struct LocalSource {
    pub sources: Vec<settings::DirectorySource>,
    pub inotify: Inotify,
}

impl StreamHandler<FileSystemEvent, io::Error> for LocalSource {
    fn handle(&mut self, item: FileSystemEvent, _ctx: &mut Context<LocalSource>) {
        info!("file system event: {}", item.path);
    }
}

impl Actor for LocalSource {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let sources = self.sources.clone();
        let watch_mapping: HashMap<inotify::WatchDescriptor, settings::DirectorySource> = sources
            .iter()
            .map(|directory_source| {
                info!("Directory source: {}", directory_source.name);
                let source_directory_str = directory_source.directory.clone();
                let source_directory = Path::new(&source_directory_str);

                let watch = self
                    .inotify
                    .add_watch(
                        source_directory,
                        WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO,
                    )
                    .expect("Failed to add inotify watch");

                info!("Added watch on {}", source_directory_str);

                (watch, directory_source.clone())
            })
            .collect();

        let buffer: Vec<u8> = Vec::with_capacity(4096);

        let event_stream = self.inotify.event_stream(buffer).map(
            move |event: inotify::Event<std::ffi::OsString>| -> FileSystemEvent {
                let name = event.name.expect("Could not decode name");

                info!("File detected: {:?}", name);

                let data_source = watch_mapping.get(&event.wd).unwrap();

                let source_path = Path::new(&data_source.directory).join(name);

                return FileSystemEvent {
                    path: source_path.to_str().unwrap().to_string(),
                };
            },
        );

        LocalSource::add_stream(event_stream, ctx);

        info!("LocalSource actor started");
    }
}
