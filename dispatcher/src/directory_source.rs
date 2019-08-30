#![cfg(target_os = "linux")]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::thread;
use std::io;
use std::fs;

extern crate inotify;

use inotify::{Inotify, WatchMask};

use futures::future::Future;
use futures::stream::Stream;

extern crate failure;
extern crate lapin;

use cortex_core::{StopCmd};

use crate::base_types::Source;
use crate::event::FileEvent;
use crate::settings;

use tokio::runtime::current_thread::Runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;


fn visit_dirs(dir: &Path, cb: &mut dyn FnMut(&Path), recurse: bool) -> io::Result<()> {
    if dir.is_dir() {
        cb(dir);

        if recurse {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    cb(&path);
                    visit_dirs(&path, cb, recurse)?;
                }
            }
        }
    }

    Ok(())
}


pub fn start_directory_sources(
    directory_sources: Vec<settings::DirectorySource>,
) -> (thread::JoinHandle<()>, StopCmd, Vec<Source>) {
    let init_result = Inotify::init();

    let mut inotify = match init_result {
        Ok(i) => i,
        Err(e) => panic!("Could not initialize inotify: {}", e),
    };

    info!("Inotify initialized");

    let mut watch_mapping: HashMap<
        inotify::WatchDescriptor,
        (String, PathBuf, UnboundedSender<FileEvent>),
    > = HashMap::new();
    let mut result_sources: Vec<(String, UnboundedReceiver<FileEvent>)> = Vec::new();

    directory_sources.iter().for_each(|directory_source| {
        info!("Directory source: {}", directory_source.name);
        let (sender, receiver) = unbounded_channel();

        result_sources.push((directory_source.name.clone(), receiver));

        let mut register_watch = |path: &Path| {
            let watch_result = inotify.add_watch(
                path,
                WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO,
            );

            match watch_result {
                Ok(w) => {
                    info!(
                        "Added watch on {}",
                        path.to_str().unwrap()
                    );
                    watch_mapping.insert(w, (directory_source.name.clone(), PathBuf::from(path), sender.clone()));
                }
                Err(e) => {
                    error!(
                        "[E02003] Failed to add inotify watch on '{}': {}",
                        &directory_source.directory.to_str().unwrap(),
                        e
                    );
                }
            };
        };

        let visit_result = visit_dirs(Path::new(&directory_source.directory), &mut register_watch, directory_source.recursive);

        match visit_result {
            Ok(_) => (),
            Err(e) => error!("[E02011] Error recursing directories: {}", e)
        };
    });

    let (join_handle, stop_cmd) = start_inotify_event_thread(inotify, watch_mapping);

    (
        join_handle,
        stop_cmd,
        result_sources
            .into_iter()
            .map(move |(name, receiver)| Source {
                name: name,
                receiver: receiver,
            })
            .collect(),
    )
}

fn start_inotify_event_thread(mut inotify: Inotify, mut watch_mapping: HashMap<
        inotify::WatchDescriptor,
        (String, PathBuf, UnboundedSender<FileEvent>)
    >) -> (thread::JoinHandle<()>, StopCmd) {
    let (stop_sender, stop_receiver) = oneshot::channel::<()>();

    let stop_cmd = Box::new(move || {
        stop_sender.send(()).unwrap();
    });

    let join_handle = thread::spawn(move || {
        let runtime_result = Runtime::new();

        let mut runtime = match runtime_result {
            Ok(r) => r,
            Err(e) => {
                error!("[E01002] Error starting Tokio runtime for inotify thread: {}", e);
                return
            }
        };

        debug!("Tokio runtime created");

        let buffer: Vec<u8> = vec![0; 1024];

        let stream = inotify
            .event_stream(buffer).map_err(|e| error!("[E02004] Error in inotify stream: {}", e))
            .for_each(move |event: inotify::Event<std::ffi::OsString>| {
                let name = event.name.expect("Could not decode name");

                let (source_name, directory, sender) = watch_mapping.get_mut(&event.wd).unwrap();

                let file_name = name.to_str().unwrap().to_string();

                let source_path = directory.join(&file_name);

                let file_event = FileEvent {
                    source_name: source_name.clone(),
                    path: source_path.clone(),
                };

                info!("New file for <{}>: '{}'", &source_name, &source_path.to_str().unwrap());

                let send_result = sender.try_send(file_event);

                match send_result {
                    Ok(_) => {
                        debug!("File event from inotify sent on local channel");
                        futures::future::ok(())
                    },
                    Err(e) => {
                        error!("[E02001] Error sending file event on local channel: {}", e);
                        futures::future::err(())
                    }
                }
            })
            .map_err(|e| {
                error!("[E02002] {:?}", e);
            });

        let stoppable_stream = stream
            //.select2(stop_receiver.into_future())
            .map(|_| debug!("End inotify stream"))
            .map_err(|_| error!("[E01001] Error in inotify stream"));

        runtime.spawn(stoppable_stream);

        runtime.run().unwrap();

        debug!("Inotify source stream ended")
    });

    (join_handle, stop_cmd)
}
