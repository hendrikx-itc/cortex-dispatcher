#![cfg(target_os = "linux")]
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::sync::mpsc::{Sender, Receiver};

extern crate inotify;

use inotify::{Inotify, WatchMask};

extern crate failure;
extern crate lapin;

use cortex_core::StopCmd;

use crate::event::{FileEvent, EventDispatcher};
use crate::local_storage::LocalStorage;
use crate::persistence::Persistence;
use crate::settings;


#[derive(Debug, Clone)]
pub struct LocalFileEvent {
    pub source_name: String,
    pub path: PathBuf,
    pub prefix: PathBuf
}

fn visit_dirs(dir: &Path, cb: &mut dyn FnMut(&Path), recurse: bool) -> io::Result<()> {
    if dir.is_dir() {
        cb(dir);

        if recurse {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    visit_dirs(&path, cb, recurse)?;
                }
            }
        }
    }

    Ok(())
}

fn visit_files(dir: &Path, cb: &mut dyn FnMut(&Path), recurse: bool) -> io::Result<()> {
    if dir.is_dir() {
        if recurse {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    visit_files(&path, cb, recurse)?;
                } else {
                    cb(&path);
                }
            }
        }
    }

    Ok(())
}

fn construct_watch_mask(events: Vec<settings::FileSystemEvent>) -> WatchMask {
    let mut watch_mask: WatchMask = WatchMask::empty();

    for event in events {
        watch_mask = watch_mask | event.watch_mask();
    }

    watch_mask
}

#[derive(Debug, Clone)]
struct InotifyEventContext {
    recursive: bool,
    watch_mask: WatchMask,
    source_name: String,
    directory: PathBuf,
    prefix: PathBuf,
    filter: Option<settings::Filter>,
}

pub fn start_directory_sweep(
    directory_sources: Vec<settings::DirectorySource>,
    local_intake_sender: Sender<LocalFileEvent>,
) -> (thread::JoinHandle<()>, StopCmd)
{
    let timeout = std::time::Duration::from_millis(15_000);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();

    let stop_cmd = Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    });

    let join_handle = thread::spawn(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            directory_sources.iter().for_each(|directory_source| {
                info!("Sweeping directory source: {}", directory_source.name);

                let mut handle_file = |path: &Path| {
                    let file_matches = match &directory_source.filter {
                        Some(f) => f.file_matches(path),
                        None => true,
                    };

                    if file_matches {
                        let local_file_event = LocalFileEvent {
                            source_name: directory_source.name.clone(),
                            path: PathBuf::from(path),
                            prefix: directory_source.directory.clone()
                        };

                        local_intake_sender.send(local_file_event);
                    }
                };

                let visit_result = visit_files(
                    Path::new(&directory_source.directory),
                    &mut handle_file,
                    directory_source.recursive,
                );

                match visit_result {
                    Ok(()) => (),
                    Err(e) => error!(
                        "Error sweeping directory '{}': {}",
                        &directory_source.directory.to_string_lossy(),
                        e
                    ),
                }
            });

            std::thread::sleep(timeout);
        }

        debug!("Directory sweep thread ended")
    });

    (join_handle, stop_cmd)
}

pub fn start_directory_sources(
    directory_sources: Vec<settings::DirectorySource>,
    local_intake_sender: Sender<LocalFileEvent>,
) -> (thread::JoinHandle<()>, StopCmd)
{
    let init_result = Inotify::init();

    let mut inotify = match init_result {
        Ok(i) => i,
        Err(e) => panic!("Could not initialize inotify: {}", e),
    };

    info!("Inotify initialized");

    let mut watch_mapping: HashMap<inotify::WatchDescriptor, InotifyEventContext> = HashMap::new();

    directory_sources.iter().for_each(|directory_source| {
        info!("Directory source: {}", directory_source.name);

        let mut watch_mask = construct_watch_mask(directory_source.events.clone());

        if directory_source.recursive {
            watch_mask = watch_mask | WatchMask::CREATE
        }

        let mut register_watch = |path: &Path| {
            let watch_result = inotify.add_watch(path, watch_mask);

            match watch_result {
                Ok(w) => {
                    info!("Added watch on {}", path.to_string_lossy());
                    watch_mapping.insert(
                        w,
                        InotifyEventContext {
                            recursive: directory_source.recursive,
                            watch_mask: watch_mask,
                            source_name: directory_source.name.clone(),
                            directory: PathBuf::from(path),
                            prefix: directory_source.directory.clone(),
                            filter: directory_source.filter.clone(),
                        },
                    );
                }
                Err(e) => {
                    error!(
                        "[E02003] Failed to add inotify watch on '{}': {}",
                        &directory_source.directory.to_string_lossy(),
                        e
                    );
                }
            };
        };

        let visit_result = visit_dirs(
            Path::new(&directory_source.directory),
            &mut register_watch,
            directory_source.recursive,
        );

        match visit_result {
            Ok(_) => (),
            Err(e) => error!("[E02011] Error recursing directories: {}", e),
        };
    });

    start_inotify_event_thread(inotify, watch_mapping, local_intake_sender)
}

fn start_inotify_event_thread(
    mut inotify: Inotify,
    mut watch_mapping: HashMap<inotify::WatchDescriptor, InotifyEventContext>,
    local_intake_sender: Sender<LocalFileEvent>,
) -> (thread::JoinHandle<()>, StopCmd)
{
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();

    let stop_cmd = Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    });

    let join_handle = thread::spawn(move || {
        let mut buffer: Vec<u8> = vec![0; 1024];

        while !stop_flag.load(Ordering::Relaxed) {
            let events = inotify.read_events_blocking(&mut buffer).unwrap();

            for event in events {
                let name = match event.name {
                    Some(name) => name,
                    None => {
                        // No name in event
                        continue;
                    }
                };

                let file_name = name.to_str().unwrap();

                let get_result = watch_mapping.get(&event.wd);

                match get_result {
                    Some(event_context) => {
                        let source_path = event_context.directory.join(&file_name);
                        let source_path_str = source_path.to_string_lossy();
                
                        if source_path.is_dir() {
                            if event_context.recursive {
                                let wd = inotify.add_watch(&source_path, event_context.watch_mask).unwrap();
        
                                let sub_event_context = InotifyEventContext {
                                    recursive: event_context.recursive,
                                    watch_mask: event_context.watch_mask,
                                    source_name: event_context.source_name.clone(),
                                    directory: source_path.clone(),
                                    prefix: event_context.prefix.clone(),
                                    filter: event_context.filter.clone(),
                                };
        
                                watch_mapping.insert(wd, sub_event_context);
        
                                info!("Registered extra watch on {}", &source_path_str);
                            }
                        } else {
                            let file_matches = match &event_context.filter {
                                Some(f) => f.file_matches(&source_path),
                                None => true,
                            };
                    
                            if file_matches {
                                debug!("Event for {} matches filter", &source_path_str);
                    
                                let file_event = LocalFileEvent {
                                    source_name: event_context.source_name.clone(),
                                    path: source_path,
                                    prefix: event_context.prefix.clone()
                                };
                    
                                let send_result = local_intake_sender.send(file_event);
                    
                                match send_result {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("Could not send file event: {}", e)
                                    }
                                }
                            }
                        }
        
                    },
                    None => {
                        error!("Could not find matching event context for {}", file_name);
                    }
                }
            }
        }

        debug!("Inotify thread ended")
    });

    (join_handle, stop_cmd)
}

pub fn start_local_intake_thread<T>(
    receiver: Receiver<LocalFileEvent>,
    mut event_dispatcher: EventDispatcher,
    local_storage: LocalStorage<T>
) -> (thread::JoinHandle<()>, StopCmd)
where
    T: Persistence,
    T: Send,
    T: Clone,
    T: 'static,
{
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();

    let stop_cmd = Box::new(move || {
        stop_clone.swap(true, Ordering::Relaxed);
    });

    let join_handle = thread::spawn(move || {
        let timeout = Duration::from_millis(500);

        while !stop_flag.load(Ordering::Relaxed) {
            let receive_result = receiver.recv_timeout(timeout);

            match receive_result {
                Ok(file_event) => {
                    let request_result = local_storage.in_storage(&file_event.source_name, &file_event.path, &file_event.prefix);

                    match request_result {
                        Ok(in_storage) => {
                            if !in_storage {
                                debug!("Not in storage yet: {}", &file_event.path.to_string_lossy());
                                let store_result = local_storage.hard_link(&file_event.source_name, &file_event.path, &file_event.prefix);
                
                                let source_path_str = file_event.path.to_string_lossy();
                    
                                match store_result {
                                    Ok(target_path) => {
                                        let source_file_event = FileEvent {
                                            source_name: file_event.source_name.clone(),
                                            path: target_path.clone(),
                                        };
                    
                                        info!(
                                            "New file for <{}>: '{}'",
                                            &file_event.source_name, &source_path_str
                                        );
                    
                                        let send_result = event_dispatcher.dispatch_event(&source_file_event);
                    
                                        match send_result {
                                            Ok(_) => {
                                                debug!("File event from inotify sent on local channel");
                                            },
                                            Err(e) => error!(
                                                "[E02001] Error sending file event on local channel: {}",
                                                e
                                            )
                                        }
                                    }
                                    Err(e) => error!("Error storing file '{}': {}", &source_path_str, &e),
                                }
                            }
                        },
                        Err(e) => {
                            error!("Error querying ")
                        }
                    }
                }
                Err(_) => {
                    // Nothing to do, just allow the stop flag to be checked in the next iteration
                }
            }
        }

        debug!("Local intake thread ended")
    });

    (join_handle, stop_cmd)
}
