use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::net::TcpStream;
use std::thread;
use std::io;
use std::fs::File;

use ssh2::{Session, Sftp};

mod settings;

pub use crate::settings::Settings;

extern crate inotify;

extern crate actix;
use actix::prelude::*;
use actix::{Actor, Addr, Context};

use inotify::{
    EventMask,
    WatchMask,
    Inotify
};

use owning_ref::OwningHandle;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

fn file_system_watcher(directory_sources: Vec<settings::DirectorySource>) -> std::thread::JoinHandle<()> {
    let builder = thread::Builder::new();
    let handler = builder.name("sftp-scanner".to_string()).spawn(move || {
        let mut inotify = Inotify::init()
            .expect("Failed to initialize inotify");

        let mut watch_mapping: HashMap<inotify::WatchDescriptor, settings::DirectorySource> = HashMap::new();

        for directory_source in directory_sources {
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

        let mut buffer = [0u8; 4096];

        loop {
            let events = inotify
                .read_events_blocking(&mut buffer)
                .expect("Failed to read inotify events");

            for event in events {
                if event.mask.contains(EventMask::CLOSE_WRITE) | event.mask.contains(EventMask::MOVED_TO) {
                    let name = event.name.expect("Could not decode name");

                    info!("File detected: {:?}", name);

                    let data_source = watch_mapping.get(&event.wd).unwrap();

                    for data_target in &data_source.targets {
                        if data_target.regex.is_match(name.to_str().unwrap()) {
                            let source_path = Path::new(&data_source.directory).join(name);
                            let target_path = Path::new(&data_target.directory).join(name);

                            let rename_result = fs::rename(&source_path, &target_path);

                            match rename_result {
                                Err(e) => error!("E003 Error moving {:?} -> {:?}: {:?}", source_path, target_path, e),
                                Ok(_o) => info!("Moved {:?} -> {:?}", source_path, target_path)
                            }
                        }
                    }
                }
            }
        }
    }).unwrap();

    handler
}

struct SftpConnection {
    tcp: TcpStream,
    sftp: OwningHandle<Box<Session>, Box<Sftp<'static>>>
}

impl SftpConnection {
    fn new(sftp_source: &settings::SftpSource) -> SftpConnection {
        let tcp = TcpStream::connect(&sftp_source.address).unwrap();

        let mut session = Box::new(Session::new().unwrap());
        session.handshake(&tcp).unwrap();
        session.userauth_agent(&sftp_source.username).expect("authentication failed");

        // OwningHandle is needed to store a value and a reference to that value in the same struct
        let sftp = OwningHandle::new_with_fn(
            session,
            unsafe { |s| Box::new((*s).sftp().unwrap()) }
        );

        return SftpConnection{tcp: tcp, sftp: sftp};
    }
}

struct SftpDownloader {
    config: settings::SftpDownloader,
    sftp_connection: SftpConnection,
}

impl SftpDownloader {
    fn new(sftp_source: settings::SftpSource, config: settings::SftpDownloader) -> SftpDownloader {
        let conn = SftpConnection::new(&sftp_source);

        return SftpDownloader {
            config: config,
            sftp_connection: conn
        };
    }
}

struct Download {
    path: String
}

impl Message for Download {
    type Result = bool;
}

impl Handler<Download> for SftpDownloader {
    type Result = bool;

    fn handle(&mut self, msg: Download, _ctx: &mut SyncContext<Self>) -> Self::Result {
        info!("{} downloading '{}'", self.config.name, msg.path);

        let path = Path::new(&msg.path);

        let mut remote_file = self.sftp_connection.sftp.open(&path).unwrap();

        let mut local_file = File::create("/tmp/test.txt").unwrap();

        let copy_result = io::copy(&mut remote_file, &mut local_file);

        info!("{} downloaded '{}'", self.config.name, msg.path);


        match copy_result {
            Ok(_) => return true,
            Err(_e) => return false
        }
    }
}

impl Actor for SftpDownloader {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("SftpDownloader actor started");
    }
}

struct SftpScanner {
    sftp_scanner: settings::SftpScanner,
    sftp_connection: SftpConnection,
    downloader: Addr<SftpDownloader>
}

struct Scan;

impl Message for Scan {
    type Result = i32;
}

impl SftpScanner {
    fn new(sftp_source: settings::SftpSource, sftp_scanner: settings::SftpScanner, downloader: Addr<SftpDownloader>) -> SftpScanner {
        let conn = SftpConnection::new(&sftp_source);

        return SftpScanner {
            sftp_scanner: sftp_scanner,
            sftp_connection: conn,
            downloader: downloader
        };
    }
}

impl Handler<Scan> for SftpScanner {
    type Result = i32;

    fn handle(&mut self, _msg: Scan, _ctx: &mut Context<Self>) -> Self::Result {
        info!("{} scanning remote directory '{}'", self.sftp_scanner.name, &self.sftp_scanner.directory);

        let result = self.sftp_connection.sftp.readdir(Path::new(&self.sftp_scanner.directory));

        let paths = result.unwrap();

        for (path, _stat) in paths {
            let file_name = path.file_name().unwrap().to_str().unwrap();

            let path_str = path.to_str().unwrap().to_string();

            if self.sftp_scanner.regex.is_match(file_name) {
                self.downloader.do_send(Download{path: path_str.clone()});
                info!(" - {} - matches!", path_str);
            } else {
                info!(" - {} - no match", path_str);
            }
        }

        //ctx.notify(Scan);

        return 16;
    }
}

impl Actor for SftpScanner {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("SftpScanner actor started");
    }
}

pub fn run(settings: settings::Settings) -> () {
    let system = actix::System::new("cortex");

    let sftp_sources_hash: HashMap<String, &settings::SftpSource> = settings.sftp_sources.iter().map(|sftp_source| {
        (sftp_source.name.clone(), sftp_source)
    }).collect();

    let downloaders: Vec<Addr<SftpDownloader>> = settings.sftp_downloaders.iter().map(|downloader| {
        let sftp_source = sftp_sources_hash.get(&downloader.sftp_source).unwrap();
        let owned_sftp_source: settings::SftpSource = sftp_source.clone().clone();

        let downloader_settings = downloader.clone();

        let addr = SyncArbiter::start(
            2,
            move || SftpDownloader::new(owned_sftp_source.clone(), downloader_settings.clone())
        );

        addr
    }).collect();

    // For now let the default downloader be the first.
    // Need to implement looking up the right one.
    let default_downloader = downloaders[0].clone();

    let _scanners: Vec<Addr<SftpScanner>> = settings.sftp_scanners.iter().map(|scanner| {
        let sftp_source = sftp_sources_hash.get(&scanner.sftp_source).unwrap();
        let owned_sftp_source: settings::SftpSource = sftp_source.clone().clone();

        let scanner_addr = SftpScanner::new(
            owned_sftp_source, scanner.clone(), default_downloader.clone()
        ).start();

        let _scan_future = scanner_addr.do_send(Scan);

        scanner_addr
    }).collect();


    let join_handle = file_system_watcher(settings.directory_sources);

    system.run();

    join_handle.join().expect("failed to join file system watcher thread");
}
