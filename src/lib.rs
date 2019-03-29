use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::net::TcpStream;
use std::thread;

use ssh2::Session;

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

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;


impl settings::SftpSource {
    /// Return new session, authorized using the username
    fn ssh_session(&self, tcp: &TcpStream) -> Session {

        let mut session = Session::new().unwrap();
        session.handshake(&tcp).unwrap();

        session.userauth_agent(&self.username).expect("authentication failed");

        session
    }
}

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

struct SftpScanner {
    sftp_source: settings::SftpSource,
    sftp_scanner: settings::SftpScanner
}

struct Scan;

impl Message for Scan {
    type Result = i32;
}

impl Handler<Scan> for SftpScanner {
    type Result = i32;

    fn handle(&mut self, _msg: Scan, ctx: &mut Context<Self>) -> Self::Result {
        info!("SftpScanner actor received scan message");

        // Setup connection once for the polling thread
        let tcp = TcpStream::connect(&self.sftp_source.address).unwrap();
        let session = self.sftp_source.ssh_session(&tcp);

        let result = session.sftp();

        let sftp = result.unwrap();

        info!("SFTP scanner: {}", self.sftp_scanner.name);
        info!("Scanning remote directory '{}'", &self.sftp_scanner.directory);

        let result = sftp.readdir(Path::new(&self.sftp_scanner.directory));

        let paths = result.unwrap();

        for (path, _stat) in paths {
            let file_name = path.file_name().unwrap().to_str().unwrap();

            let path_str = path.to_str().unwrap().to_string();

            if self.sftp_scanner.regex.is_match(file_name) {
                info!(" - {} - matches!", path_str);
            } else {
                info!(" - {} - no match", path_str);
            }
        }

        ctx.address().do_send(Scan);

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

    let _scanners: Vec<Addr<SftpScanner>> = settings.sftp_scanners.iter().map(|scanner| {
        let sftp_source = sftp_sources_hash.get(&scanner.sftp_source).unwrap().clone();
        let owned_sftp_source: settings::SftpSource = sftp_source.clone();

        let scanner_addr = SftpScanner{sftp_source: owned_sftp_source, sftp_scanner: scanner.clone()}.start();
        let _scan_future = scanner_addr.do_send(Scan);

        scanner_addr
    }).collect();

    let join_handle = file_system_watcher(settings.directory_sources);

    system.run();

    join_handle.join().expect("failed to join file system watcher thread");
}
