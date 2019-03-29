use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::net::TcpStream;
use std::thread;

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

struct SftpConnection {
    tcp: TcpStream,
    sftp: OwningHandle<Box<Session>, Box<Sftp<'static>>>
}

struct SftpScanner {
    sftp_scanner: settings::SftpScanner,
    sftp_connection: SftpConnection
}

struct Scan;

impl Message for Scan {
    type Result = i32;
}

impl SftpScanner {
    fn new(sftp_source: settings::SftpSource, sftp_scanner: settings::SftpScanner) -> SftpScanner {
        let tcp = TcpStream::connect(&sftp_source.address).unwrap();

        let session = Box::new(sftp_source.ssh_session(&tcp));

        let sftp = OwningHandle::new_with_fn(
            session,
            unsafe { |s| Box::new((*s).sftp().unwrap()) }
        );

        let conn = SftpConnection{tcp: tcp, sftp: sftp};

        return SftpScanner {
            sftp_scanner: sftp_scanner,
            sftp_connection: conn
        };
    }
}

impl Handler<Scan> for SftpScanner {
    type Result = i32;

    fn handle(&mut self, _msg: Scan, ctx: &mut Context<Self>) -> Self::Result {
        info!("SFTP scanner: {}", self.sftp_scanner.name);
        info!("Scanning remote directory '{}'", &self.sftp_scanner.directory);

        let result = self.sftp_connection.sftp.readdir(Path::new(&self.sftp_scanner.directory));

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

        ctx.notify(Scan);

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

        let scanner_addr = SftpScanner::new(
            owned_sftp_source, scanner.clone()
        ).start();

        let _scan_future = scanner_addr.do_send(Scan);

        scanner_addr
    }).collect();

    let join_handle = file_system_watcher(settings.directory_sources);

    system.run();

    join_handle.join().expect("failed to join file system watcher thread");
}
