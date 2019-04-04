use std::collections::HashMap;
use std::path::Path;
use std::net::TcpStream;
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

use futures::stream::{Stream};


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

struct Download {
    path: String,
    size: Option<u64>
}

impl Message for Download {
    type Result = bool;
}

impl Handler<Download> for SftpDownloader {
    type Result = bool;

    fn handle(&mut self, msg: Download, _ctx: &mut SyncContext<Self>) -> Self::Result {
        info!("{} downloading '{}' {} bytes", self.config.name, msg.path, msg.size.unwrap());

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

impl Handler<Scan> for SftpScanner {
    type Result = i32;

    fn handle(&mut self, _msg: Scan, _ctx: &mut Context<Self>) -> Self::Result {
        info!("{} scanning remote directory '{}'", self.sftp_scanner.name, &self.sftp_scanner.directory);

        let result = self.sftp_connection.sftp.readdir(Path::new(&self.sftp_scanner.directory));

        let paths = result.unwrap();

        for (path, stat) in paths {
            let file_name = path.file_name().unwrap().to_str().unwrap();

            let path_str = path.to_str().unwrap().to_string();

            if self.sftp_scanner.regex.is_match(file_name) {
                let msg = Download{
                    path: path_str.clone(),
                    size: stat.size
                };

                self.downloader.do_send(msg);
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

struct FileSystemEvent {
    path: String
}

impl Message for FileSystemEvent {
    type Result = bool;
}

struct LocalSource {
    sources: Vec<settings::DirectorySource>,
    inotify: Inotify
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
        let watch_mapping: HashMap<inotify::WatchDescriptor, settings::DirectorySource> = sources.iter().map(|directory_source| {
            info!("Directory source: {}", directory_source.name);
            let source_directory_str = directory_source.directory.clone();
            let source_directory = Path::new(&source_directory_str);

            let watch = self.inotify
                .add_watch(
                    source_directory,
                    WatchMask::CLOSE_WRITE | WatchMask::MOVED_TO
                )
                .expect("Failed to add inotify watch");

            info!("Added watch on {}", source_directory_str);

            (watch, directory_source.clone())
        }).collect();

        let buffer: Vec<u8> = Vec::with_capacity(4096);

        let event_stream = self.inotify
            .event_stream(buffer)
            .filter(|event| {
                info!("Filesystem event");
                event.mask.contains(EventMask::CLOSE_WRITE) | event.mask.contains(EventMask::MOVED_TO)
            })
            .map(move |event: inotify::Event<std::ffi::OsString>| -> FileSystemEvent {
                let name = event.name.expect("Could not decode name");

                info!("File detected: {:?}", name);

                let data_source = watch_mapping.get(&event.wd).unwrap();

                let source_path = Path::new(&data_source.directory).join(name);

                return FileSystemEvent { path: source_path.to_str().unwrap().to_string() };
            });

        LocalSource::add_stream(event_stream, ctx);

        info!("LocalSource actor started");
    }
}


pub fn run(settings: settings::Settings) -> () {
    let system = actix::System::new("cortex");

    let sftp_sources_hash: HashMap<String, &settings::SftpSource> = settings
        .sftp_sources
        .iter()
        .map(|sftp_source| {
            (sftp_source.name.clone(), sftp_source)
        }).collect();

    let downloaders: Vec<Addr<SftpDownloader>> = settings.sftp_downloaders.iter().map(|downloader| {
        let sftp_source = sftp_sources_hash.get(&downloader.sftp_source).unwrap();
        let owned_sftp_source: settings::SftpSource = sftp_source.clone().clone();

        let downloader_settings = downloader.clone();

        let addr = SyncArbiter::start(
            downloader_settings.thread_count,
            move || {
                let conn = SftpConnection::new(&owned_sftp_source.clone());

                return SftpDownloader {
                    config: downloader_settings.clone(),
                    sftp_connection: conn
                };
            }
        );

        addr
    }).collect();

    // For now let the default downloader be the first.
    // Need to implement looking up the right one.
    let default_downloader = downloaders[0].clone();

    let _scanners: Vec<Addr<SftpScanner>> = settings.sftp_scanners.iter().map(|scanner| {
        let sftp_source = sftp_sources_hash.get(&scanner.sftp_source).unwrap();
        let owned_sftp_source: settings::SftpSource = sftp_source.clone().clone();

        let conn = SftpConnection::new(&owned_sftp_source.clone());

        let scanner = SftpScanner {
            sftp_scanner: scanner.clone(),
            sftp_connection: conn,
            downloader: default_downloader.clone()
        };

        let scanner_addr = scanner.start();

        let _scan_future = scanner_addr.do_send(Scan);

        scanner_addr
    }).collect();

    let init_result = Inotify::init();

    let inotify = match init_result {
        Ok(i) => i,
        Err(e) => panic!("Could not initialize inotify: {}", e)
    };

    let local_source = LocalSource {
        sources: settings.directory_sources,
        inotify: inotify
    };

    local_source.start();

    system.run();
}
