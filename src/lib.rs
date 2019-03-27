use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::net::{TcpStream};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;

use ssh2::{Session, Sftp};

mod settings;

pub use settings::Settings;

extern crate inotify;

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

        session.userauth_agent(&self.username);

        session
    }
}


fn sftp_scanner(sftp_source: settings::SftpSource, sftp_scanner: settings::SftpScanner, tx: Sender<Box<PathBuf>>) -> std::thread::JoinHandle<()> {
    let sftp_scanner = thread::Builder::new().name("sftp-scanner".to_string()).spawn(move || {
        // Setup connection once for the polling thread
        let tcp = TcpStream::connect(&sftp_source.address).unwrap();
        let session = sftp_source.ssh_session(&tcp);

        let result = session.sftp();

        let sftp: Sftp = match result {
            Err(e) => {
                error!("E001 {}", e);
                return;
            },
            Ok(sftp) => sftp
        };

        loop {
            info!("SFTP scanner: {}", sftp_scanner.name);
            info!("Scanning remote directory '{}'", &sftp_scanner.directory);

            let result = sftp.readdir(Path::new(&sftp_scanner.directory));

            let paths = result.unwrap();

            for (path, _stat) in paths {
                let file_name = path.file_name().unwrap().to_str().unwrap();

                let path_str = path.to_str().unwrap().to_string();

                if sftp_scanner.regex.is_match(file_name) {
                    info!(" - {} - matches!", path_str);
                    let send_result = tx.send(Box::new(path));

                    match send_result {
                        Err(e) => error!("E002 {}", e),
                        Ok(_) => info!("message sent: {}", path_str)
                    }
                } else {
                    info!(" - {} - no match", path_str);
                }
            }

            thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    sftp_scanner.unwrap()
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

pub fn run(settings: settings::Settings) -> () {
    let settings_copy = settings.clone();

    let sftp_sources_hash: HashMap<String, &settings::SftpSource> = settings.sftp_sources.iter().map(|sftp_source| {
        (sftp_source.name.clone(), sftp_source)
    }).collect();

    let rxs: Vec<Receiver<Box<PathBuf>>> = settings_copy.sftp_scanners.iter().map(|scanner| {
        let (tx, rx): (Sender<Box<PathBuf>>, Receiver<Box<PathBuf>>) = mpsc::channel();

        let sftp_source = sftp_sources_hash.get(&scanner.sftp_source).unwrap().clone();

        let owned_sftp_source: settings::SftpSource = sftp_source.clone();

        sftp_scanner(owned_sftp_source, scanner.clone(), tx.clone());

        rx
    }).collect();

    let join_handle = file_system_watcher(settings_copy.directory_sources);

    join_handle.join();
}
