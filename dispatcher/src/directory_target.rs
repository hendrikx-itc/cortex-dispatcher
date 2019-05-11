use std::path::PathBuf;
use std::os::unix::fs::symlink;

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::runtime::Runtime;
use futures::stream::Stream;

use crate::settings;
use crate::event::FileEvent;

pub struct DirectoryTarget {
    pub name: String,
    pub path: PathBuf,
    pub sender: UnboundedSender<FileEvent>
}

impl DirectoryTarget {
    pub fn from_settings(settings: &settings::DirectoryTarget, runtime: &mut Runtime) -> Self {
        let (sender, receiver) = unbounded_channel();

        let target_name = settings.name.clone();
        let target_directory = settings.directory.clone();

        runtime.spawn(receiver.map_err(|_| ()).for_each(move |file_event: FileEvent| {
            let source_path_str = file_event.path.to_str().unwrap();
            let file_name = file_event.path.file_name().unwrap();
            let target_path = target_directory.join(file_name);
            let target_path_str = target_path.to_str().unwrap();

            info!("FileEvent for {}: {}", &target_name, &source_path_str);

            let result = symlink(&file_event.path, &target_path);

            match result {
                Ok(()) => {
                    info!("{} symlinked to {}", &source_path_str, &target_path_str);
                },
                Err(e) => {
                    error!("Error symlinking {} to {}: {}", &source_path_str, &target_path_str, &e);
                }
            }

            futures::future::ok(())
        }));

        DirectoryTarget {
            name: settings.name.clone(),
            path: settings.directory.clone(),
            sender: sender
        }
    }
}
