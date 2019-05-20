use std::os::unix::fs::symlink;

use tokio::sync::mpsc::{UnboundedReceiver};
use futures::stream::Stream;

use crate::settings;
use crate::event::FileEvent;

pub fn to_stream(settings: &settings::DirectoryTarget, receiver: UnboundedReceiver<FileEvent>) -> impl futures::Stream< Item = FileEvent, Error = () > {
    let target_name = settings.name.clone();
    let target_directory = settings.directory.clone();

    receiver.map_err(|_| ()).map(move |file_event: FileEvent| {
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

        file_event
    })
}