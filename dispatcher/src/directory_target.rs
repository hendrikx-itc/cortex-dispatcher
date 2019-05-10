use std::path::PathBuf;

use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel};
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

        runtime.spawn(receiver.map_err(|_| ()).for_each(move |file_event: FileEvent| {
            info!("FileEvent for target {}: {}", &target_name, file_event.path.to_str().unwrap());

            futures::future::ok(())
        }));

        DirectoryTarget {
            name: settings.name.clone(),
            path: settings.directory.clone(),
            sender: sender
        }
    }
}
