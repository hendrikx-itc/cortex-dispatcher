use std::collections::HashMap;
use std::path::PathBuf;

use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub struct FileEvent {
    pub file_id: i64,
    pub source_name: String,
    pub path: PathBuf,
    pub hash: String,
}

pub struct EventDispatcher {
    pub senders: HashMap<String, UnboundedSender<FileEvent>>,
}

impl EventDispatcher {
    /// Send the file_event to the channel for the corresponding source
    pub fn dispatch_event(&mut self, file_event: &FileEvent) -> Result<(), String> {
        let sender = self.senders.get_mut(&file_event.source_name).unwrap();

        let send_result = sender.send(file_event.clone());

        match send_result {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("{}", e)),
        }
    }
}
