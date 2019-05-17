use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::event::FileEvent;
use crate::settings;

#[derive(Debug)]
pub struct Notify {
    pub message_template: String
}

impl Notify {
    pub fn send(message: String) {
        info!("Notification: {}", message);
    }
}

pub struct Source {
    pub name: String,
    pub receiver: UnboundedReceiver<FileEvent>,
}

#[derive(Debug)]
pub struct Target {
    pub name: String,
    pub sender: UnboundedSender<FileEvent>,
    pub notify: Option<Notify>
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub source_name: String,
    pub target: Arc<Target>,
    pub filter: settings::Filter
}