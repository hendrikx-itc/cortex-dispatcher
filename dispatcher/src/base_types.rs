use tokio::sync::mpsc::{UnboundedReceiver};

use crate::event::FileEvent;

pub struct Source {
    pub name: String,
    pub receiver: UnboundedReceiver<FileEvent>,
}
