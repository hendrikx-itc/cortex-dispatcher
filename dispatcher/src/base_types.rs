use tokio::sync::mpsc::{UnboundedReceiver};

use crate::event::FileEvent;

pub trait Source {
    fn name(self) -> String;
    fn events(self) -> UnboundedReceiver<FileEvent>;
}
