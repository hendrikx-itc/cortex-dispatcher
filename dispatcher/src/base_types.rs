use std::sync::{Arc, Mutex};

use tera::{Context, Tera};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::event::FileEvent;
use crate::settings;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel, CloseOnDrop};


pub struct RabbitMQNotify {
    pub message_template: String,
    pub channel: CloseOnDrop<Channel>,
    pub exchange: String,
    pub routing_key: String,
}

impl RabbitMQNotify {
    pub async fn notify(&self, file_event: FileEvent) {
        let template_name = "notification";
        let channel = self.channel.clone();
        let exchange = self.exchange.clone();
        let routing_key = self.routing_key.clone();

        let mut tera = Tera::default();

        if let Err(e) = tera.add_raw_template(template_name, &self.message_template) {
            error!("Error adding template: {}", e);
        }

        let mut context = Context::new();
        context.insert("file_path", &file_event.path);

        let render_result = tera.render(template_name, &context);

        match render_result {
            Ok(message_str) => {
                channel.basic_publish(&exchange, &routing_key, BasicPublishOptions::default(), message_str.as_bytes().to_vec(), BasicProperties::default())
                .wait()
                .expect("basic_publish");
            },
            Err(e) => {
                error!("Error rendering template: {}", e);
            }
        }
    }
}

#[derive(Debug)]
pub struct Source {
    pub name: String,
    pub receiver: UnboundedReceiver<FileEvent>,
}

#[derive(Debug)]
pub struct Target {
    pub name: String,
    pub sender: UnboundedSender<FileEvent>,
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub source_name: String,
    pub target: Arc<Target>,
    pub filter: Option<settings::Filter>,
}

#[derive(Debug, Clone)]
pub struct CortexConfig {
    pub sftp_sources: std::sync::Arc<Mutex<Vec<settings::SftpSource>>>,
    pub directory_targets: std::sync::Arc<Mutex<Vec<settings::DirectoryTarget>>>,
    pub connections: std::sync::Arc<Mutex<Vec<settings::Connection>>>,
}

#[derive(Debug, Clone)]
pub enum MessageResponse {
    Ack { delivery_tag: u64 },
    Nack { delivery_tag: u64 }
}
