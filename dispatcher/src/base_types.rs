use std::sync::{Arc, Mutex};

use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::future::Future;
use tera::{Tera, Context};

use crate::event::FileEvent;
use crate::settings;
use lapin_futures::channel::{Channel, BasicProperties, BasicPublishOptions};

pub trait Notify {
    fn and_then_notify<T>(&self, stream: T) -> Box<futures::Stream< Item = FileEvent, Error = () > + Send> where T: futures::Stream< Item = FileEvent, Error = () > + 'static + Send;
}

pub struct RabbitMQNotify {
    pub message_template: String,
    pub channel: Channel<TcpStream>,
    pub exchange: String,
    pub routing_key: String
}

impl Notify for RabbitMQNotify {
    fn and_then_notify<T>(&self, stream: T) -> Box<futures::Stream< Item = FileEvent, Error = () > + Send> where T: futures::Stream< Item = FileEvent, Error = () > + 'static + Send {
        let template_name = "notification";
        let channel = self.channel.clone();
        let exchange = self.exchange.clone();
        let routing_key = self.routing_key.clone();

        let mut tera = Tera::default();

        tera.add_raw_template(template_name, &self.message_template).unwrap();
        
        Box::new(stream.and_then(move |file_event| {
            let mut context = Context::new();
            context.insert("file_path", &file_event.path);
            let message_str = tera.render(template_name, &context).unwrap();
            let message_str_log = message_str.clone();
            channel.basic_publish(
                &exchange,
                &routing_key,
                message_str.as_bytes().to_vec(),
                BasicPublishOptions::default(),
                BasicProperties::default(),
            ).and_then(move |_request_result| {
                debug!("Notification sent: {}", message_str_log);
                // No confirmation/ack is expected
                Ok(file_event)
            }).map_err(|e| {
                error!("Error sending notification: {:?}", e);
            })
        }))
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
    pub filter: settings::Filter
}

#[derive(Debug, Clone)]
pub struct CortexConfig {
    pub sftp_sources: std::sync::Arc<Mutex<Vec<settings::SftpSource>>>,
    pub directory_targets: std::sync::Arc<Mutex<Vec<settings::DirectoryTarget>>>,
    pub connections: std::sync::Arc<Mutex<Vec<settings::Connection>>>
}
