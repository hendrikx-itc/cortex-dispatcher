use std::sync::{Arc, Mutex};

use futures::future::Either;
use futures::FutureExt;
use tera::{Context, Tera};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::event::FileEvent;
use crate::settings;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel};

pub trait Notify {
    fn and_then_notify<T>(
        &self,
        stream: impl futures::StreamExt<Item = Result<FileEvent, ()>> + 'static + Send,
    ) -> Box<dyn futures::Stream<Item = Result<FileEvent, ()>> + 'static + Send>
    where
        T: futures::Stream<Item = Result<FileEvent, ()>> + 'static + Send;
}

pub struct RabbitMQNotify {
    pub message_template: String,
    pub channel: Channel,
    pub exchange: String,
    pub routing_key: String,
}

impl Notify for RabbitMQNotify {
    fn and_then_notify<T>(
        &self,
        stream: impl futures::StreamExt<Item = Result<FileEvent, ()>> + 'static + Send,
    ) -> Box<dyn futures::Stream<Item = Result<FileEvent, ()>> + 'static + Send>
    where
        T: futures::Stream<Item = Result<FileEvent, ()>> + 'static + Send,
    {
        let template_name = "notification";
        let channel = self.channel.clone();
        let exchange = self.exchange.clone();
        let routing_key = self.routing_key.clone();

        let mut tera = Tera::default();

        if let Err(e) = tera.add_raw_template(template_name, &self.message_template) {
            error!("Error adding template: {}", e);
        }

        Box::new(stream.then(move |item| {
            if let Ok(file_event) = item {
                let mut context = Context::new();
                context.insert("file_path", &file_event.path);

                let render_result = tera.render(template_name, &context);

                match render_result {
                    Ok(message_str) => {
                        let message_str_log = message_str.clone();
                        Either::Left(
                            channel
                                .basic_publish(
                                    &exchange,
                                    &routing_key,
                                    BasicPublishOptions::default(),
                                    message_str.as_bytes().to_vec(),
                                    BasicProperties::default(),
                                )
                                .then(|_r| { futures::future::ok(file_event) })
                        )
                    },
                    Err(e) => {
                        error!("Error rendering template: {}", e);
                        Either::Right(
                            futures::future::ok(file_event)
                        )
                    }
                }
            } else {
                Either::Right(
                    futures::future::err(())
                )
            }
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
