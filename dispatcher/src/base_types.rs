use std::sync::{Arc, Mutex};

use futures::future::{Future, Either};
use futures::stream::Stream;
use tera::{Context, Tera};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::event::FileEvent;
use crate::settings;
use lapin_futures::options::BasicPublishOptions;
use lapin_futures::{BasicProperties, Channel};

pub trait Notify {
    fn and_then_notify<T>(
        &self,
        stream: T,
    ) -> Box<dyn futures::Stream<Item = FileEvent, Error = ()> + Send>
    where
        T: futures::Stream<Item = FileEvent, Error = ()> + 'static + Send;
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
        stream: T,
    ) -> Box<dyn futures::Stream<Item = FileEvent, Error = ()> + Send>
    where
        T: futures::Stream<Item = FileEvent, Error = ()> + 'static + Send,
    {
        let template_name = "notification";
        let channel = self.channel.clone();
        let exchange = self.exchange.clone();
        let routing_key = self.routing_key.clone();

        let mut tera = Tera::default();

        if let Err(e) = tera.add_raw_template(template_name, &self.message_template) {
            error!("Error adding template: {}", e);
        }

        Box::new(stream.map_err(|e| error!("Error in file event stream: {:?}", e)).and_then(move |file_event| {
            let mut context = Context::new();
            context.insert("file_path", &file_event.path);

            let render_result = tera.render(template_name, &context);

            match render_result {
                Ok(message_str) => {
                    let message_str_log = message_str.clone();
                    Either::A(
                        channel
                            .basic_publish(
                                &exchange,
                                &routing_key,
                                message_str.as_bytes().to_vec(),
                                BasicPublishOptions::default(),
                                BasicProperties::default(),
                            )
                            .and_then(move |_| {
                                debug!("Notification sent to AMQP queue: {}", message_str_log);

                                futures::future::ok(file_event)
                            })
                            .map_err(|e| {
                                error!("Error sending notification: {:?}", e);
                            })
                    )
                },
                Err(e) => {
                    error!("Error rendering template: {}", e);
                    Either::B(
                        futures::future::ok(file_event)
                    )
                }
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
    pub filter: settings::Filter,
}

#[derive(Debug, Clone)]
pub struct CortexConfig {
    pub sftp_sources: std::sync::Arc<Mutex<Vec<settings::SftpSource>>>,
    pub directory_targets: std::sync::Arc<Mutex<Vec<settings::DirectoryTarget>>>,
    pub connections: std::sync::Arc<Mutex<Vec<settings::Connection>>>,
}
