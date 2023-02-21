use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tera::{Context, Tera};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use chrono::prelude::{DateTime, Utc};

use deadpool_lapin::{Config, Runtime};
use serde_json::json;

use log::error;

use crate::event::FileEvent;
use crate::settings::{self, RabbitMQNotify};
use deadpool_lapin::lapin::options::BasicPublishOptions;
use deadpool_lapin::lapin::{BasicProperties, Channel};

pub struct RabbitMQNotifier {
    pub address: String,
    pub message_template: String,
    pub exchange: String,
    pub routing_key: String,
    channel: Option<Channel>,
}

impl From<&RabbitMQNotify> for RabbitMQNotifier {
    fn from(value: &RabbitMQNotify) -> Self {
        RabbitMQNotifier {
            address: value.address.clone(),
            message_template: value.message_template.clone(),
            exchange: value.exchange.clone(),
            routing_key: value.routing_key.clone(),
            channel: None,
        }
    }
}

impl RabbitMQNotifier {
    async fn connect(&mut self) -> Result<Channel, String> {
        let mut cfg = Config::default();
        cfg.url = Some(self.address.clone());
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| format!("Error creating pool for AMQP server: {e}"))?;

        let connection = pool
            .get()
            .await
            .map_err(|e| format!("Error connecting to AMQP server: {e}"))?;

        let amqp_channel = connection
            .create_channel()
            .await
            .map_err(|e| format!("Error creating AMQP channel: {e}"))?;

        Ok(amqp_channel)
    }

    async fn publish(&mut self, message: &str) -> Result<(), String> {
        self.channel.as_ref().unwrap()
            .basic_publish(
                &self.exchange.clone(),
                &self.routing_key.clone(),
                BasicPublishOptions::default(),
                message.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .map_err(|e| format!("Error publishing notification: {}", e))?;

        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), String> {
        self.channel = None;

        while self.channel.is_none() {
            match self.connect().await {
                Ok(channel) => {
                    self.channel = Some(channel);
                }
                Err(e) => {
                    error!("{e}");
                }
            }
        }

        Ok(())
    }

    pub async fn notify(&mut self, file_event: FileEvent) -> Result<(), String> {
        if self.channel.is_none() {
            self.channel = Some(self.connect().await?);
        }

        let context = Context::from_value(json!({"file_path": &file_event.path}))
            .map_err(|e| format!("Could not create context: {e}"))?;

        let message = Tera::one_off(&self.message_template, &context, true)
            .map_err(|e| format!("Error rendering template: {}", e))?;

        let mut published = false;

        while !published {
            match self.publish(&message).await {
                Ok(_) => {
                    published = true;
                },
                Err(e) => {
                    error!("{e}");
                    self.reconnect().await?;
                }
            }
        }

        Ok(())
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
    Nack { delivery_tag: u64 },
}

pub struct FileInfo {
    pub source: String,
    pub path: PathBuf,
    pub modified: DateTime<Utc>,
    pub size: i64,
    pub hash: Option<String>,
}
