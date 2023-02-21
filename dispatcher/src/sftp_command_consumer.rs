use std::{fmt, fmt::Display};

use deadpool_lapin::lapin::message::Delivery;
use log::{debug, info, error};

use futures::StreamExt;

use deadpool_lapin::lapin::options::BasicConsumeOptions;
use deadpool_lapin::lapin::types::FieldTable;
use deadpool_lapin::lapin::ConnectionProperties;
use deadpool_lapin::lapin;

use crossbeam_channel::{Sender, TrySendError};
use stream_reconnect::ReconnectStream;

use crate::metrics;

use cortex_core::SftpDownload;

#[derive(Clone, Debug)]
pub enum ConsumeError {
    RabbitMQError(lapin::Error),
}

impl From<lapin::Error> for ConsumeError {
    fn from(err: lapin::Error) -> ConsumeError {
        ConsumeError::RabbitMQError(err)
    }
}

impl Display for ConsumeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConsumeError::RabbitMQError(ref e) => e.fmt(f),
        }
    }
}

#[derive(Clone)]
struct AMQPQueStreamConfig {
    pub address: String,
    pub queue_name: String,
}

impl stream_reconnect::UnderlyingStream<AMQPQueStreamConfig, Result<Delivery, lapin::Error>, lapin::Error> for lapin::Consumer {
    fn establish(config: AMQPQueStreamConfig) -> std::pin::Pin<Box<dyn futures::Future<Output = Result<Self, lapin::Error>> + Send>> {
        Box::pin(async move {
            let amqp_client = lapin::Connection::connect(
                &config.address,
                ConnectionProperties::default(),
            )
            .await?;
        
            let amqp_channel = amqp_client.create_channel().await?;
        
            let id = amqp_channel.id();
            info!("Created SFTP command AMQP channel with id {id}");
       
            let consumer_tag = "cortex-dispatcher";

            let mut options = BasicConsumeOptions::default();

            options.no_ack = true;
        
            // Setup command consuming stream
            let consumer = amqp_channel
                .basic_consume(
                    &config.queue_name,
                    &consumer_tag,
                    options,
                    FieldTable::default(),
                )
                .await?;

            Ok(consumer)
        })
    }

    // The following errors are considered disconnect errors.
    fn is_write_disconnect_error(&self, err: &lapin::Error) -> bool {
        matches!(
            err,
            lapin::Error::IOError(_) | lapin::Error::ProtocolError(_)
        )
    }

    // If an `Err` is read, then there might be an disconnection.
    fn is_read_disconnect_error(&self, item: &Result<Delivery, lapin::Error>) -> bool {
        if let Err(e) = item {
            self.is_write_disconnect_error(e)
        } else {
            false
        }
    }

    // Return "Exhausted" if all retry attempts are failed.
    fn exhaust_err() -> lapin::Error {
        lapin::Error::IOError(std::sync::Arc::new(std::io::Error::new(std::io::ErrorKind::Other, "Exhausted")))
    }
}

type ReconnectMessageStream = ReconnectStream<lapin::Consumer, AMQPQueStreamConfig, Result<Delivery, lapin::Error>, lapin::Error>;

#[derive(Clone)]
struct MessageProcessor {
    pub command_sender: Sender<(u64, SftpDownload)>,
    pub sftp_source_name: String,
}

impl MessageProcessor {
    pub async fn process_message(&self, message: Result<Delivery, lapin::Error>) -> Result<(), String> {
        let delivery = message
            .map_err(|e| {
                format!("Could not read AMQP message: {e}")
            })?;

        let action_command_sender = self.command_sender.clone();

        metrics::MESSAGES_RECEIVED_COUNTER
            .with_label_values(&[&self.sftp_source_name])
            .inc();

        let sftp_download: SftpDownload =
            serde_json::from_slice(delivery.data.as_slice())
            .map_err(|e| {
                format!("Error deserializing message: {e}")
            })?;

        action_command_sender
            .try_send((delivery.delivery_tag, sftp_download))
            .map_err(|e| {
                match e {
                    TrySendError::Disconnected(_) => format!("Channel disconnected"),
                    TrySendError::Full(_) => format!("Could not send command on channel: channel full"),
                }
            })?;

        Ok(())
    }
}

pub async fn start(
    amqp_address: String,
    sftp_source_name: String,
    command_sender: Sender<(u64, SftpDownload)>,
) -> Result<(), ConsumeError> {
    let queue_name = format!("source.{}", &sftp_source_name);

    let amqp_stream_config = AMQPQueStreamConfig {
        address: amqp_address.clone(),
        queue_name: queue_name.clone(),
    };

    let mut consumer = ReconnectMessageStream::connect(amqp_stream_config).await?;

    let message_processor = MessageProcessor {
        command_sender,
        sftp_source_name: sftp_source_name.clone(),
    };

    let m = message_processor.clone();

    while let Some(message) = consumer.next().await {
        match m.process_message(message).await {
            Ok(_) => {
                debug!("Received message from AMQP queue '{}'", &queue_name);
            },
            Err(e) => {
                error!("Could not process message: {e}")
            }
        }
    }

    Ok(())
}
