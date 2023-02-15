use std::{fmt, fmt::Display, time};

use deadpool_lapin::lapin::message::Delivery;
use log::{debug, info, error};

use futures::StreamExt;

use deadpool_lapin::lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use deadpool_lapin::lapin::types::FieldTable;
use deadpool_lapin::lapin::ConnectionProperties;
use deadpool_lapin::lapin;

use crossbeam_channel::{Sender, TrySendError};
use stream_reconnect::ReconnectStream;

use crate::base_types::MessageResponse;
use crate::metrics;

use cortex_core::SftpDownload;

#[derive(Clone, Debug)]
pub enum ConsumeError {
    ChannelFull,
    ChannelDisconnected,
    DeserializeError,
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
            _ => f.write_str("ConsumeError"),
        }
    }
}

async fn setup_message_responder(
    channel: lapin::Channel,
    ack_receiver: async_channel::Receiver<MessageResponse>,
) -> Result<(), ConsumeError> {
    while let Ok(message_response) = ack_receiver.recv().await {
        match message_response {
            MessageResponse::Ack { delivery_tag } => {
                channel
                    .basic_ack(delivery_tag, BasicAckOptions { multiple: false })
                    .await?;
                debug!("Sent Ack for {delivery_tag}");
            }
            MessageResponse::Nack { delivery_tag } => {
                channel
                    .basic_nack(
                        delivery_tag,
                        BasicNackOptions {
                            multiple: false,
                            requeue: false,
                        },
                    )
                    .await?;
                debug!("Sent Nack for {delivery_tag}");
            }
        }
    }

    Ok(())
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
        
            // Setup command consuming stream
            let consumer = amqp_channel
                .basic_consume(
                    &config.queue_name,
                    &consumer_tag,
                    BasicConsumeOptions::default(),
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
        let delivery = match message {
            Ok(v) => v,
            Err(e) => {
                error!("Could not read AMQP message: {e}");
                return Ok(());
            }
        };

        let action_command_sender = self.command_sender.clone();

        metrics::MESSAGES_RECEIVED_COUNTER
            .with_label_values(&[&self.sftp_source_name])
            .inc();

        let deserialize_result: serde_json::Result<SftpDownload> =
            serde_json::from_slice(delivery.data.as_slice());

        let result = match deserialize_result {
            Ok(sftp_download) => {
                let send_result =
                    action_command_sender.try_send((delivery.delivery_tag, sftp_download));

                match send_result {
                    Ok(_) => Ok(()),
                    Err(e) => match e {
                        TrySendError::Disconnected(_) => Err(ConsumeError::ChannelDisconnected),
                        TrySendError::Full(_) => Err(ConsumeError::ChannelFull),
                    },
                }
            }
            Err(_e) => Err(ConsumeError::DeserializeError),
        };

        if let Err(e) = result {
            let requeue_message = match e {
                ConsumeError::DeserializeError => {
                    error!("Error deserializing message: {e}");
                    false
                }
                ConsumeError::ChannelFull => {
                    debug!("Could not send command on channel: channel full");
                    tokio::time::sleep(time::Duration::from_millis(200)).await;
                    // Put the message back on the queue, because we could temporarily not process
                    // it
                    true
                }
                ConsumeError::ChannelDisconnected => {
                    error!("Channel disconnected");
                    true
                }
                ConsumeError::RabbitMQError(e) => {
                    error!("{}", e);
                    true
                }
            };

            if requeue_message {
                debug!("Should requeue message");
            }

            // self.amqp_channel
            //     .basic_nack(
            //         delivery.delivery_tag,
            //         BasicNackOptions {
            //             multiple: false,
            //             requeue: requeue_message,
            //         },
            //     )
            //     .await
            //     .map_err(|e| format!("Error sending nack: {e}"))?;
        }

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
