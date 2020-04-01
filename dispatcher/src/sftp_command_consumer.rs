use std::{fmt, fmt::Display};

extern crate lapin;

use futures::StreamExt;

use lapin::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions, BasicNackOptions, BasicAckOptions};
use lapin::types::FieldTable;

use crossbeam_channel::{Sender, TrySendError};

use crate::base_types::MessageResponse;
use crate::metrics;

use cortex_core::SftpDownload;

use failure::{Fail, Context, Backtrace};

#[derive(Debug)]
struct ConsumeError {
    inner: Context<ConsumeErrorKind>
}

impl From<ConsumeErrorKind> for ConsumeError {
    fn from(kind: ConsumeErrorKind) -> ConsumeError {
        ConsumeError { inner: Context::new(kind) }
    }
}

impl From<Context<ConsumeErrorKind>> for ConsumeError {
    fn from(inner: Context<ConsumeErrorKind>) -> ConsumeError {
        ConsumeError { inner: inner }
    }
}

impl From<lapin::Error> for ConsumeError {
    fn from(_err: lapin::Error) -> ConsumeError {
		ConsumeError::from(ConsumeErrorKind::UnknownStreamError)
	}
}

impl ConsumeError {
    pub fn kind(&self) -> ConsumeErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for ConsumeError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for ConsumeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
enum ConsumeErrorKind {
    ChannelFull,
    ChannelDisconnected,
    DeserializeError,
    UnknownStreamError,
	NackFailure,
	SetupError
}

impl Display for ConsumeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("ConsumeErrorKind")
    }
}

async fn setup_message_responder(channel: lapin::Channel, mut ack_receiver: tokio::sync::mpsc::Receiver<MessageResponse>) {
	while let Some(message_response) = ack_receiver.next().await {
		match message_response {
			MessageResponse::Ack { delivery_tag } => {
				channel.basic_ack(delivery_tag, BasicAckOptions { multiple: false }).await;
			},
			MessageResponse::Nack { delivery_tag } => {
				channel.basic_nack(delivery_tag, BasicNackOptions { multiple: false, requeue: false }).await;
			}
		}
	}
}

#[derive(Debug)]
pub enum SftpCommandConsumeError {
	RabbitMQError(lapin::Error)
}

impl fmt::Display for SftpCommandConsumeError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			SftpCommandConsumeError::RabbitMQError(ref e) => e.fmt(f),
		}
	}
}

impl std::error::Error for SftpCommandConsumeError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match *self {
			SftpCommandConsumeError::RabbitMQError(ref e) => Some(e),
		}
	}
}

impl From<lapin::Error> for SftpCommandConsumeError {
	fn from(err: lapin::Error) -> SftpCommandConsumeError {
		SftpCommandConsumeError::RabbitMQError(err)
	}
}

pub async fn start(
    amqp_client: lapin::Connection,
    sftp_source_name: String,
	ack_receiver: tokio::sync::mpsc::Receiver<MessageResponse>,
    command_sender: Sender<(u64, SftpDownload)>
) -> Result<(), SftpCommandConsumeError> {
    let sftp_source_name_2 = sftp_source_name.clone();

	let channel_resp = amqp_client.create_channel().await;

	let channel = channel_resp.unwrap();

	let id = channel.id();
	info!("Created channel with id {}", id);

	//tokio::spawn(setup_message_responder(channel.clone(), ack_receiver));

	let consumer_tag = "cortex-dispatcher";
	let queue_name = format!("source.{}", &sftp_source_name);

	let queue_declare_result = channel
		.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::default()).await;

	let queue = queue_declare_result.unwrap();

	info!("channel {} declared queue '{}'", id, &queue_name);
	let routing_key = format!("source.{}", &sftp_source_name);
	let exchange = "amq.direct";

	channel.queue_bind(
		&queue_name,
		&exchange,
		&routing_key,
		QueueBindOptions::default(),
		FieldTable::default(),
	).await?;

	debug!("Queue '{}' bound to exchange '{}' for routing key '{}'", &queue_name, &exchange, &routing_key);

	// Setup command consuming stream
	let consume_result = channel.basic_consume(
		&queue_name, &consumer_tag, BasicConsumeOptions::default(), FieldTable::default()
	).await;

	let mut consumer = consume_result.unwrap();

	while let Some(message) = consumer.next().await {
		let message = message.unwrap();
		let action_command_sender = command_sender.clone();

		debug!("Received message from AMQP queue '{}'", &queue_name);
		metrics::MESSAGES_RECEIVED_COUNTER
			.with_label_values(&[&sftp_source_name_2])
			.inc();

		let deserialize_result: serde_json::Result<SftpDownload> = serde_json::from_slice(message.data.as_slice());

		let result = match deserialize_result {
			Ok(sftp_download) => {
				let send_result = action_command_sender.try_send((message.delivery_tag, sftp_download));
				
				match send_result {
					Ok(_) => Ok(()),
					Err(e) => {
						match e {
							TrySendError::Disconnected(_) => Err(ConsumeErrorKind::ChannelDisconnected),
							TrySendError::Full(_) => Err(ConsumeErrorKind::ChannelFull)
						}
					}
				}
			},
			Err(e) => {
				Err(ConsumeErrorKind::DeserializeError)
			}
		};

		if let Err(e) = result {
			let requeue_message = match e {
				ConsumeErrorKind::DeserializeError => {
					error!("Error deserializing message: {}", e);
					false
				}
				ConsumeErrorKind::ChannelFull => {
					error!("Error sending command on channel: channel full");
					// Put the message back on the queue, because we could temporarily not process it
					true
				},
				ConsumeErrorKind::ChannelDisconnected => {
					error!("Channel disconnected");
					true
				},
				ConsumeErrorKind::UnknownStreamError => {
					error!("Unknown stream error");
					true
				},
				ConsumeErrorKind::NackFailure => {
					error!("Error sending nack");
					true
				},
				ConsumeErrorKind::SetupError => {
					error!("Setup error should not occur here");
					true
				}
			};

			channel.basic_nack(message.delivery_tag, BasicNackOptions{ multiple: false, requeue: requeue_message}).await;
		}
	}

	Ok(())
}
