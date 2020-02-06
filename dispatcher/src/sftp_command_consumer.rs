use std::{fmt, fmt::Display};
use std::time::Duration;

extern crate lapin_futures;

use futures::future::Future;

use lapin_futures::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin_futures::types::FieldTable;

use tokio::prelude::*;

use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use crossbeam_channel::{Sender, TrySendError};

use crate::metrics;
use crate::base_types::MessageResponse;

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

impl From<lapin_futures::Error> for ConsumeError {
    fn from(_err: lapin_futures::Error) -> ConsumeError {
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

fn setup_message_responder(channel: lapin_futures::Channel, ack_receiver: tokio::sync::mpsc::Receiver<MessageResponse>) -> impl Future<Item=(), Error=()> {
	ack_receiver.map_err(|e| {
		error!("Error receiving message response from stream: {}", e)
	}).for_each(move |message_response| {
		match message_response {
			MessageResponse::Ack { delivery_tag } => {
				futures::future::Either::A(channel.basic_ack(delivery_tag, false).map_err(|e| {
					error!("Error sending Ack on AMQP channel: {}", e)
				}))
			},
			MessageResponse::Nack { delivery_tag } => {
				futures::future::Either::B(channel.basic_nack(delivery_tag, false, false).map_err(|e| {
					error!("Error sending Nack on AMQP channel: {}", e)
				}))
			}
		}
	})
}

pub fn start(
    amqp_client: lapin_futures::Client,
    sftp_source_name: String,
	ack_receiver: tokio::sync::mpsc::Receiver<MessageResponse>,
    command_sender: Sender<(u64, SftpDownload)>
) -> Box<dyn Future<Item=(), Error=()> + Send> {
    let sftp_source_name_2 = sftp_source_name.clone();

	let channel_future = amqp_client.create_channel()
		.map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError));

	let future = channel_future.and_then(move |channel| {
		let ch = channel.clone();
		let id = channel.id();
		info!("Created channel with id {}", id);

		tokio::spawn(setup_message_responder(channel.clone(), ack_receiver));

		let consumer_tag = "cortex-dispatcher";
		let queue_name = format!("source.{}", &sftp_source_name);
		let stream_handler_queue_name = queue_name.clone();

		let queue_declare_future = channel
			.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::default())
			.map_err(|_| ConsumeError::from(ConsumeErrorKind::SetupError));

		let consume_future = queue_declare_future.and_then(move |queue| {
			info!("channel {} declared queue '{}'", id, &queue_name);
			let routing_key = format!("source.{}", &sftp_source_name);
			let exchange = "amq.direct";

			channel.queue_bind(
				&queue_name,
				&exchange,
				&routing_key,
				QueueBindOptions::default(),
				FieldTable::default(),
			).map_err(|_| ConsumeError::from(ConsumeErrorKind::SetupError)).and_then(move |_| {
				debug!("Queue '{}' bound to exchange '{}' for routing key '{}'", &queue_name, &exchange, &routing_key);

				// Setup command consuming stream
				channel
					.basic_consume(&queue, &consumer_tag, BasicConsumeOptions::default(), FieldTable::default())
					.map_err(|_| ConsumeError::from(ConsumeErrorKind::SetupError))
			})
		});

		consume_future.and_then(|stream| {
			stream.map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError)).for_each(move |message| {
				let action_source_name = sftp_source_name_2.clone();
				let action_command_sender = command_sender.clone();
				let or_else_delivery_tag = message.delivery_tag;

				let queue_name = stream_handler_queue_name.clone();

				let action = move || {
					debug!("Received message from AMQP queue '{}'", &queue_name);
					metrics::MESSAGES_RECEIVED_COUNTER
						.with_label_values(&[&action_source_name])
						.inc();

					let deserialize_result: serde_json::Result<SftpDownload> = serde_json::from_slice(message.data.as_slice());

					match deserialize_result {
						Ok(sftp_download) => {
							let send_result = action_command_sender.try_send((message.delivery_tag, sftp_download));
							
							match send_result {
								Ok(_) => Ok(()),
								Err(e) => {
									match e {
										TrySendError::Disconnected(_) => Err(e.context(ConsumeErrorKind::ChannelDisconnected)),
										TrySendError::Full(_) => Err(e.context(ConsumeErrorKind::ChannelFull))
									}
								}
							}
						},
						Err(e) => {
							Err(e.context(ConsumeErrorKind::DeserializeError))
						}
					}
				};

				fn condition(e: &Context<ConsumeErrorKind>) -> bool {
					match e.get_context() {
						ConsumeErrorKind::DeserializeError{..} => false,
						ConsumeErrorKind::ChannelDisconnected{..} => false,
						_ => true
					}
				};

				let retry_strategy = ExponentialBackoff::from_millis(10)
					.max_delay(Duration::from_millis(10000))
					.map(jitter)
					.take(7);

				let or_else_ch = ch.clone();

				RetryIf::spawn(retry_strategy, action, condition)
					.and_then(|_| futures::future::ok(()) )
					.or_else(move |e| {
						let map_to_empty = |_| ();
						let map_to_consume_err = |_| ConsumeError::from(ConsumeErrorKind::NackFailure);

						let requeue_message = match e {
							tokio_retry::Error::OperationError(er) => {
								match er.get_context() {
									ConsumeErrorKind::DeserializeError => {
										error!("Error deserializing message: {}", er);
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
								}
							},
							tokio_retry::Error::TimerError(te) => {
								error!("Retry timer error: {}", te);
								true
							}
						};

						or_else_ch.basic_nack(or_else_delivery_tag, false, requeue_message)
							.map(map_to_empty)
							.map_err(map_to_consume_err)
					})
			})
		})
	}).map_err(|_| ());

	Box::new(future)
}
