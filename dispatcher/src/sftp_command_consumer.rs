use std::{fmt, fmt::Display};

extern crate lapin_futures;

use futures::future::Future;

use lapin_futures::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin_futures::types::FieldTable;

use tokio::prelude::*;

use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use crossbeam_channel::{Sender, TrySendError};

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

impl From<lapin_futures::Error> for ConsumeError {
    fn from(err: lapin_futures::Error) -> ConsumeError {
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
	RetryFailure,
    UnknownStreamError,
	AckFailure,
	NackFailure
}

impl Display for ConsumeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("ConsumeErrorKind")
    }
}

pub fn start(
    amqp_client: lapin_futures::Client,
    sftp_source_name: String,
    command_sender: Sender<SftpDownload>
) -> Box<dyn Future<Item=(), Error=()> + Send> {
    let sftp_source_name_2 = sftp_source_name.clone();

	let channel_future = amqp_client.create_channel();
	let channel_future = channel_future.map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError));

	let future = channel_future.and_then(move |channel| {
		let ch = channel.clone();
		let id = channel.id();
		info!("Created channel with id {}", id);

		let consumer_tag = "cortex-dispatcher";
		let queue_name = format!("source.{}", &sftp_source_name);

		let queue_declare_future = channel
			.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::default())
			.map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError));

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
			).map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError)).and_then(move |_| {
				debug!("Queue '{}' bound to exchange '{}' for routing key '{}'", &queue_name, &exchange, &routing_key);
				channel
					.basic_consume(&queue, &consumer_tag, BasicConsumeOptions::default(), FieldTable::default())
					.map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError))
			})
		});

		let handled_stream = consume_future.and_then(|stream|{
			stream.map_err(|_| ConsumeError::from(ConsumeErrorKind::UnknownStreamError)).and_then(move |message| {
				let action_source_name = sftp_source_name_2.clone();
				let action_command_sender = command_sender.clone();
				let then_delivery_tag = message.delivery_tag;
				let or_else_delivery_tag = message.delivery_tag;

				let action = move || {
					debug!("Received message from AMQP queue");
					metrics::MESSAGES_RECEIVED_COUNTER
						.with_label_values(&[&action_source_name])
						.inc();

					let deserialize_result: serde_json::Result<SftpDownload> = serde_json::from_slice(message.data.as_slice());

					match deserialize_result {
						Ok(sftp_download) => {
							let send_result = action_command_sender.try_send(sftp_download.clone());
							
							match send_result {
								Ok(_) => Ok(message.delivery_tag),
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
					.map(jitter)
					.take(3);

				let or_else_ch = ch.clone();
				let and_then_ch = ch.clone();

				RetryIf::spawn(retry_strategy, action, condition)
					.map_err(|_| ConsumeError::from(ConsumeErrorKind::RetryFailure))
					.then(move |_| {
						debug!("Sent command on channel");
						and_then_ch.basic_ack(then_delivery_tag, false)
							.map(|_| ())
							.map_err(|_| ConsumeError::from(ConsumeErrorKind::AckFailure))
					})
					.or_else(move |e| {
						let map_to_empty = |_| ();
						let map_to_consume_err = |_| ConsumeError::from(ConsumeErrorKind::NackFailure);
						match e.inner.get_context() {
							ConsumeErrorKind::DeserializeError => {
								error!("Error deserializing message: {}", e);
								or_else_ch.basic_nack(or_else_delivery_tag, false, false)
									.map(map_to_empty)
									.map_err(map_to_consume_err)
							}
							ConsumeErrorKind::ChannelFull => {
								error!("Error sending command on channel: channel full");
								// Put the message back on the queue, because we could temporarily not process it
								or_else_ch.basic_nack(or_else_delivery_tag, false, true)
									.map(map_to_empty)
									.map_err(map_to_consume_err)
							},
							ConsumeErrorKind::ChannelDisconnected => {
								error!("Channel disconnected");
								or_else_ch.basic_nack(or_else_delivery_tag, false, true)
									.map(map_to_empty)
									.map_err(map_to_consume_err)
							}
							_ => {
								error!("Other error");
								or_else_ch.basic_nack(or_else_delivery_tag, false, true)
									.map(map_to_empty)
									.map_err(map_to_consume_err)
							}
						}
					})
			}).for_each(|_| Ok(()))
		});

		handled_stream
	}).map_err(|e| error!("Unexpected error: {}", e));

	Box::new(future)
}
