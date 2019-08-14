use std::thread;
use std::time;

extern crate lapin_futures;

use lapin_futures::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin_futures::types::FieldTable;

use tokio::prelude::*;

use crossbeam_channel::Sender;

//use retry::retry;
//use retry::delay::{Exponential, jitter};

use crate::metrics;

use cortex_core::SftpDownload;


pub fn start(
    amqp_client: lapin_futures::Client,
    sftp_source_name: String,
    command_sender: Sender<SftpDownload>
) -> impl Future<Item=(), Error=lapin_futures::Error> {
    let sftp_source_name_2 = sftp_source_name.clone();

    amqp_client
        .create_channel()
        .and_then(move |channel| {
            let ch = channel.clone();
            let id = channel.id();
            info!("Created channel with id {}", id);

            let consumer_tag = "cortex-dispatcher";
            let queue_name = format!("source.{}", &sftp_source_name);

            channel.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::default()).and_then(move |queue| {
                info!("channel {} declared queue '{}'", id, &queue_name);
                let routing_key = format!("source.{}", &sftp_source_name);
                let exchange = "amq.direct";
                channel.queue_bind(
                    &queue_name,
                    &exchange,
                    &routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                ).and_then(move |_| {
                    debug!("Queue '{}' bound to exchange '{}' for routing key '{}'", &queue_name, &exchange, &routing_key);
                    channel.basic_consume(&queue, &consumer_tag, BasicConsumeOptions::default(), FieldTable::default())
                })
            }).and_then(|stream|{
                stream.for_each(move |message| {
                    debug!("Received message from AMQP queue");
                    metrics::MESSAGES_RECEIVED_COUNTER
                        .with_label_values(&[&sftp_source_name_2])
                        .inc();

                    let deserialize_result: serde_json::Result<SftpDownload> = serde_json::from_slice(message.data.as_slice());

                    match deserialize_result {
                        Ok(sftp_download) => {
                            let send_result = command_sender.try_send(sftp_download.clone());

                            match send_result {
                                Ok(_) => debug!("Sent command on channel"),
                                Err(e) => {
                                    error!("Error sending command on channel: {}", e);
                                    thread::sleep(time::Duration::from_millis(1000));
                                }
                            }

                            ch.basic_ack(message.delivery_tag, false)
                        },
                        Err(e) => {
                            error!("Error deserializing message: {}", e);
                            ch.basic_nack(message.delivery_tag, false, false)
                        }
                    }
                })
            })
        })
}
