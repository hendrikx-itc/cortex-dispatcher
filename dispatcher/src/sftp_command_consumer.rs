use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

extern crate failure;

extern crate lapin;
use lapin::{
  Connection, Channel, ConsumerDelegate,
  message::Delivery,
  options::*,
  types::FieldTable,
};

use crossbeam_channel::Sender;

use crate::metrics;
use crate::settings;

use cortex_core::SftpDownload;

pub struct SftpCommandConsumer
{
    pub sftp_source: settings::SftpSource,
    pub channel: Channel,
    pub sender: Sender<SftpDownload>
}

impl ConsumerDelegate for SftpCommandConsumer 
{
    fn on_new_delivery(&self, delivery: Delivery) {
        metrics::MESSAGES_RECEIVED_COUNTER
            .with_label_values(&[&self.sftp_source.name])
            .inc();
        debug!("Received message from RabbitMQ");

        let deserialize_result: serde_json::Result<SftpDownload> =
            serde_json::from_slice(delivery.data.as_slice());

        match deserialize_result {
            Ok(command) => {
                let send_result = self.sender.send(command);

                match send_result {
                    Ok(_) => debug!("Message sent to downloaders"),
                    Err(e) => error!("Error sending message to downloaders: {}", e)
                }

                self.channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).as_error().expect("basic_ack");
            }
            Err(e) => {
                error!("Error deserializing command: {}", e);
                self.channel.basic_nack(delivery.delivery_tag, BasicNackOptions::default()).as_error().expect("basic_nack");
            }
        }

    }
}

impl SftpCommandConsumer
{
    pub fn start(
    	stop: Arc<AtomicBool>,
        amqp_client: Connection,
        config: settings::SftpSource,
        sender: Sender<SftpDownload>
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let channel = amqp_client.create_channel().wait().expect("create_channel");

            let sftp_downloader = Box::new(SftpCommandConsumer {
                sftp_source: config.clone(),
                channel: channel.clone(),
                sender: sender.clone()
            });

            info!("Created channel with id {}", channel.id());

            let queue_name = format!("source.{}", &config.name);

            let queue = channel.queue_declare(
                &queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            ).wait().expect("queue_declare");

            info!("Channel {} declared queue {}", channel.id(), &queue_name);

            let routing_key = format!("source.{}", &config.name);
            let exchange = "amq.direct";

            channel
                .queue_bind(&queue_name, &exchange, &routing_key, QueueBindOptions::default(), FieldTable::default())
                .wait()
                .expect("queue_bind");

            channel
                .basic_consume(&queue, "cortex-dispatcher", BasicConsumeOptions::default(), FieldTable::default())
                .wait()
                .expect("basic_consume")
                .set_delegate(sftp_downloader);

        	while !stop.load(Ordering::Relaxed) {
                thread::sleep(time::Duration::from_millis(100));
            }

            debug!("SFTP source stream ended");
        })
    }
}
