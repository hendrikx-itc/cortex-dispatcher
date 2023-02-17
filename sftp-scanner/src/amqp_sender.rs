use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lapin::{options::BasicPublishOptions, BasicProperties};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use cortex_core::SftpDownload;

use log::{debug, error, info};

pub async fn start_sender(
    stop: Arc<AtomicBool>,
    receiver: Receiver<SftpDownload>,
    address: String,
) {
    let amqp_conn =
        lapin::Connection::connect(&address, lapin::ConnectionProperties::default())
            .await
            .expect("connection error");

    let channel = amqp_conn.create_channel().await.expect("create_channel");
    info!("Created channel with id {}", channel.id());

    let exchange = "amq.direct";

    while !stop.load(Ordering::Relaxed) {
        let receive_result = receiver.recv_timeout(Duration::from_millis(100));

        match receive_result {
            Ok(command) => {
                let command_str = serde_json::to_string(&command).unwrap();
                let routing_key = format!("source.{}", &command.sftp_source);

                channel
                    .basic_publish(
                        exchange,
                        &routing_key,
                        BasicPublishOptions::default(),
                        command_str.as_bytes(),
                        BasicProperties::default(),
                    )
                    .await
                    .expect("basic_publish");

                debug!("Sent on AMQP with routing key '{}'", &routing_key);
            }
            Err(e) => match e {
                RecvTimeoutError::Timeout => (),
                RecvTimeoutError::Disconnected => error!("Error receiving from channel: {}", e),
            },
        }
    }

    debug!("SFTP source stream ended");
}
