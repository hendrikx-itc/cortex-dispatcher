use std::thread;
use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};

use lapin::{BasicProperties, options::BasicPublishOptions};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use serde_json;

use cortex_core::SftpDownload;

use log::{debug, info, error};

use lapin;


pub fn start_sender(stop: Arc<AtomicBool>, receiver: Receiver<SftpDownload>, address: String) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let amqp_conn = lapin::Connection::connect(&address, lapin::ConnectionProperties::default())
            .wait()
            .expect("connection error");

        let channel = amqp_conn.create_channel().wait().expect("create_channel");
        info!("Created channel with id {}", channel.id());

        let exchange = "amq.direct";
        let properies = BasicProperties::default().with_delivery_mode(2);

        while !stop.load(Ordering::Relaxed) {
            let receive_result = receiver.recv_timeout(Duration::from_millis(100));

            match receive_result {
                Ok(command) => {
                    let command_str = serde_json::to_string(&command).unwrap();
                    let routing_key = format!("source.{}", &command.sftp_source);

                    channel.basic_publish(exchange, &routing_key, BasicPublishOptions::default(), command_str.as_bytes().to_vec(), properies)
                        .wait()
                        .expect("basic_publish");
                    debug!("Sent on AMQP");
                },
                Err(e) => {
                    match e {
                        RecvTimeoutError::Timeout => (),
                        RecvTimeoutError::Disconnected => error!("Error receiving from channel: {}", e)
                    }
                }
            } 
        }

        debug!("SFTP source stream ended");
    })
}
