use std::thread;

use log::{error, info};

use actix_web::{middleware, web, App, HttpServer, Responder, HttpResponse};

use prometheus::{Encoder, TextEncoder};

pub fn start_http_server(addr: std::net::SocketAddr) -> (thread::JoinHandle<()>, actix_rt::System, actix_web::dev::Server) {
    let (tx, rx) = std::sync::mpsc::channel();
    let (tx_http, rx_http) = std::sync::mpsc::channel();

    let join_handle = thread::spawn(move || {
        let system = actix_rt::System::new("http_server");

        let bind_result = HttpServer::new(|| {
            App::new()
                .wrap(middleware::Logger::default())
                .service(web::resource("/api/metrics").to(metrics))
        })
        .bind(addr);

        let server = match bind_result {
            Ok(http_server) => {
                info!("Web server bound to address: {}", addr);
                Some(http_server.run())
            },
            Err(e) => {
                error!("Could not bind to address {}: {}", addr, e);
                None
            }
        };

        tx.send(actix_rt::System::current()).unwrap();

        match server {
            Some(s) => tx_http.send(s).unwrap(),
            None => ()
        }        

        system.run().unwrap();
    });

    // Get data from the thread that was just started
    let system = rx.recv().unwrap();
    let server = rx_http.recv().unwrap();

    (join_handle, system, server)
}

async fn metrics() -> impl Responder {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();

    let encode_result = encoder.encode(&metric_families, &mut buffer);

    match encode_result {
        Ok(_) => {}
        Err(e) => error!("Error encoding metrics: {}", e),
    }

    HttpResponse::from(String::from_utf8(buffer).unwrap())
}
