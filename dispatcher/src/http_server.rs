use std::thread;

use actix_rt;
use actix_web::{web, App, HttpServer, middleware, Responder};

use prometheus::{TextEncoder, Encoder};


pub fn start_http_server(addr: std::net::SocketAddr) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let system = actix_rt::System::new("http_server");

        HttpServer::new(|| {
            App::new()
                .wrap(middleware::Logger::default())
                .service(
                    web::resource("/metrics").to(metrics)
                )
        }).bind(addr).unwrap().start();

        system.run().unwrap();
    })
}

fn metrics() -> impl Responder {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();

    let encode_result = encoder.encode(&metric_families, &mut buffer);
    
    match encode_result {
        Ok(_) => {},
        Err(e) => {
            error!("Error encoding metrics: {}", e)
        }
    }

    String::from_utf8(buffer).unwrap()
}
