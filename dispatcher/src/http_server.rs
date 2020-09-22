use std::thread;

use actix_web::{middleware, web, App, HttpRequest, HttpServer, Responder};

use prometheus::{Encoder, TextEncoder};


pub fn start_http_server(
    addr: std::net::SocketAddr,
) -> (thread::JoinHandle<()>, actix_rt::System, actix_web::dev::Server) {
    let (tx, rx) = std::sync::mpsc::channel();
    let (tx_http, rx_http) = std::sync::mpsc::channel();

    let join_handle = thread::spawn(move || {
        let system = actix_rt::System::new("http_server");

        let server = HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("Access-Control-Allow-Origin", "*"))
                .service(web::resource("/api/metrics").to(metrics))
        })
        .disable_signals()
        .bind(addr)
        .unwrap()
        .run();

        tx.send(actix_rt::System::current()).unwrap();
        tx_http.send(server).unwrap();

        system.run().unwrap();

        debug!("http server shutdown");
    });

    let system = rx.recv().unwrap();
    let server = rx_http.recv().unwrap();

    (join_handle, system, server)
}

async fn metrics(_req: HttpRequest) -> impl Responder {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();

    let encode_result = encoder.encode(&metric_families, &mut buffer);

    match encode_result {
        Ok(_) => {}
        Err(e) => error!("[E02011] Error encoding metrics: {}", e),
    }

    String::from_utf8(buffer).unwrap()
}
