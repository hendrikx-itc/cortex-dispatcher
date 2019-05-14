use std::thread;

use actix_web::{actix, server, App, HttpRequest, middleware};

use prometheus::{TextEncoder, Encoder};


pub fn start_http_server(addr: std::net::SocketAddr) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let sys = actix::System::new("sftp-scanner");

        server::HttpServer::new(|| {
            App::new()
                .middleware(middleware::Logger::default())
                .resource("/metrics", |r| r.f(metrics))                
        }).bind(addr).unwrap().start();

        sys.run();
    })
}

fn metrics(_req: &HttpRequest) -> String {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer).unwrap();

    String::from_utf8(buffer).unwrap()
}
