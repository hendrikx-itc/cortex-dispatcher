use std::thread;

use actix_files;
use actix_rt;
use actix_web::{middleware, web, App, HttpRequest, HttpServer, Responder, HttpResponse};

use prometheus::{Encoder, TextEncoder};

use crate::base_types::CortexConfig;


pub fn start_http_server(
    addr: std::net::SocketAddr,
    static_content: std::path::PathBuf,
    cortex_config: CortexConfig
) -> (thread::JoinHandle<()>, actix_rt::System, actix_web::dev::Server) {
    let (tx, rx) = std::sync::mpsc::channel();
    let (tx_http, rx_http) = std::sync::mpsc::channel();

    let join_handle = thread::spawn(move || {
        let system = actix_rt::System::new("http_server");

        let local_static_content = static_content.clone();

        info!(
            "Serving static content from {}",
            static_content.to_str().unwrap().to_string()
        );

        let server = HttpServer::new(move || {
            App::new()
                .data(cortex_config.clone())
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("Access-Control-Allow-Origin", "*"))
                .service(web::resource("/api/metrics").to(metrics))
//                .service(web::resource("/api/sftp-sources").to(sftp_sources_list))
//                .service(web::resource("/api/directory-targets").to(directory_targets_list))
//                .service(web::resource("/api/connections").to(connections_list))
                .service(
                    actix_files::Files::new("/", &local_static_content).index_file("index.html"),
                )
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

async fn metrics(req: HttpRequest) -> impl Responder {
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

async fn sftp_sources_list(cortex_config: CortexConfig, req: HttpRequest) -> HttpResponse {
    let sftp_sources = cortex_config.sftp_sources.lock().unwrap();

    let body = serde_json::to_string(&*sftp_sources).unwrap();

    HttpResponse::Ok().body(body)
}

async fn directory_targets_list(cortex_config: CortexConfig, _req: HttpRequest) -> impl Responder {
    let directory_targets = cortex_config.directory_targets.lock().unwrap();

    serde_json::to_string(&*directory_targets).unwrap()
}

async fn connections_list(cortex_config: CortexConfig, _req: HttpRequest) -> impl Responder {
    let connections = cortex_config.connections.lock().unwrap();

    serde_json::to_string(&*connections).unwrap()
}
