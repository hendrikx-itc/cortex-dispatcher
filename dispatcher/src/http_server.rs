use std::thread;

use actix_files;
use actix_http::{Error, Response};
use actix_rt;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};

use futures::future::{ok, FutureResult};

use prometheus::{Encoder, TextEncoder};

use crate::base_types::CortexConfig;
use crate::settings;

struct SftpResponse {
    pub sftp_sources: std::sync::Arc<std::sync::Mutex<Vec<settings::SftpSource>>>,
}

impl Responder for SftpResponse {
    type Error = Error;
    type Future = FutureResult<Response, Error>;

    fn respond_to(self, _req: &HttpRequest) -> Self::Future {
        let sftp_sources = self.sftp_sources.lock().unwrap();

        let result = serde_json::to_string(&*sftp_sources).unwrap();

        //let mut r = HttpResponse::Ok();

        //ok(r.finish())
        ok(HttpResponse::from(result))
    }
}

struct DirectoryTargetsResponse {
    pub directory_targets: std::sync::Arc<std::sync::Mutex<Vec<settings::DirectoryTarget>>>,
}

impl Responder for DirectoryTargetsResponse {
    type Error = Error;
    type Future = FutureResult<Response, Error>;

    fn respond_to(self, _req: &HttpRequest) -> Self::Future {
        let directory_targets = self.directory_targets.lock().unwrap();

        let result = serde_json::to_string(&*directory_targets).unwrap();

        ok(HttpResponse::from(result))
    }
}

struct ConnectionsResponse {
    pub connections: std::sync::Arc<std::sync::Mutex<Vec<settings::Connection>>>,
}

impl Responder for ConnectionsResponse {
    type Error = Error;
    type Future = FutureResult<Response, Error>;

    fn respond_to(self, _req: &HttpRequest) -> Self::Future {
        let connections = self.connections.lock().unwrap();

        let result = serde_json::to_string(&*connections).unwrap();

        ok(HttpResponse::from(result))
    }
}

pub fn start_http_server(
    addr: std::net::SocketAddr,
    static_content: std::path::PathBuf,
    cortex_config: CortexConfig,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let system = actix_rt::System::new("http_server");

        let local_static_content = static_content.clone();

        info!(
            "Serving static content from {}",
            static_content.to_str().unwrap().to_string()
        );

        let sftp_sources = cortex_config.sftp_sources.clone();
        let directory_targets = cortex_config.directory_targets.clone();
        let connections = cortex_config.connections.clone();

        HttpServer::new(move || {
            let sftp_sources = sftp_sources.clone();
            let sftp_sources_resp = move || -> SftpResponse {
                SftpResponse {
                    sftp_sources: sftp_sources.clone(),
                }
            };

            let directory_targets = directory_targets.clone();
            let directory_targets_resp = move || -> DirectoryTargetsResponse {
                DirectoryTargetsResponse {
                    directory_targets: directory_targets.clone(),
                }
            };

            let connections = connections.clone();
            let connections_resp = move || -> ConnectionsResponse {
                ConnectionsResponse {
                    connections: connections.clone(),
                }
            };

            App::new()
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("Access-Control-Allow-Origin", "*"))
                .service(web::resource("/api/metrics").to(metrics))
                .service(web::resource("/api/sftp-sources").to(sftp_sources_resp))
                .service(web::resource("/api/directory-targets").to(directory_targets_resp))
                .service(web::resource("/api/connections").to(connections_resp))
                .service(
                    actix_files::Files::new("/", &local_static_content).index_file("index.html"),
                )
        })
        .bind(addr)
        .unwrap()
        .start();

        system.run().unwrap();
    })
}

fn metrics() -> impl Responder {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();

    let encode_result = encoder.encode(&metric_families, &mut buffer);

    match encode_result {
        Ok(_) => {}
        Err(e) => error!("Error encoding metrics: {}", e),
    }

    String::from_utf8(buffer).unwrap()
}
