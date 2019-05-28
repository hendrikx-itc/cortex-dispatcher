use std::thread;

use actix_rt;
use actix_web::{web, App, HttpServer, middleware, Responder, HttpRequest, HttpResponse};
use actix_http::{Response, Error};
use actix_files;

use futures::future::{ok, FutureResult};

use prometheus::{TextEncoder, Encoder};

use crate::base_types::CortexConfig;
use crate::settings;


struct SftpResponse {
    pub sftp_sources: std::sync::Arc<std::sync::Mutex<Vec<settings::SftpSource>>>
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

pub fn start_http_server(addr: std::net::SocketAddr, static_content: std::path::PathBuf, cortex_config: CortexConfig) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let system = actix_rt::System::new("http_server");

        let local_static_content = static_content.clone();

        info!("Serving static content from {}", static_content.to_str().unwrap().to_string());

        let c = cortex_config.sftp_sources.clone();

        HttpServer::new(move || {
            let cc = c.clone();
            let res_resp = move || -> SftpResponse {
                SftpResponse {
                    sftp_sources: cc.clone()
                } 
            };

            App::new()
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("Access-Control-Allow-Origin", "*"))
                .service(
                    web::resource("/api/metrics").to(metrics)
                )
                .service(
                    web::resource("/api/sftp-sources").to(res_resp)
                )
                .service(
                    actix_files::Files::new("/", &local_static_content).index_file("index.html")
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
