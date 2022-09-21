use log::error;

use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder, http::header::ContentType};

use prometheus::{Encoder, TextEncoder};

pub async fn start_http_server(
    addr: std::net::SocketAddr,
) -> std::io::Result<()> {
    let server = HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(web::resource("/api/metrics").to(metrics))
    })
    .bind(addr)?
    .run();

    server.await
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

    HttpResponse::Ok().content_type(ContentType::plaintext()).body(String::from_utf8(buffer).unwrap())
}
