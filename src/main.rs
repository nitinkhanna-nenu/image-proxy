// Strict clippy lints enforced on production code only. See src/lib.rs.
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::arithmetic_side_effects)]
#![deny(clippy::unreachable)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unchecked_time_subtraction)]
#![warn(clippy::todo)]
#![deny(clippy::string_slice)]
#![deny(clippy::panic_in_result_fn)]
#![deny(clippy::panic)]
#![deny(clippy::exit)]

use std::sync::Arc;

use actix_web::{App, HttpServer, middleware, web};

use image_proxy::{
    api::image::process_image_request,
    api::metrics::metrics_handler,
    api::proxy::{self, BackendClient, forward_apis},
    api::upload::store_image,
    config::EncodingConfig,
    utils::build_http_client,
};

#[actix_web::main]
#[hotpath::main]
async fn main() -> anyhow::Result<()> {
    image_proxy::logs::setup_tracing();
    let otel_metrics = image_proxy::otel::install();
    let config = Arc::new(EncodingConfig::from_env());
    let store = image_proxy::storage::ImageStore::from_env(&config.root_path)?;
    let upload_auth = image_proxy::api::upload::UploadAuth::from_env()?;
    tracing::info!(backend = %store, uploads = upload_auth.enabled(), "image store mounted");
    let (prometheus_registry, app_metrics) = image_proxy::metrics::setup_metrics();
    let hybrid_cache = image_proxy::cache::setup_cache(&config, &prometheus_registry).await?;
    let payload_limit = image_proxy::api::upload::payload_limit(config.max_source_size);
    if let Some(url) = &config.backend_url {
        proxy::validate_backend_url(url)?;
        tracing::info!(backend = %url, "api passthrough enabled");
    }
    HttpServer::new(move || {
        let http_client = build_http_client(&config.user_agent);
        let backend_client = BackendClient::new(config.backend_timeout);

        App::new()
            .app_data(web::PayloadConfig::new(payload_limit))
            .app_data(web::Data::new(http_client))
            .app_data(web::Data::new(backend_client))
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(store.clone()))
            .app_data(web::Data::new(upload_auth.clone()))
            .app_data(web::Data::new(hybrid_cache.clone()))
            .app_data(web::Data::new(prometheus_registry.clone()))
            .app_data(web::Data::new(app_metrics.clone()))
            .wrap(middleware::from_fn(forward_apis))
            .wrap(middleware::Logger::new("%a %r %s %b %D"))
            .service(metrics_handler)
            .service(store_image)
            .service(process_image_request)
    })
    .bind(std::env::var("IMAGE_PROXY_BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:8000".to_string()))?
    .run()
    .await?;

    if let Some(exporter) = otel_metrics {
        exporter.shutdown();
    }

    Ok(())
}
