mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use actix_web::{http::header, test};
use common::{make_test_jpeg_bytes, test_config};
use image_proxy::api::upload::UploadAuth;
use image_proxy::config::EncodingConfig;
use image_proxy::storage::ImageStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

macro_rules! backend_app {
    ($config:expr) => {{
        let config = $config;
        let (cfg, client, cache, registry, app_metrics) = common::build_app_data(config.clone());
        ::actix_web::test::init_service(
            ::actix_web::App::new()
                .app_data(cfg)
                .app_data(client)
                .app_data(cache)
                .app_data(registry)
                .app_data(app_metrics)
                .app_data(::actix_web::web::Data::new(
                    image_proxy::api::proxy::BackendClient::new(config.backend_timeout),
                ))
                .app_data(::actix_web::web::Data::new(ImageStore::local(
                    config.root_path.clone(),
                )))
                .app_data(::actix_web::web::Data::new(
                    UploadAuth::from_token(None).unwrap(),
                ))
                .wrap(::actix_web::middleware::from_fn(
                    image_proxy::api::proxy::forward_apis,
                ))
                .service(image_proxy::api::metrics::metrics_handler)
                .service(image_proxy::api::upload::store_image)
                .service(image_proxy::api::image::process_image_request),
        )
        .await
    }};
}

struct Echo {
    base: String,
    hits: Arc<AtomicUsize>,
}

async fn start_echo() -> Echo {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_task = Arc::clone(&hits);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            hits_task.fetch_add(1, Ordering::SeqCst);
            let request = match read_http_request(&mut socket).await {
                Some(request) => request,
                None => break,
            };
            let (status, location, body) = if request.target.starts_with("/redirect") {
                ("302 Found", Some("http://127.0.0.1:1/followed"), Vec::new())
            } else {
                ("201 Created", None, request.body)
            };
            let location = location
                .map(|value| format!("Location: {value}\r\n"))
                .unwrap_or_default();
            let header = format!(
                "HTTP/1.1 {status}\r\n\
                 Content-Type: text/plain\r\n\
                 X-Echo-Method: {}\r\n\
                 X-Echo-Target: {}\r\n\
                 X-Echo-Authorization: {}\r\n\
                 X-Echo-Forwarded-Host: {}\r\n\
                 {location}\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n",
                request.method,
                request.target,
                request.authorization,
                request.forwarded_host,
                body.len(),
            );
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
    });
    Echo {
        base: format!("http://{addr}"),
        hits,
    }
}

struct CapturedRequest {
    method: String,
    target: String,
    authorization: String,
    forwarded_host: String,
    body: Vec<u8>,
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Option<CapturedRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let header_end = loop {
        let n = socket.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(end) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    };
    let header = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let mut authorization = String::new();
    let mut forwarded_host = String::new();
    let mut content_length = 0usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("authorization") {
            authorization = value.to_string();
        } else if name.eq_ignore_ascii_case("x-forwarded-host") {
            forwarded_host = value.to_string();
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().unwrap_or(0);
        }
    }
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = socket.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Some(CapturedRequest {
        method,
        target,
        authorization,
        forwarded_host,
        body,
    })
}

fn config_with_backend(root: &str, backend: &str) -> Arc<EncodingConfig> {
    Arc::new(EncodingConfig {
        root_path: root.to_string(),
        backend_url: Some(backend.to_string()),
        ..EncodingConfig::default()
    })
}

#[actix_web::test]
async fn api_requests_keep_method_path_query_and_body() {
    let echo = start_echo().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_with_backend(dir.path().to_str().unwrap(), &echo.base);
    let app = backend_app!(config);
    let payload = br#"{"name":"Ada"}"#.to_vec();

    let req = test::TestRequest::post()
        .uri("/api/items?x=1")
        .insert_header((header::AUTHORIZATION, "Bearer secret"))
        .insert_header((header::CONTENT_TYPE, "application/json"))
        .set_payload(payload.clone())
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    assert_eq!(
        resp.headers()
            .get("x-echo-method")
            .unwrap()
            .to_str()
            .unwrap(),
        "POST"
    );
    assert_eq!(
        resp.headers()
            .get("x-echo-target")
            .unwrap()
            .to_str()
            .unwrap(),
        "/api/items?x=1"
    );
    assert_eq!(
        resp.headers()
            .get("x-echo-authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer secret"
    );
    assert!(
        !resp
            .headers()
            .get("x-echo-forwarded-host")
            .unwrap()
            .to_str()
            .unwrap()
            .is_empty()
    );
    assert_eq!(test::read_body(resp).await.as_ref(), payload.as_slice());

    let notes = test::TestRequest::get().uri("/notes.txt").to_request();
    let notes = test::call_service(&app, notes).await;
    assert_eq!(notes.status(), 201);
    assert_eq!(
        notes
            .headers()
            .get("x-echo-target")
            .unwrap()
            .to_str()
            .unwrap(),
        "/notes.txt"
    );
}

#[actix_web::test]
async fn images_and_metrics_stay_on_the_proxy() {
    let echo = start_echo().await;
    let dir = tempfile::tempdir().unwrap();
    let jpeg = make_test_jpeg_bytes(4, 4);
    std::fs::write(dir.path().join("photo.jpg"), &jpeg).unwrap();
    let config = config_with_backend(dir.path().to_str().unwrap(), &echo.base);
    let app = backend_app!(config);

    let get = test::TestRequest::get().uri("/photo.jpg").to_request();
    let served = test::call_service(&app, get).await;
    assert_eq!(served.status(), 200);
    assert_eq!(test::read_body(served).await.as_ref(), jpeg.as_slice());

    let put = test::TestRequest::put()
        .uri("/photo.jpg")
        .set_payload(jpeg)
        .to_request();
    assert_eq!(test::call_service(&app, put).await.status(), 403);

    let metrics = test::TestRequest::get().uri("/metrics").to_request();
    assert_eq!(test::call_service(&app, metrics).await.status(), 200);
    assert_eq!(echo.hits.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn strip_path_is_removed_before_forwarding() {
    let echo = start_echo().await;
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        backend_url: Some(echo.base.clone()),
        strip_path: Some("/edge".to_string()),
        ..EncodingConfig::default()
    });
    let app = backend_app!(config);
    let req = test::TestRequest::get()
        .uri("/edge/api/items?x=1")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    assert_eq!(
        resp.headers()
            .get("x-echo-target")
            .unwrap()
            .to_str()
            .unwrap(),
        "/api/items?x=1"
    );
}

#[actix_web::test]
async fn redirects_are_returned_to_the_caller() {
    let echo = start_echo().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_with_backend(dir.path().to_str().unwrap(), &echo.base);
    let app = backend_app!(config);
    let req = test::TestRequest::get().uri("/redirect").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 302);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "http://127.0.0.1:1/followed"
    );
    assert_eq!(echo.hits.load(Ordering::SeqCst), 1);
}

#[actix_web::test]
async fn unreachable_backend_is_a_bad_gateway() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        backend_url: Some("http://127.0.0.1:1".to_string()),
        backend_timeout: std::time::Duration::from_secs(1),
        ..EncodingConfig::default()
    });
    let app = backend_app!(config);
    let req = test::TestRequest::get().uri("/api/health").to_request();
    assert_eq!(test::call_service(&app, req).await.status(), 502);
}

#[actix_web::test]
async fn unset_backend_keeps_the_unsupported_path_response() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_str().unwrap());
    let app = backend_app!(config);
    let req = test::TestRequest::get().uri("/api/health").to_request();
    assert_eq!(test::call_service(&app, req).await.status(), 415);
}
