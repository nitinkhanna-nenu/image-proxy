#[macro_use]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use actix_web::test;
use common::{make_test_jpeg_bytes, start_simple_http_server};
use image_proxy::config::EncodingConfig;
use tokio::io::AsyncWriteExt;

/// Helper: write nothing to the dir (so every lookup misses and hits fallback).
fn empty_root() -> tempfile::TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

#[actix_web::test]
async fn fallback_direct_stream_when_no_transform() {
    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, _handle) = start_simple_http_server(jpeg.clone(), "image/jpeg").await;

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream.clone()),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    // No query params -> direct stream of whatever the upstream returned (same format)
    let req = test::TestRequest::get()
        .uri("/valid-image.jpeg")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "image/jpeg"
    );
    let body = test::read_body(resp).await;
    assert_eq!(body.to_vec(), jpeg);
}

#[actix_web::test]
async fn fallback_with_transform_runs_pipeline() {
    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, _handle) = start_simple_http_server(jpeg, "image/jpeg").await;

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    // Different format or size forces body() + decode + pipeline
    let req = test::TestRequest::get()
        .uri("/valid-image.jpeg?format=png&size=10")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "image/png"
    );
}

#[actix_web::test]
async fn fallback_upstream_non_success_propagates_status() {
    // We simulate by using a server that returns 404 for everything.
    // The crude start_simple_http_server always returns 200. Build a tiny responder here.
    let dir = empty_root();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{}/", addr);

    let _handle = tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let _ = socket.writable().await;
            let resp = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = socket.try_write(resp);
        }
    });

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(base),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get().uri("/ghost.jpeg").to_request();
    let resp = test::call_service(&app, req).await;
    // The handler turns non-success into an InternalError carrying the upstream status.
    assert_eq!(resp.status().as_u16(), 404);
}

#[actix_web::test]
async fn fallback_connect_failure_returns_bad_gateway() {
    let dir = empty_root();
    // Point at a port that is not listening.
    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some("http://127.0.0.1:1/".to_string()),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get().uri("/nope.jpeg").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 502);
}

/// Counts GETs. The base URL has a trailing slash, matching the other fallback servers.
async fn start_counting_server(
    data: Vec<u8>,
    content_type: &'static str,
) -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_task = Arc::clone(&hits);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind counting server");
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}/");

    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(_) => break,
            };
            let mut request = String::new();
            let mut buf = [0u8; 2048];
            loop {
                if socket.readable().await.is_err() {
                    break;
                }
                match socket.try_read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        request.push_str(&String::from_utf8_lossy(&buf[..n]));
                        if request.contains("\r\n\r\n") || request.len() > 8192 {
                            break;
                        }
                    }
                    Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => break,
                }
            }
            if !request.contains("\r\n\r\n") {
                continue;
            }
            hits_task.fetch_add(1, Ordering::SeqCst);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                data.len()
            );
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(&data).await;
            let _ = socket.flush().await;
        }
    });

    (base, hits)
}

fn hits_of(hits: &AtomicUsize) -> usize {
    hits.load(Ordering::SeqCst)
}

#[actix_web::test]
async fn fallback_write_back_stores_upstream_bytes_and_skips_the_next_fetch() {
    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, hits) = start_counting_server(jpeg.clone(), "image/jpeg").await;
    let stored = dir.path().join("providers/one.jpeg");

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        write_back_on_fetch: true,
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/providers/one.jpeg")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body = test::read_body(resp).await;
    assert_eq!(body.as_ref(), jpeg.as_slice());
    assert_eq!(std::fs::read(&stored).unwrap(), jpeg);
    assert_eq!(hits_of(&hits), 1);

    let again = test::TestRequest::get()
        .uri("/providers/one.jpeg")
        .to_request();
    let resp = test::call_service(&app, again).await;
    assert_eq!(resp.status(), 200);
    let body = test::read_body(resp).await;
    assert_eq!(body.as_ref(), jpeg.as_slice());
    assert_eq!(
        hits_of(&hits),
        1,
        "the second request should be served from the store"
    );
}

#[actix_web::test]
async fn fallback_write_back_keeps_the_upstream_bytes_when_the_response_is_transformed() {
    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, hits) = start_counting_server(jpeg.clone(), "image/jpeg").await;
    let stored = dir.path().join("providers/one.jpeg");

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        write_back_on_fetch: true,
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/providers/one.jpeg?format=png&size=10")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "image/png"
    );
    let body = test::read_body(resp).await;
    assert_ne!(body.as_ref(), jpeg.as_slice());
    assert_eq!(&body[..4], b"\x89PNG");
    assert_eq!(
        std::fs::read(&stored).unwrap(),
        jpeg,
        "the store should keep the upstream original, not the png derivative"
    );
    assert_eq!(hits_of(&hits), 1);

    let again = test::TestRequest::get()
        .uri("/providers/one.jpeg?format=png&size=10")
        .to_request();
    let resp = test::call_service(&app, again).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        hits_of(&hits),
        1,
        "a later transform should load the stored original"
    );
}

#[actix_web::test]
async fn fallback_without_write_back_leaves_the_store_empty() {
    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, hits) = start_counting_server(jpeg, "image/jpeg").await;
    let stored = dir.path().join("providers/one.jpeg");

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        write_back_on_fetch: false,
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    for _ in 0..2 {
        let req = test::TestRequest::get()
            .uri("/providers/one.jpeg")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let _ = test::read_body(resp).await;
    }

    assert!(!stored.exists());
    assert_eq!(hits_of(&hits), 2);
}

#[actix_web::test]
async fn fallback_write_back_does_not_store_a_failed_fetch() {
    let dir = empty_root();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}/");
    let _handle = tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(_) => break,
            };
            let _ = socket.writable().await;
            let resp = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = socket.try_write(resp);
        }
    });

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(base),
        write_back_on_fetch: true,
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get().uri("/ghost.jpeg").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status().as_u16(), 404);
    assert!(!dir.path().join("ghost.jpeg").exists());
}

#[actix_web::test]
async fn fallback_write_back_skips_objects_over_max_source_size() {
    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, hits) = start_counting_server(jpeg, "image/jpeg").await;
    let stored = dir.path().join("wide.jpeg");

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        write_back_on_fetch: true,
        max_source_size: Some(1),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    for _ in 0..2 {
        let req = test::TestRequest::get().uri("/wide.jpeg").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
        let _ = test::read_body(resp).await;
    }

    assert!(!stored.exists());
    assert_eq!(hits_of(&hits), 2);
}

#[actix_web::test]
async fn fallback_write_back_does_not_store_when_decode_fails() {
    let dir = empty_root();
    let (upstream, hits) = start_counting_server(b"not-a-jpeg".to_vec(), "image/jpeg").await;
    let stored = dir.path().join("bad.jpeg");

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        write_back_on_fetch: true,
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/bad.jpeg?format=png")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 500);
    assert!(!stored.exists());
    assert_eq!(hits_of(&hits), 1);
}

#[actix_web::test]
async fn fallback_write_back_serves_when_the_store_rejects_the_write() {
    use std::os::unix::fs::PermissionsExt;

    let dir = empty_root();
    let jpeg = make_test_jpeg_bytes(32, 20);
    let (upstream, _hits) = start_counting_server(jpeg.clone(), "image/jpeg").await;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        fallback_image_url: Some(upstream),
        write_back_on_fetch: true,
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/valid-image.jpeg")
        .to_request();
    let resp = test::call_service(&app, req).await;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    assert_eq!(resp.status(), 200);
    let body = test::read_body(resp).await;
    assert_eq!(body.as_ref(), jpeg.as_slice());
    assert!(!dir.path().join("valid-image.jpeg").exists());
}
