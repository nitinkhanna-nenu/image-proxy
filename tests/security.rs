#[macro_use]
mod common;

use actix_web::test;
use common::{make_test_jpeg_bytes, write_test_jpeg};
use image_proxy::config::EncodingConfig;
use std::sync::Arc;

#[actix_web::test]
async fn directory_traversal_prevented() {
    let dir = tempfile::tempdir().unwrap();
    // Create a file outside the root
    let secret = dir.path().join("secret.jpeg");
    std::fs::write(&secret, b"secret").unwrap();

    // Root is a subdir
    let root = dir.path().join("images");
    std::fs::create_dir_all(&root).unwrap();
    let config = Arc::new(EncodingConfig {
        root_path: root.to_str().unwrap().to_string(),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    // Attempt directory traversal
    let req = test::TestRequest::get().uri("/../secret.jpeg").to_request();
    let resp = test::call_service(&app, req).await;
    // Should NOT serve the file outside root - either 404 or the path is sanitized
    assert_eq!(resp.status(), 404);
}

/// A source file larger than `IMAGE_PROXY_MAX_SOURCE_SIZE` must be rejected
/// before its bytes are read into memory.
#[actix_web::test]
async fn rejects_oversized_source_file() {
    let dir = tempfile::tempdir().unwrap();
    // Write a valid JPEG, then set the source-size cap below its on-disk size.
    write_test_jpeg(dir.path(), "big.jpeg");
    let metadata = std::fs::metadata(dir.path().join("big.jpeg")).unwrap();
    let file_size = metadata.len() as usize;

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        max_source_size: Some(file_size.saturating_sub(1).max(1)),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/big.jpeg?size=4")
        .to_request();
    let resp = test::call_service(&app, req).await;
    // The size cap rejects the read before decode; the handler maps this to 413.
    assert_eq!(resp.status(), 413);
}

/// A source file at or below the cap is served normally.
#[actix_web::test]
async fn allows_source_file_at_size_cap() {
    let dir = tempfile::tempdir().unwrap();
    write_test_jpeg(dir.path(), "ok.jpeg");
    let metadata = std::fs::metadata(dir.path().join("ok.jpeg")).unwrap();
    let file_size = metadata.len() as usize;

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        max_source_size: Some(file_size),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get().uri("/ok.jpeg").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

/// An image whose declared dimensions exceed `max_dimension` must be rejected
/// during decode (decompression-bomb protection), even though the file itself
/// is small enough to pass the source-size check.
#[actix_web::test]
async fn rejects_image_exceeding_max_dimension() {
    let dir = tempfile::tempdir().unwrap();
    // 256×256 JPEG — small on disk, but larger than the configured dimension cap.
    let buf = make_test_jpeg_bytes(256, 256);
    std::fs::write(dir.path().join("big_dim.jpeg"), &buf).unwrap();

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        max_dimension: Some(128),
        // Generous source-size cap so the file passes the read stage.
        max_source_size: Some(10 * 1024 * 1024),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/big_dim.jpeg?size=64")
        .to_request();
    let resp = test::call_service(&app, req).await;
    // Decode fails due to the dimension limit; handler maps to 500.
    assert_eq!(resp.status(), 500);
}

/// A small image within all limits is served and transformed normally.
#[actix_web::test]
async fn allows_small_image_within_limits() {
    let dir = tempfile::tempdir().unwrap();
    write_test_jpeg(dir.path(), "small.jpeg");

    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        max_dimension: Some(8192),
        max_source_size: Some(100 * 1024 * 1024),
        ..EncodingConfig::default()
    });
    let app = init_test_app!(config);

    let req = test::TestRequest::get()
        .uri("/small.jpeg?size=4")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}
