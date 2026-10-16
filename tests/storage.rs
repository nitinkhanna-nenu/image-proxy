mod common;

use std::sync::Arc;

use actix_web::{http::header, test};
use common::{make_test_jpeg_bytes, test_config};
use image_proxy::api::upload::UploadAuth;
use image_proxy::config::EncodingConfig;
use image_proxy::storage::ImageStore;

macro_rules! mount_app {
    ($store:expr, $auth:expr, $config:expr) => {{
        let (cfg, client, cache, registry, app_metrics) = common::build_app_data($config);
        ::actix_web::test::init_service(
            ::actix_web::App::new()
                .app_data(::actix_web::web::PayloadConfig::new(1024 * 1024))
                .app_data(cfg)
                .app_data(client)
                .app_data(cache)
                .app_data(registry)
                .app_data(app_metrics)
                .app_data(::actix_web::web::Data::new($store))
                .app_data(::actix_web::web::Data::new($auth))
                .service(::image_proxy::api::upload::store_image)
                .service(::image_proxy::api::image::process_image_request),
        )
        .await
    }};
}

fn bearer(token: &str) -> (header::HeaderName, String) {
    (header::AUTHORIZATION, format!("Bearer {token}"))
}

#[actix_web::test]
async fn put_stores_into_the_mount_and_get_serves_it() {
    let store = ImageStore::in_memory("mounted").unwrap();
    let auth = UploadAuth::from_token(Some("upload-token".to_string())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_str().unwrap());
    let app = mount_app!(store, auth, config);
    let jpeg = make_test_jpeg_bytes(8, 8);

    let put = test::TestRequest::put()
        .uri("/albums/photo.jpg")
        .insert_header(bearer("upload-token"))
        .set_payload(jpeg.clone())
        .to_request();
    let stored = test::call_service(&app, put).await;
    assert_eq!(stored.status(), 201);
    assert_eq!(
        stored.headers().get("location").unwrap().to_str().unwrap(),
        "/albums/photo.jpg"
    );

    let get = test::TestRequest::get()
        .uri("/albums/photo.jpg")
        .to_request();
    let served = test::call_service(&app, get).await;
    assert_eq!(served.status(), 200);
    assert_eq!(
        served
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "image/jpeg"
    );
    let body = test::read_body(served).await;
    assert_eq!(body.as_ref(), jpeg.as_slice());

    // The object store load path (not the stream) still decodes.
    let resized = test::TestRequest::get()
        .uri("/albums/photo.jpg?size=4")
        .to_request();
    let transformed = test::call_service(&app, resized).await;
    assert_eq!(transformed.status(), 200);
}

#[actix_web::test]
async fn upload_requires_the_configured_token() {
    let store = ImageStore::in_memory("mounted").unwrap();
    let auth = UploadAuth::from_token(Some("upload-token".to_string())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_str().unwrap());
    let app = mount_app!(store, auth, config);
    let jpeg = make_test_jpeg_bytes(4, 4);

    let missing = test::TestRequest::put()
        .uri("/photo.jpg")
        .set_payload(jpeg.clone())
        .to_request();
    assert_eq!(test::call_service(&app, missing).await.status(), 401);

    let wrong = test::TestRequest::put()
        .uri("/photo.jpg")
        .insert_header(bearer("nope"))
        .set_payload(jpeg)
        .to_request();
    let resp = test::call_service(&app, wrong).await;
    assert_eq!(resp.status(), 401);
    assert!(
        resp.headers()
            .get("www-authenticate")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("Bearer")
    );
}

#[actix_web::test]
async fn uploads_stay_disabled_without_a_token() {
    let store = ImageStore::in_memory("").unwrap();
    let auth = UploadAuth::from_token(None).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_str().unwrap());
    let app = mount_app!(store, auth, config);

    let put = test::TestRequest::put()
        .uri("/photo.jpg")
        .insert_header(bearer("anything"))
        .set_payload(make_test_jpeg_bytes(4, 4))
        .to_request();
    assert_eq!(test::call_service(&app, put).await.status(), 403);
}

#[actix_web::test]
async fn traversal_is_stored_inside_the_mount() {
    let store = ImageStore::in_memory("mounted").unwrap();
    let auth = UploadAuth::from_token(Some("upload-token".to_string())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_str().unwrap());
    let app = mount_app!(store, auth, config);
    let jpeg = make_test_jpeg_bytes(4, 4);

    let put = test::TestRequest::put()
        .uri("/../../secret.jpg")
        .insert_header(bearer("upload-token"))
        .set_payload(jpeg.clone())
        .to_request();
    let stored = test::call_service(&app, put).await;
    assert_eq!(stored.status(), 201);
    assert_eq!(
        stored.headers().get("location").unwrap().to_str().unwrap(),
        "/secret.jpg"
    );

    let get = test::TestRequest::get().uri("/secret.jpg").to_request();
    let served = test::call_service(&app, get).await;
    assert_eq!(served.status(), 200);
    assert_eq!(test::read_body(served).await.as_ref(), jpeg.as_slice());
    assert!(dir.path().read_dir().unwrap().next().is_none());
}

#[actix_web::test]
async fn local_put_stays_under_the_root() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let store = ImageStore::local(&root);
    let auth = UploadAuth::from_token(Some("upload-token".to_string())).unwrap();
    let config = test_config(root.to_str().unwrap());
    let app = mount_app!(store, auth, config);
    let jpeg = make_test_jpeg_bytes(4, 4);

    let put = test::TestRequest::put()
        .uri("/nested/../photo.jpg")
        .insert_header(bearer("upload-token"))
        .set_payload(jpeg.clone())
        .to_request();
    // `..` is dropped, not resolved, so this stays nested inside the root.
    let stored = test::call_service(&app, put).await;
    assert_eq!(stored.status(), 201);
    assert_eq!(
        stored.headers().get("location").unwrap().to_str().unwrap(),
        "/nested/photo.jpg"
    );
    assert_eq!(std::fs::read(root.join("nested/photo.jpg")).unwrap(), jpeg);
    assert!(!root.join("photo.jpg").exists());
    assert!(!base.path().join("photo.jpg").exists());

    let get = test::TestRequest::get()
        .uri("/nested/photo.jpg")
        .to_request();
    let served = test::call_service(&app, get).await;
    assert_eq!(served.status(), 200);
    assert_eq!(test::read_body(served).await.as_ref(), jpeg.as_slice());
}

#[actix_web::test]
async fn oversized_upload_is_rejected() {
    let store = ImageStore::in_memory("mounted").unwrap();
    let auth = UploadAuth::from_token(Some("upload-token".to_string())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(EncodingConfig {
        root_path: dir.path().to_str().unwrap().to_string(),
        max_source_size: Some(8),
        ..EncodingConfig::default()
    });
    let app = mount_app!(store, auth, config);

    let put = test::TestRequest::put()
        .uri("/photo.jpg")
        .insert_header(bearer("upload-token"))
        .set_payload(vec![0_u8; 9])
        .to_request();
    assert_eq!(test::call_service(&app, put).await.status(), 413);
}

#[actix_web::test]
async fn unsupported_upload_extension_is_rejected() {
    let store = ImageStore::in_memory("mounted").unwrap();
    let auth = UploadAuth::from_token(Some("upload-token".to_string())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_str().unwrap());
    let app = mount_app!(store, auth, config);

    let put = test::TestRequest::put()
        .uri("/notes.txt")
        .insert_header(bearer("upload-token"))
        .set_payload(b"hello".to_vec())
        .to_request();
    assert_eq!(test::call_service(&app, put).await.status(), 415);
}
