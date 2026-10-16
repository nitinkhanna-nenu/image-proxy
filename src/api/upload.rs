//! `PUT /{filename}` stores an image into the mounted filesystem or S3 bucket.
//!
//! Writes stay off unless `IMAGE_PROXY_UPLOAD_TOKEN` is set. That token is an
//! application secret, separate from the AWS security token used to mount the bucket.

use std::sync::Arc;

use crate::{
    api::image::SUPPORTED_INPUT_FORMATS,
    config::EncodingConfig,
    metrics::{AppMetrics, RejectFormat, RequestPath, RequestStatus, RequestTracker},
    storage::ImageStore,
    utils::{PathValidationError, sanitize_and_validate_path},
};
use actix_web::{HttpRequest, HttpResponse, web};
use bytes::Bytes;

/// Uploads are capped here when `IMAGE_PROXY_MAX_SOURCE_SIZE` is disabled.
pub const DEFAULT_UPLOAD_LIMIT: usize = 100 * 1024 * 1024;

pub fn payload_limit(max_source_size: Option<usize>) -> usize {
    max_source_size.unwrap_or(DEFAULT_UPLOAD_LIMIT)
}

/// Bearer token required to store images. `None` disables uploads.
#[derive(Clone)]
pub struct UploadAuth {
    token: Option<String>,
}

impl std::fmt::Debug for UploadAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadAuth")
            .field("enabled", &self.enabled())
            .finish()
    }
}

pub enum AuthError {
    Disabled,
    Unauthorized,
}

impl UploadAuth {
    pub fn from_env() -> anyhow::Result<Self> {
        let Some(raw) = std::env::var("IMAGE_PROXY_UPLOAD_TOKEN").ok() else {
            return Ok(Self { token: None });
        };
        Self::from_token(Some(raw))
    }

    pub fn from_token(token: Option<String>) -> anyhow::Result<Self> {
        let Some(token) = token else {
            return Ok(Self { token: None });
        };
        let token = token.trim().to_string();
        if token.is_empty() {
            return Ok(Self { token: None });
        }
        if token.chars().any(char::is_whitespace) {
            anyhow::bail!("IMAGE_PROXY_UPLOAD_TOKEN must be a single token without whitespace");
        }
        Ok(Self { token: Some(token) })
    }

    pub fn enabled(&self) -> bool {
        self.token.is_some()
    }

    pub fn authorize(&self, req: &HttpRequest) -> Result<(), AuthError> {
        let Some(expected) = &self.token else {
            return Err(AuthError::Disabled);
        };
        match bearer_token(req) {
            Some(provided) if tokens_equal(expected, provided) => Ok(()),
            _ => Err(AuthError::Unauthorized),
        }
    }
}

fn bearer_token(req: &HttpRequest) -> Option<&str> {
    let header = req
        .headers()
        .get(actix_web::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let mut parts = header.split_whitespace();
    let scheme = parts.next()?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some(token)
}

fn tokens_equal(expected: &str, provided: &str) -> bool {
    let expected = expected.as_bytes();
    let provided = provided.as_bytes();
    let mut diff = expected.len() ^ provided.len();
    let mut expected_bytes = expected.iter();
    let mut provided_bytes = provided.iter();
    loop {
        match (expected_bytes.next(), provided_bytes.next()) {
            (Some(left), Some(right)) => diff |= (*left ^ *right) as usize,
            (Some(left), None) => diff |= *left as usize,
            (None, Some(right)) => diff |= *right as usize,
            (None, None) => break,
        }
    }
    diff == 0
}

#[actix_web::put("/{filename:.*}")]
#[tracing::instrument(skip_all, fields(filename = %filename), level = "debug")]
#[allow(clippy::too_many_arguments)]
pub async fn store_image(
    req: HttpRequest,
    filename: web::Path<String>,
    payload: web::Payload,
    config: web::Data<Arc<EncodingConfig>>,
    store: web::Data<ImageStore>,
    auth: web::Data<UploadAuth>,
    metrics: web::Data<AppMetrics>,
) -> actix_web::Result<HttpResponse> {
    let mut tracker = RequestTracker::new(metrics.get_ref().clone());

    if let Err(err) = auth.authorize(&req) {
        return Ok(match err {
            AuthError::Disabled => {
                tracker.fail(RequestPath::Store, RequestStatus::Forbidden);
                HttpResponse::Forbidden().body("Image uploads are disabled")
            }
            AuthError::Unauthorized => {
                tracker.fail(RequestPath::Store, RequestStatus::Unauthorized);
                HttpResponse::Unauthorized()
                    .insert_header(("WWW-Authenticate", "Bearer realm=\"image-proxy\""))
                    .body("Unauthorized")
            }
        });
    }

    let (sanitized_path, _, ext) = match sanitize_and_validate_path(
        &filename,
        config.strip_path.as_deref(),
        &config.root_path,
        SUPPORTED_INPUT_FORMATS,
    ) {
        Ok(result) => result,
        Err(PathValidationError::MissingExtension) => {
            tracker.reject(RejectFormat::Unknown, RequestStatus::UnsupportedMediaType);
            return Ok(
                HttpResponse::UnsupportedMediaType().body("Missing or unsupported file format")
            );
        }
        Err(PathValidationError::UnsupportedFormat(ext)) => {
            tracing::debug!(ext = %ext, "unsupported upload format");
            tracker.reject(
                RejectFormat::Unsupported,
                RequestStatus::UnsupportedMediaType,
            );
            return Ok(HttpResponse::UnsupportedMediaType()
                .body(format!("Unsupported file format: {ext}")));
        }
    };
    tracker.set_format(ext);

    let limit = payload_limit(config.max_source_size);
    let bytes = read_payload(payload, limit).await.inspect_err(|err| {
        let status = err.as_response_error().status_code();
        if status == actix_web::http::StatusCode::PAYLOAD_TOO_LARGE {
            tracker.fail(RequestPath::Store, RequestStatus::Error);
        } else {
            tracker.fail(RequestPath::Store, RequestStatus::BadRequest);
        }
    })?;

    store.put(&sanitized_path, bytes).await.map_err(|err| {
        tracing::error!("Failed to store image: {err}");
        tracker.fail(RequestPath::Store, RequestStatus::Error);
        actix_web::error::ErrorInternalServerError("Failed to store image")
    })?;

    let location = format!("/{}", url_path(&sanitized_path));
    let body = Bytes::from_static(b"stored\n");
    tracker.ok(RequestPath::Store, body.len() as u64);
    Ok(HttpResponse::Created()
        .insert_header(("Location", location))
        .content_type("text/plain; charset=utf-8")
        .body(body))
}

fn url_path(relative: &std::path::Path) -> String {
    let mut path = String::new();
    let mut first = true;
    for component in relative.components() {
        let std::path::Component::Normal(segment) = component else {
            continue;
        };
        if !first {
            path.push('/');
        }
        first = false;
        path.push_str(&segment.to_string_lossy());
    }
    path
}

async fn read_payload(mut payload: web::Payload, max: usize) -> actix_web::Result<Bytes> {
    use futures_util::StreamExt;

    let mut buf = bytes::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        let next = buf.len().saturating_add(chunk.len());
        if next > max {
            return Err(actix_web::error::ErrorPayloadTooLarge(
                "Upload exceeds the configured maximum size",
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    if buf.is_empty() {
        return Err(actix_web::error::ErrorBadRequest("Empty image body"));
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_compare_does_not_stop_at_the_first_difference() {
        assert!(tokens_equal("abc", "abc"));
        assert!(!tokens_equal("abc", "abd"));
        assert!(!tokens_equal("abc", "ab"));
        assert!(!tokens_equal("abc", "abcd"));
        assert!(!tokens_equal("", "a"));
        assert!(tokens_equal("", ""));
    }

    #[test]
    fn whitespace_upload_token_is_rejected() {
        let err = UploadAuth::from_token(Some("has space".to_string())).unwrap_err();
        assert!(err.to_string().contains("IMAGE_PROXY_UPLOAD_TOKEN"));
    }
}
