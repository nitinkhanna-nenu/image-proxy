//! Forwards API requests that are not images to a configured HTTP backend.
//!
//! Image paths stay on this service (`GET` serve, `PUT` upload). `/metrics` stays
//! local. Everything else is proxied when `IMAGE_PROXY_BACKEND_URL` is set.

use std::sync::Arc;
use std::time::Duration;

use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::Uri;
use actix_web::http::header::{CONTENT_LENGTH, HOST, HeaderMap, HeaderName};
use actix_web::middleware::Next;
use actix_web::{HttpResponse, web};
use bytes::BytesMut;
use futures_util::StreamExt;

use crate::api::image::SUPPORTED_INPUT_FORMATS;
use crate::api::upload::payload_limit;
use crate::config::EncodingConfig;
use crate::metrics::{AppMetrics, RequestPath, RequestStatus, RequestTracker};

/// Outbound client for API passthrough. Redirects are not followed.
#[derive(Clone)]
pub struct BackendClient(awc::Client);

impl BackendClient {
    pub fn new(timeout: Duration) -> Self {
        Self(
            awc::ClientBuilder::new()
                .disable_redirects()
                .timeout(timeout)
                .finish(),
        )
    }
}

/// Actix middleware. Installed around the app so non-image methods and paths
/// never reach the image routes.
pub async fn forward_apis(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    if !should_forward(&req) {
        return Ok(next.call(req).await?.map_into_boxed_body());
    }
    dispatch(req).await
}

fn should_forward(req: &ServiceRequest) -> bool {
    let Some(config) = req.app_data::<web::Data<Arc<EncodingConfig>>>() else {
        return false;
    };
    if config.backend_url.is_none() {
        return false;
    }
    let path = req.path();
    path != "/metrics" && !is_image_path(path)
}

pub(crate) fn is_image_path(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    let path = path.trim_end_matches('/');
    let Some(name) = path.rsplit('/').next() else {
        return false;
    };
    let Some((_, ext)) = name.rsplit_once('.') else {
        return false;
    };
    if ext.is_empty() {
        return false;
    }
    SUPPORTED_INPUT_FORMATS
        .iter()
        .any(|supported| supported.eq_ignore_ascii_case(ext))
}

async fn dispatch(req: ServiceRequest) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    let config = req
        .app_data::<web::Data<Arc<EncodingConfig>>>()
        .map(|data| data.get_ref().clone());
    let client = req
        .app_data::<web::Data<BackendClient>>()
        .map(|data| data.get_ref().clone());
    let metrics = req
        .app_data::<web::Data<AppMetrics>>()
        .map(|data| data.get_ref().clone());
    let mut tracker = metrics.map(RequestTracker::new);

    let Some(config) = config else {
        fail(&mut tracker, RequestStatus::Error);
        return plain_req(
            req,
            HttpResponse::InternalServerError().body("Backend is not configured"),
        );
    };
    let Some(base) = config.backend_url.clone() else {
        fail(&mut tracker, RequestStatus::Error);
        return plain_req(
            req,
            HttpResponse::InternalServerError().body("Backend is not configured"),
        );
    };
    let Some(client) = client else {
        fail(&mut tracker, RequestStatus::Error);
        tracing::error!("backend passthrough is enabled but no HTTP client is configured");
        return plain_req(
            req,
            HttpResponse::InternalServerError().body("Backend client is not configured"),
        );
    };

    let forwarded = forwarded_headers(&req);
    let (http_req, mut payload) = req.into_parts();
    let path_and_query = forward_path(http_req.uri(), config.strip_path.as_deref());
    let target = match target_url(&base, &path_and_query) {
        Ok(target) => target,
        Err(err) => {
            tracing::debug!("refusing api passthrough: {err}");
            fail(&mut tracker, RequestStatus::BadRequest);
            return plain(
                http_req,
                HttpResponse::BadRequest().body("Refusing to forward request"),
            );
        }
    };

    tracing::debug!(method = %http_req.method(), path = %path_and_query, "forwarding api request");

    let limit = payload_limit(config.max_source_size);
    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(err) => {
                tracing::debug!("failed to read request body: {err}");
                let too_large = matches!(err, actix_web::error::PayloadError::Overflow);
                fail(
                    &mut tracker,
                    if too_large {
                        RequestStatus::Error
                    } else {
                        RequestStatus::BadRequest
                    },
                );
                return plain(
                    http_req,
                    if too_large {
                        HttpResponse::PayloadTooLarge().body("Request body too large")
                    } else {
                        HttpResponse::BadRequest().body("Failed to read request body")
                    },
                );
            }
        };
        if body.len().saturating_add(chunk.len()) > limit {
            fail(&mut tracker, RequestStatus::Error);
            return plain(
                http_req,
                HttpResponse::PayloadTooLarge().body("Request body too large"),
            );
        }
        body.extend_from_slice(&chunk);
    }

    let mut outbound = client
        .0
        .request(http_req.method().clone(), target)
        .timeout(config.backend_timeout)
        .no_decompress();
    outbound = copy_request_headers(outbound, http_req.headers());
    outbound = apply_forwarded(outbound, http_req.headers(), &forwarded);

    let upstream = match outbound.send_body(body.freeze()).await {
        Ok(response) => response,
        Err(err) => {
            tracing::debug!("backend request failed: {err}");
            fail(&mut tracker, RequestStatus::BadGateway);
            return plain(
                http_req,
                HttpResponse::BadGateway().body("Backend unavailable"),
            );
        }
    };

    let declared = content_length(upstream.headers());
    if let Some(tracker) = tracker.as_mut() {
        tracker.ok(RequestPath::Backend, declared);
    }

    let mut builder = HttpResponse::build(upstream.status());
    for (name, value) in upstream.headers() {
        if forward_response_header(name) {
            builder.append_header((name.clone(), value.clone()));
        }
    }
    let response = builder.streaming(upstream);
    Ok(ServiceResponse::new(http_req, response).map_into_boxed_body())
}

fn plain(
    req: actix_web::HttpRequest,
    response: HttpResponse,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    Ok(ServiceResponse::new(req, response).map_into_boxed_body())
}

fn plain_req(
    req: ServiceRequest,
    response: HttpResponse,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    let (http_req, _) = req.into_parts();
    plain(http_req, response)
}

fn fail(tracker: &mut Option<RequestTracker>, status: RequestStatus) {
    if let Some(tracker) = tracker.as_mut() {
        tracker.fail(RequestPath::Backend, status);
    }
}

struct Forwarded {
    host: String,
    proto: String,
    peer: String,
}

fn forwarded_headers(req: &ServiceRequest) -> Forwarded {
    let info = req.connection_info();
    Forwarded {
        host: info.host().to_string(),
        proto: info.scheme().to_string(),
        peer: info.peer_addr().unwrap_or("").to_string(),
    }
}

fn copy_request_headers(
    mut outbound: awc::ClientRequest,
    headers: &HeaderMap,
) -> awc::ClientRequest {
    let listed = connection_listed(headers);
    for (name, value) in headers {
        if skip_request_header(name, &listed) {
            continue;
        }
        outbound = outbound.append_header((name.clone(), value.clone()));
    }
    outbound
}

fn apply_forwarded(
    mut outbound: awc::ClientRequest,
    headers: &HeaderMap,
    forwarded: &Forwarded,
) -> awc::ClientRequest {
    if !headers.contains_key("x-forwarded-host") && !forwarded.host.is_empty() {
        outbound = outbound.append_header(("x-forwarded-host", forwarded.host.clone()));
    }
    if !headers.contains_key("x-forwarded-proto") && !forwarded.proto.is_empty() {
        outbound = outbound.append_header(("x-forwarded-proto", forwarded.proto.clone()));
    }
    if !headers.contains_key("x-forwarded-for") && !forwarded.peer.is_empty() {
        outbound = outbound.append_header(("x-forwarded-for", forwarded.peer.clone()));
    }
    outbound
}

fn skip_request_header(name: &HeaderName, connection_listed: &[HeaderName]) -> bool {
    name == CONTENT_LENGTH
        || name == HOST
        || is_hop_by_hop(name)
        || connection_listed.iter().any(|listed| listed == name)
}

fn forward_response_header(name: &HeaderName) -> bool {
    name != CONTENT_LENGTH && !is_hop_by_hop(name)
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn connection_listed(headers: &HeaderMap) -> Vec<HeaderName> {
    let mut names = Vec::new();
    for value in headers.get_all("connection") {
        let Ok(text) = value.to_str() else {
            continue;
        };
        for part in text.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Ok(name) = HeaderName::from_bytes(part.as_bytes()) {
                names.push(name);
            }
        }
    }
    names
}

fn content_length(headers: &HeaderMap) -> u64 {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn forward_path(uri: &Uri, strip: Option<&str>) -> String {
    let path = stripped_path(uri.path(), strip);
    match uri.query() {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        _ => path,
    }
}

fn stripped_path(path: &str, strip: Option<&str>) -> String {
    let Some(prefix) = strip
        .map(str::trim)
        .map(|value| value.trim_end_matches('/'))
        .filter(|value| !value.is_empty())
    else {
        return if path.is_empty() {
            "/".to_string()
        } else {
            path.to_string()
        };
    };
    if path == prefix {
        return "/".to_string();
    }
    if let Some(rest) = path.strip_prefix(prefix)
        && rest.starts_with('/')
    {
        return rest.to_string();
    }
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
}

struct BaseUrl {
    origin: String,
    prefix: String,
}

pub fn validate_backend_url(url: &str) -> anyhow::Result<()> {
    parse_base(url).map(|_| ())
}

fn parse_base(url: &str) -> anyhow::Result<BaseUrl> {
    if url.contains('#')
        || url
            .split("://")
            .nth(1)
            .is_some_and(|rest| rest.contains('?'))
    {
        anyhow::bail!("IMAGE_PROXY_BACKEND_URL must not include a query or fragment");
    }
    let uri: Uri = url
        .parse()
        .map_err(|err| anyhow::anyhow!("IMAGE_PROXY_BACKEND_URL is not a valid URL: {err}"))?;
    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        _ => anyhow::bail!("IMAGE_PROXY_BACKEND_URL must start with http:// or https://"),
    }
    let Some(authority) = uri.authority() else {
        anyhow::bail!("IMAGE_PROXY_BACKEND_URL must include a host");
    };
    if authority.as_str().contains('@') {
        anyhow::bail!("IMAGE_PROXY_BACKEND_URL must not include user info");
    }
    if uri.host().is_none() {
        anyhow::bail!("IMAGE_PROXY_BACKEND_URL must include a host");
    }
    let prefix = uri.path().trim_end_matches('/').to_string();
    if path_escapes(&prefix) {
        anyhow::bail!("IMAGE_PROXY_BACKEND_URL path must not contain '..'");
    }
    Ok(BaseUrl {
        origin: format!("{}://{authority}", uri.scheme_str().unwrap_or("http")),
        prefix,
    })
}

pub(crate) fn target_url(base: &str, path_and_query: &str) -> anyhow::Result<String> {
    let base = parse_base(base)?;
    let (path, query) = match path_and_query.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path_and_query, None),
    };
    if !path.starts_with('/') || path.starts_with("//") || path.contains('\\') || path_escapes(path)
    {
        anyhow::bail!("request path cannot be forwarded");
    }
    let mut target = format!("{}{}{path}", base.origin, base.prefix);
    if let Some(query) = query {
        target.push('?');
        target.push_str(query);
    }
    let target_uri: Uri = target
        .parse()
        .map_err(|_| anyhow::anyhow!("forwarded URL is not valid"))?;
    let base_uri: Uri = format!("{}{}", base.origin, base.prefix)
        .parse()
        .map_err(|_| anyhow::anyhow!("backend URL is not valid"))?;
    if target_uri.scheme_str() != base_uri.scheme_str()
        || target_uri.authority() != base_uri.authority()
    {
        anyhow::bail!("refusing to forward outside the configured backend");
    }
    Ok(target)
}

fn path_escapes(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment == ".." || segment.eq_ignore_ascii_case("%2e%2e"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_extensions_stay_on_the_proxy() {
        assert!(is_image_path("/photos/cat.JPG"));
        assert!(is_image_path("/a/b/file.tar.jpg"));
        assert!(!is_image_path("/api/users"));
        assert!(!is_image_path("/notes.txt"));
        assert!(!is_image_path("/metrics"));
    }

    #[test]
    fn target_keeps_the_backend_origin_and_prefix() {
        let url = target_url("https://api.internal/svc/", "/api/items?x=1").unwrap();
        assert_eq!(url, "https://api.internal/svc/api/items?x=1");
    }

    #[test]
    fn target_rejects_parent_segments_and_scheme_relative_paths() {
        assert!(target_url("https://api.internal/svc", "/api/../admin").is_err());
        assert!(target_url("https://api.internal", "//evil.test/x").is_err());
        assert!(target_url("https://api.internal", "/api/%2e%2e/secret").is_err());
    }

    #[test]
    fn backend_url_rejects_userinfo_and_query() {
        assert!(validate_backend_url("https://user:secret@api.internal").is_err());
        assert!(validate_backend_url("https://api.internal/path?token=1").is_err());
        assert!(validate_backend_url("ftp://api.internal").is_err());
        assert!(validate_backend_url("https://api.internal/v1").is_ok());
    }

    #[test]
    fn strip_path_is_removed_before_joining() {
        let path = forward_path(&"/edge/api/items?x=1".parse().unwrap(), Some("/edge"));
        assert_eq!(path, "/api/items?x=1");
        let url = target_url("http://127.0.0.1:9", &path).unwrap();
        assert_eq!(url, "http://127.0.0.1:9/api/items?x=1");
    }
}
