//! OpenTelemetry metrics for image requests.
//!
//! One call records every instrument for a request, from the same bounded
//! labels. `format`, `status`, `path`, and `step` are closed sets; a value
//! outside the set is `unknown`. `error_type` is added only when the status is
//! not `ok`, and it has three values: who has to act. Duration and byte counts
//! are measurements, not labels.
//!
//! Export is off until `OTEL_EXPORTER_OTLP_ENDPOINT` or
//! `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is set. `/metrics` is unchanged.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::{KeyValue, metrics::MeterProvider as _};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};

use crate::metrics::{PipelineStep, RequestPath, RequestStatus};

const METER_NAME: &str = "image-proxy";

const REQUEST_BUCKETS: [f64; 15] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

const STEP_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

const FORMATS: &[&str] = &[
    "avif",
    "jpeg",
    "jpg",
    "png",
    "webp",
    "svg",
    "ico",
    "gif",
    "bmp",
    "tiff",
    "jxl",
    "unknown",
    "unsupported",
];

static METRICS: OnceLock<OtelMetrics> = OnceLock::new();

struct OtelMetrics {
    request_count: opentelemetry::metrics::Counter<u64>,
    request_duration: opentelemetry::metrics::Histogram<f64>,
    response_bytes: opentelemetry::metrics::Counter<u64>,
    in_flight: opentelemetry::metrics::UpDownCounter<i64>,
    step_duration: opentelemetry::metrics::Histogram<f64>,
}

impl OtelMetrics {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            request_count: meter
                .u64_counter("image.request.count")
                .with_description("Image requests, one per finished handler")
                .with_unit("{request}")
                .build(),
            request_duration: meter
                .f64_histogram("image.request.duration")
                .with_description(
                    "Handler wall time until the response is returned. Streaming paths exclude body transfer",
                )
                .with_unit("s")
                .with_boundaries(REQUEST_BUCKETS.to_vec())
                .build(),
            response_bytes: meter
                .u64_counter("image.response.bytes")
                .with_description(
                    "Bytes in successful responses. Streaming pass-through uses the declared size",
                )
                .with_unit("By")
                .build(),
            in_flight: meter
                .i64_up_down_counter("image.request.in_flight")
                .with_description("Image requests currently inside a handler")
                .with_unit("{request}")
                .build(),
            step_duration: meter
                .f64_histogram("image.pipeline.step.duration")
                .with_description("Time spent inside one decode, resize, grayscale, or encode step")
                .with_unit("s")
                .with_boundaries(STEP_BUCKETS.to_vec())
                .build(),
        }
    }

    fn record_request(
        &self,
        format: &str,
        status: RequestStatus,
        path: RequestPath,
        bytes: u64,
        duration: Duration,
    ) {
        let format = bound_format(format);
        let mut labels = vec![
            KeyValue::new("format", format),
            KeyValue::new("status", status.as_str()),
            KeyValue::new("path", path.as_str()),
        ];
        if let Some(error_type) = error_type(status) {
            labels.push(KeyValue::new("error_type", error_type));
        }
        self.request_count.add(1, &labels);
        self.request_duration
            .record(duration.as_secs_f64(), &labels);
        if bytes > 0 && status == RequestStatus::Ok {
            self.response_bytes.add(
                bytes,
                &[
                    KeyValue::new("format", format),
                    KeyValue::new("path", path.as_str()),
                ],
            );
        }
    }

    fn add_in_flight(&self, delta: i64) {
        self.in_flight.add(delta, &[]);
    }

    fn record_step(&self, step: PipelineStep, format: &str, duration: Duration) {
        self.step_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("step", step.as_str()),
                KeyValue::new("format", bound_format(format)),
            ],
        );
    }
}

/// Starts OTLP metric export when an endpoint is configured.
///
/// The returned provider must stay alive for the process and be shut down on
/// the way out so the last batch is flushed. `None` means export is off and
/// Prometheus on `/metrics` is the only metrics output.
pub(super) fn install() -> Option<super::Exporter> {
    let choice = choose_from_env();
    match choice {
        ExportChoice::Off => {
            tracing::debug!("otel metrics export is off");
            None
        }
        ExportChoice::Rejected(message) => {
            tracing::warn!("{message}");
            None
        }
        ExportChoice::On { endpoint } => match start(&endpoint) {
            Ok(provider) => {
                tracing::info!(endpoint = %redact_endpoint(&endpoint), "otel metrics export enabled");
                Some(super::Exporter::new(provider))
            }
            Err(err) => {
                tracing::warn!(error = %err, "otel metrics export disabled");
                None
            }
        },
    }
}

pub(crate) fn record_request(
    format: &str,
    status: RequestStatus,
    path: RequestPath,
    bytes: u64,
    duration: Duration,
) {
    if let Some(metrics) = METRICS.get() {
        metrics.record_request(format, status, path, bytes, duration);
    }
}

pub(crate) fn add_in_flight(delta: i64) {
    if let Some(metrics) = METRICS.get() {
        metrics.add_in_flight(delta);
    }
}

pub(crate) fn record_step(step: PipelineStep, format: &str, duration: Duration) {
    if let Some(metrics) = METRICS.get() {
        metrics.record_step(step, format, duration);
    }
}

fn start(endpoint: &str) -> anyhow::Result<SdkMeterProvider> {
    let exporter = build_exporter(endpoint)?;
    let provider = SdkMeterProvider::builder()
        .with_resource(resource())
        .with_periodic_exporter(exporter)
        .build();
    let metrics = OtelMetrics::new(&provider.meter(METER_NAME));
    if METRICS.set(metrics).is_err() {
        tracing::warn!("otel metrics were already installed");
    }
    Ok(provider)
}

fn build_exporter(endpoint: &str) -> anyhow::Result<opentelemetry_otlp::MetricExporter> {
    use opentelemetry_otlp::WithExportConfig;
    opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .map_err(|err| anyhow::anyhow!("otel metrics exporter: {err}"))
}

fn resource() -> Resource {
    let mut builder = Resource::builder();
    if env_nonempty("OTEL_SERVICE_NAME").is_none() {
        builder = builder.with_service_name("image-proxy");
    }
    builder = builder.with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")));
    if let Some(environment) = env_nonempty("DEPLOYMENT_ENVIRONMENT") {
        builder = builder.with_attribute(KeyValue::new("deployment.environment.name", environment));
    }
    builder.build()
}

enum ExportChoice {
    Off,
    Rejected(String),
    On { endpoint: String },
}

fn choose_from_env() -> ExportChoice {
    choose(
        env_nonempty("OTEL_SDK_DISABLED").as_deref(),
        env_nonempty("OTEL_METRICS_EXPORTER").as_deref(),
        env_nonempty("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
            .or_else(|| env_nonempty("OTEL_EXPORTER_OTLP_PROTOCOL"))
            .as_deref(),
        env_nonempty("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").as_deref(),
        env_nonempty("OTEL_EXPORTER_OTLP_ENDPOINT").as_deref(),
    )
}

fn choose(
    disabled: Option<&str>,
    exporter: Option<&str>,
    protocol: Option<&str>,
    metrics_endpoint: Option<&str>,
    base_endpoint: Option<&str>,
) -> ExportChoice {
    if disabled.is_some_and(|value| value.eq_ignore_ascii_case("true")) {
        return ExportChoice::Off;
    }
    match exporter.map(|value| value.to_ascii_lowercase()).as_deref() {
        None | Some("otlp") => {}
        Some("none") => return ExportChoice::Off,
        Some(other) => {
            return ExportChoice::Rejected(format!(
                "OTEL_METRICS_EXPORTER={other} is not supported. Leave it unset or set otlp. Prometheus stays on /metrics"
            ));
        }
    }
    match protocol {
        None | Some("http/protobuf") => {}
        Some(other) => {
            return ExportChoice::Rejected(format!(
                "otel metrics export speaks http/protobuf. OTEL_EXPORTER_OTLP_PROTOCOL={other} is not supported"
            ));
        }
    }
    match resolve_endpoint(metrics_endpoint, base_endpoint) {
        Some(endpoint) => ExportChoice::On { endpoint },
        None => ExportChoice::Off,
    }
}

/// Signal endpoint wins. A base URL gets `/v1/metrics` appended, which is what
/// the OTLP HTTP exporter does when the base env var is set.
fn resolve_endpoint(metrics_endpoint: Option<&str>, base_endpoint: Option<&str>) -> Option<String> {
    if let Some(endpoint) = metrics_endpoint
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(endpoint.to_string());
    }
    let base = base_endpoint
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let base = base.trim_end_matches('/');
    Some(format!("{base}/v1/metrics"))
}

fn redact_endpoint(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((_, host)) = rest.split_once('@') else {
        return url.to_string();
    };
    format!("{scheme}://***@{host}")
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn bound_format(raw: &str) -> &'static str {
    FORMATS
        .iter()
        .copied()
        .find(|format| *format == raw)
        .unwrap_or("unknown")
}

/// Who has to act. Absent on `ok`.
fn error_type(status: RequestStatus) -> Option<&'static str> {
    match status {
        RequestStatus::Ok => None,
        RequestStatus::NotFound
        | RequestStatus::UnsupportedMediaType
        | RequestStatus::Unauthorized
        | RequestStatus::Forbidden
        | RequestStatus::BadRequest => Some("client_error"),
        RequestStatus::BadGateway => Some("site_error"),
        RequestStatus::Error => Some("internal_error"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    use super::{
        ExportChoice, OtelMetrics, REQUEST_BUCKETS, choose, redact_endpoint, resolve_endpoint,
    };
    use crate::metrics::{PipelineStep, RequestPath, RequestStatus};

    fn labels<'a>(attrs: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
        attrs
            .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().into_owned()))
            .collect()
    }

    struct Harness {
        exporter: InMemoryMetricExporter,
        provider: SdkMeterProvider,
        metrics: OtelMetrics,
    }

    impl Harness {
        fn new() -> Self {
            let exporter = InMemoryMetricExporter::default();
            let reader = PeriodicReader::builder(exporter.clone()).build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            let metrics = OtelMetrics::new(&provider.meter(super::METER_NAME));
            Self {
                exporter,
                provider,
                metrics,
            }
        }

        fn collect(&self) -> Vec<opentelemetry_sdk::metrics::data::ResourceMetrics> {
            self.provider.force_flush().unwrap();
            self.exporter.get_finished_metrics().unwrap()
        }
    }

    fn metric<'a>(
        batches: &'a [opentelemetry_sdk::metrics::data::ResourceMetrics],
        name: &str,
    ) -> &'a Metric {
        batches
            .iter()
            .flat_map(|batch| batch.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .find(|metric| metric.name() == name)
            .unwrap()
    }

    fn u64_points(metric: &Metric) -> Vec<(BTreeMap<String, String>, u64)> {
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
            return Vec::new();
        };
        sum.data_points()
            .map(|point| (labels(point.attributes()), point.value()))
            .collect()
    }

    fn i64_points(metric: &Metric) -> Vec<(BTreeMap<String, String>, i64)> {
        let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
            return Vec::new();
        };
        sum.data_points()
            .map(|point| (labels(point.attributes()), point.value()))
            .collect()
    }

    type HistogramPoint = (BTreeMap<String, String>, u64, f64, Vec<f64>);

    fn histogram_points(metric: &Metric) -> Vec<HistogramPoint> {
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
            return Vec::new();
        };
        histogram
            .data_points()
            .map(|point| {
                (
                    labels(point.attributes()),
                    point.count(),
                    point.sum(),
                    point.bounds().collect(),
                )
            })
            .collect()
    }

    #[test]
    fn one_ok_request_records_count_duration_and_bytes_with_the_same_labels() {
        let harness = Harness::new();
        harness.metrics.record_request(
            "jpeg",
            RequestStatus::Ok,
            RequestPath::PassThrough,
            120,
            Duration::from_millis(20),
        );
        let batches = harness.collect();

        let count = u64_points(metric(&batches, "image.request.count"));
        assert_eq!(count.len(), 1);
        assert_eq!(count[0].1, 1);
        assert_eq!(
            count[0].0,
            BTreeMap::from([
                ("format".to_string(), "jpeg".to_string()),
                ("path".to_string(), "pass_through".to_string()),
                ("status".to_string(), "ok".to_string()),
            ])
        );

        let duration = histogram_points(metric(&batches, "image.request.duration"));
        assert_eq!(duration.len(), 1);
        assert_eq!(duration[0].0, count[0].0);
        assert_eq!(duration[0].1, 1);
        assert!((duration[0].2 - 0.02).abs() < 0.000_001);
        assert_eq!(duration[0].3, REQUEST_BUCKETS.to_vec());

        let bytes = u64_points(metric(&batches, "image.response.bytes"));
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0].1, 120);
        assert_eq!(
            bytes[0].0,
            BTreeMap::from([
                ("format".to_string(), "jpeg".to_string()),
                ("path".to_string(), "pass_through".to_string()),
            ])
        );
    }

    #[test]
    fn failures_add_error_type_and_drop_bytes() {
        let harness = Harness::new();
        harness.metrics.record_request(
            "exe",
            RequestStatus::NotFound,
            RequestPath::NotFound,
            50,
            Duration::from_millis(1),
        );
        harness.metrics.record_request(
            "jpeg",
            RequestStatus::BadGateway,
            RequestPath::Backend,
            50,
            Duration::from_millis(1),
        );
        harness.metrics.record_request(
            "jpeg",
            RequestStatus::Error,
            RequestPath::Transform,
            50,
            Duration::from_millis(1),
        );
        let batches = harness.collect();
        let points = u64_points(metric(&batches, "image.request.count"));
        let mut by_status = BTreeMap::new();
        for (labels, value) in points {
            assert_eq!(value, 1);
            by_status.insert(labels["status"].clone(), labels);
        }
        assert_eq!(by_status["not_found"]["format"], "unknown");
        assert_eq!(by_status["not_found"]["error_type"], "client_error");
        assert_eq!(by_status["bad_gateway"]["error_type"], "site_error");
        assert_eq!(by_status["error"]["error_type"], "internal_error");
        let bytes = batches
            .iter()
            .flat_map(|batch| batch.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .find(|metric| metric.name() == "image.response.bytes");
        let bytes = bytes.map(u64_points).unwrap_or_default();
        assert!(
            bytes.is_empty(),
            "failed requests must not add response bytes"
        );
    }

    #[test]
    fn pipeline_step_labels_are_step_and_bounded_format() {
        let harness = Harness::new();
        harness
            .metrics
            .record_step(PipelineStep::Encode, "avif", Duration::from_millis(40));
        harness
            .metrics
            .record_step(PipelineStep::Decode, "nope", Duration::from_millis(2));
        let batches = harness.collect();
        let points = histogram_points(metric(&batches, "image.pipeline.step.duration"));
        assert_eq!(points.len(), 2);
        let encode = points
            .iter()
            .find(|point| point.0.get("step").map(String::as_str) == Some("encode"))
            .unwrap();
        assert_eq!(
            encode.0,
            BTreeMap::from([
                ("format".to_string(), "avif".to_string()),
                ("step".to_string(), "encode".to_string()),
            ])
        );
        assert_eq!(encode.3, super::STEP_BUCKETS.to_vec());
        let decode = points
            .iter()
            .find(|point| point.0.get("step").map(String::as_str) == Some("decode"))
            .unwrap();
        assert_eq!(decode.0["format"], "unknown");
    }

    #[test]
    fn in_flight_is_an_unlabeled_up_down_counter() {
        let harness = Harness::new();
        harness.metrics.add_in_flight(2);
        harness.metrics.add_in_flight(-1);
        let batches = harness.collect();
        let points = i64_points(metric(&batches, "image.request.in_flight"));
        assert_eq!(points, vec![(BTreeMap::new(), 1)]);
    }

    #[test]
    fn endpoint_resolution_and_redaction() {
        assert_eq!(resolve_endpoint(None, None), None);
        assert_eq!(
            resolve_endpoint(None, Some("http://collector:4318/")).as_deref(),
            Some("http://collector:4318/v1/metrics")
        );
        assert_eq!(
            resolve_endpoint(Some("http://collector:4318/custom"), Some("http://ignored"))
                .as_deref(),
            Some("http://collector:4318/custom")
        );
        assert_eq!(
            redact_endpoint("https://user:secret@collector:4318/v1/metrics"),
            "https://***@collector:4318/v1/metrics"
        );
        assert_eq!(
            redact_endpoint("http://collector:4318/v1/metrics"),
            "http://collector:4318/v1/metrics"
        );
    }

    #[test]
    fn export_stays_off_without_an_endpoint_and_rejects_other_protocols() {
        assert!(matches!(
            choose(None, None, None, None, None),
            ExportChoice::Off
        ));
        assert!(matches!(
            choose(Some("true"), None, None, Some("http://c"), None),
            ExportChoice::Off
        ));
        assert!(matches!(
            choose(None, Some("none"), None, Some("http://c"), None),
            ExportChoice::Off
        ));
        let on = choose(None, None, None, None, Some("http://collector:4318"));
        assert!(matches!(
            on,
            ExportChoice::On { ref endpoint } if endpoint == "http://collector:4318/v1/metrics"
        ));
        let rejected = choose(
            None,
            None,
            Some("grpc"),
            Some("http://collector:4317"),
            None,
        );
        assert!(matches!(
            rejected,
            ExportChoice::Rejected(ref message) if message.contains("http/protobuf")
        ));
    }

    #[test]
    fn a_bad_endpoint_fails_before_a_provider_starts() {
        let err = super::build_exporter("not a url").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("not a url"), "{message}");
    }
}
