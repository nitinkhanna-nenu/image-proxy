//! OpenTelemetry metrics for image requests.
//!
//! The exporter is compiled in with `--features otel` and left out otherwise.
//! Without that feature every function here is a no-op and [`install`] returns
//! `None`. With the feature, export still stays off until
//! `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is
//! set. `/metrics` is unchanged.
//!
//! One call records every instrument for a request, from the same bounded
//! labels. `format`, `status`, `path`, and `step` are closed sets; a value
//! outside the set is `unknown`. `error_type` is added only when the status is
//! not `ok`, and it has three values: who has to act. Duration and byte counts
//! are measurements, not labels.

use std::time::Duration;

use crate::metrics::{PipelineStep, RequestPath, RequestStatus};

#[cfg(feature = "otel")]
mod export;

/// Keeps the OTLP meter provider alive until [`Exporter::shutdown`].
pub struct Exporter {
    #[cfg(feature = "otel")]
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl Exporter {
    #[cfg(feature = "otel")]
    pub(super) fn new(provider: opentelemetry_sdk::metrics::SdkMeterProvider) -> Self {
        Self { provider }
    }

    /// Flushes the last batch. A no-op when the `otel` feature is off.
    pub fn shutdown(self) {
        #[cfg(feature = "otel")]
        if let Err(err) = self.provider.shutdown() {
            tracing::warn!(error = %err, "otel metrics shutdown failed");
        }
    }
}

/// Starts export when the `otel` feature is on and an endpoint is configured.
///
/// Hold the returned guard for the life of the process, then call
/// [`Exporter::shutdown`]. `None` means export is off.
#[must_use]
pub fn install() -> Option<Exporter> {
    #[cfg(feature = "otel")]
    {
        export::install()
    }
    #[cfg(not(feature = "otel"))]
    {
        None
    }
}

#[cfg_attr(not(feature = "otel"), allow(unused_variables))]
pub(crate) fn record_request(
    format: &str,
    status: RequestStatus,
    path: RequestPath,
    bytes: u64,
    duration: Duration,
) {
    #[cfg(feature = "otel")]
    export::record_request(format, status, path, bytes, duration);
}

#[cfg_attr(not(feature = "otel"), allow(unused_variables))]
pub(crate) fn add_in_flight(delta: i64) {
    #[cfg(feature = "otel")]
    export::add_in_flight(delta);
}

#[cfg_attr(not(feature = "otel"), allow(unused_variables))]
pub(crate) fn record_step(step: PipelineStep, format: &str, duration: Duration) {
    #[cfg(feature = "otel")]
    export::record_step(step, format, duration);
}

#[cfg(all(test, not(feature = "otel")))]
mod tests {
    #[test]
    fn feature_off_does_not_export() {
        assert!(super::install().is_none());
    }
}
