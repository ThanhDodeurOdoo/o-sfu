//! Prometheus text rendering for runtime metrics.
//!
//! The renderer emits `RuntimeMetrics` in the Prometheus text exposition format
//! used by the server `/metrics` route.
//!
//! ```
//! use o_sfu_telemetry::{
//!     metrics::{HttpRoute, RuntimeMetrics},
//!     prometheus::render_prometheus,
//! };
//!
//! let metrics = RuntimeMetrics::default();
//! let request = metrics.track_http_request(HttpRoute::Noop);
//! let rendered = render_prometheus(&metrics);
//! drop(request);
//!
//! let expected_lines = [
//!     "# TYPE osfu_http_noop_requests_total counter",
//!     "osfu_http_noop_requests_total 1",
//!     "# TYPE osfu_http_inflight_requests gauge",
//!     "osfu_http_inflight_requests{route=\"noop\"} 1",
//!     "# TYPE osfu_http_request_duration_seconds histogram",
//!     "osfu_http_request_duration_seconds_count{route=\"noop\"} 0",
//! ];
//!
//! for line in expected_lines {
//!     assert!(rendered.contains(line), "missing Prometheus line: {line}");
//! }
//! ```

use crate::metrics::{RuntimeMetrics, render_prometheus_text};

pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub fn render_prometheus(metrics: &RuntimeMetrics) -> String {
    render_prometheus_text(metrics)
}

#[cfg(test)]
#[path = "TESTS/mod.rs"]
mod tests;
