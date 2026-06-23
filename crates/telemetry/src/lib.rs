//! telemetry setup, metric catalog and diagnostics schema
//!
//! `o-sfu-telemetry` keep runtime observability contracts in one crate
//! the server initializes tracing from `TelemetryConfig`, reocrds process-local
//! metrics through typed helpers and renders diagnostics or Prometheus output
//! from snapshot types
//!
//! ```
//! use o_sfu_telemetry::{
//!     metrics::RuntimeMetrics,
//!     prometheus::render_prometheus,
//! };
//!
//! let metrics = RuntimeMetrics::default();
//! metrics.record_http_noop_request();
//!
//! let body = render_prometheus(&metrics);
//! assert!(body.contains("osfu_http_noop_requests_total"));
//! ```
//!
//! process-global tracing setup lives behind `init_tracing`
//! code that only needs counters, diagnostics or schema names can use those
//! modules without installing a tracing subscriber

mod config;
pub mod diagnostics;
pub mod graph;
pub mod metrics;
pub mod prometheus;
pub mod schema;
mod setup;

pub use config::{
    DEFAULT_MEDIA_QUALITY_INTERVAL, DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT,
    DEFAULT_TELEMETRY_SERVICE_NAME, TelemetryConfig, TelemetryLogFormat, TelemetryResource,
    TraceExportConfig,
};
#[cfg(feature = "macros")]
pub use o_sfu_telemetry_macros::{measure_duration, measure_http_request};
pub use setup::{
    TelemetryHandle, activated_span, http_request_span, init_tracing, ws_handshake_span,
    ws_upgrade_span,
};
