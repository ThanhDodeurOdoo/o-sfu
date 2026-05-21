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
