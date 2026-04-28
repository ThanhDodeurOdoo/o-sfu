mod codec_flags;
mod codec_preferences;
mod diagnostics;
mod feature_flags;
mod loader;
mod log_view;
mod parsing;
mod settings;
mod telemetry;
mod transport;

pub use o_sfu_core::{CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits};

pub(crate) use self::log_view::ConfigLogView;
pub use self::{
    diagnostics::DiagnosticsConfig,
    feature_flags::RuntimeFeatureFlags,
    settings::Config,
    telemetry::{TelemetryConfig, TelemetryLogFormat, TelemetryResource, TraceExportConfig},
};
