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

pub use o_sfu_core::{
    Bitrate, CodecPreferences, MediaCodecFlags, RoomWorkerPolicy, RtcPortRange, VideoBitrateLimits,
};

pub(crate) use self::log_view::ConfigLogView;
pub use self::{
    diagnostics::DiagnosticsConfig,
    feature_flags::RuntimeFeatureFlags,
    settings::{
        AuthConfig, CodecConfig, Config, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, HttpConfig, TransportConfig,
        UserConfig,
    },
    telemetry::{TelemetryConfig, TelemetryLogFormat, TelemetryResource, TraceExportConfig},
};
