use std::net::{IpAddr, SocketAddr};

use super::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    diagnostics::DiagnosticsConfig, feature_flags::RuntimeFeatureFlags, telemetry::TelemetryConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub auth: AuthConfig,
    pub http: HttpConfig,
    pub user: UserConfig,
    pub transport: TransportConfig,
    pub codecs: CodecConfig,
    pub features: RuntimeFeatureFlags,
    pub telemetry: TelemetryConfig,
    pub diagnostics: DiagnosticsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub key: String,
    pub authentication_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind_address: SocketAddr,
    pub trust_proxy_headers: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserConfig {
    pub room_size: usize,
    pub timeout_ms: u64,
    pub ping_interval_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub public_ip: IpAddr,
    pub max_bitrate_in_bps: u64,
    pub max_bitrate_out_bps: u64,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub rtc_media_worker_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecConfig {
    pub flags: MediaCodecFlags,
    pub preferences: CodecPreferences,
}
