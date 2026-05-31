use std::net::{IpAddr, SocketAddr};

use o_sfu_core::prelude::Bitrate;

use super::{
    CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange,
    VideoBitrateLimits, diagnostics::DiagnosticsConfig, feature_flags::RuntimeFeatureFlags,
    telemetry::TelemetryConfig,
};

pub const DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS: usize = 512;
pub const DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN: usize = 16;

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
    pub max_pre_auth_websocket_sessions: usize,
    pub max_pre_auth_websocket_sessions_per_origin: usize,
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
    pub outbound_queue_capacity: usize,
    pub outbound_queue_byte_capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub public_ip: IpAddr,
    pub max_bitrate_in: Bitrate,
    pub max_bitrate_out: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub rtc_media_worker_count: usize,
    pub room_worker_policy: RoomWorkerPolicy,
    pub room_media_limits: RoomMediaLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecConfig {
    pub flags: MediaCodecFlags,
    pub preferences: CodecPreferences,
}
