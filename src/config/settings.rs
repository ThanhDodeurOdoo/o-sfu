use std::net::{IpAddr, SocketAddr};

use super::{
    codec_flags::MediaCodecFlags, diagnostics::DiagnosticsConfig,
    feature_flags::RuntimeFeatureFlags, telemetry::TelemetryConfig, transport::RtcPortRange,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub auth_key: String,
    pub bind_address: SocketAddr,
    pub authentication_timeout_ms: u64,
    pub room_size: usize,
    pub diagnostics: DiagnosticsConfig,
    pub user_timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub trust_proxy_headers: bool,
    pub feature_flags: RuntimeFeatureFlags,
    pub codec_flags: MediaCodecFlags,
    pub telemetry: TelemetryConfig,
    pub public_ip: IpAddr,
    pub max_bitrate_in_bps: u64,
    pub max_bitrate_out_bps: u64,
    pub rtc_port_range: RtcPortRange,
    pub rtc_media_worker_count: usize,
}
