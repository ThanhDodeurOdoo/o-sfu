use std::net::{IpAddr, SocketAddr};

use o_sfu_core::prelude::{AudioCodecPreference, Bitrate, VideoCodecPreference};

use super::{
    CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange,
    VideoBitrateLimits, diagnostics::DiagnosticsConfig, feature_flags::RuntimeFeatureFlags,
    log_view::ConfigLogField, telemetry::TelemetryConfig,
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

impl Config {
    #[must_use]
    pub(super) fn top_log_fields(&self, process_id: u32) -> [ConfigLogField; 3] {
        [
            ConfigLogField::new("pid", process_id),
            ConfigLogField::new("bind_address", self.http.bind_address),
            ConfigLogField::new("public_ip", self.transport.public_ip),
        ]
    }

    #[must_use]
    pub(super) fn timing_and_admission_log_fields(&self) -> [ConfigLogField; 10] {
        [
            ConfigLogField::new(
                "authentication_timeout_ms",
                self.auth.authentication_timeout_ms,
            ),
            ConfigLogField::new(
                "max_pre_auth_websocket_sessions",
                self.auth.max_pre_auth_websocket_sessions,
            ),
            ConfigLogField::new(
                "max_pre_auth_websocket_sessions_per_origin",
                self.auth.max_pre_auth_websocket_sessions_per_origin,
            ),
            ConfigLogField::new("user_timeout_ms", self.user.timeout_ms),
            ConfigLogField::new("ping_interval_ms", self.user.ping_interval_ms),
            ConfigLogField::new(
                "user_outbound_queue_capacity",
                self.user.outbound_queue_capacity,
            ),
            ConfigLogField::new(
                "user_outbound_queue_byte_capacity",
                self.user.outbound_queue_byte_capacity,
            ),
            ConfigLogField::new("room_size", self.user.room_size),
            ConfigLogField::new("trust_proxy_headers", self.http.trust_proxy_headers),
            ConfigLogField::new("diagnostics_access", self.diagnostics_access()),
        ]
    }

    fn diagnostics_access(&self) -> &'static str {
        if self.diagnostics.auth_token.is_some() {
            "bearer_token"
        } else if self.http.bind_address.ip().is_loopback() {
            "loopback_only"
        } else {
            "disabled"
        }
    }
}

impl CodecConfig {
    #[must_use]
    pub(super) fn log_fields(&self) -> [ConfigLogField; 10] {
        [
            ConfigLogField::new("opus", self.flags.opus_enabled()),
            ConfigLogField::new("pcmu", self.flags.pcmu_enabled()),
            ConfigLogField::new("pcma", self.flags.pcma_enabled()),
            ConfigLogField::new("vp8", self.flags.vp8_enabled()),
            ConfigLogField::new("h264", self.flags.h264_enabled()),
            ConfigLogField::new("h265", self.flags.h265_enabled()),
            ConfigLogField::new("vp9", self.flags.vp9_enabled()),
            ConfigLogField::new("av1", self.flags.av1_enabled()),
            ConfigLogField::new(
                "audio_preference",
                self.preferences
                    .audio_order()
                    .map(AudioCodecPreference::wire_name)
                    .join(","),
            ),
            ConfigLogField::new(
                "video_preference",
                self.preferences
                    .video_order()
                    .map(VideoCodecPreference::wire_name)
                    .join(","),
            ),
        ]
    }
}
