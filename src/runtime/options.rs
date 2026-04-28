use std::net::SocketAddr;

use crate::{
    config::{Config, DiagnosticsConfig, RuntimeFeatureFlags},
    core::{CodecOptions, CoreOptions, MediaOptions, ObservabilityOptions, RoutingOptions},
    runtime::SessionBitrateLimits,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeOptions {
    pub(crate) room: RoomOptions,
    pub(crate) recording_policy: RecordingPolicyOptions,
    pub(crate) core: CoreOptions,
    pub(crate) http: HttpOptions,
    pub(crate) websocket: SocketOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthOptions {
    pub(crate) key: String,
    pub(crate) authentication_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoomOptions {
    pub(crate) max_users: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UserOptions {
    pub(crate) timeout_ms: u64,
    pub(crate) ping_interval_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordingPolicyOptions {
    pub(crate) audio_enabled: bool,
    pub(crate) video_enabled: bool,
    pub(crate) transcription_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpOptions {
    pub(crate) bind_address: SocketAddr,
    pub(crate) auth: AuthOptions,
    pub(crate) diagnostics: DiagnosticsConfig,
    pub(crate) trust_proxy_headers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SocketOptions {
    pub(crate) auth: AuthOptions,
    pub(crate) user: UserOptions,
    pub(crate) trust_proxy_headers: bool,
}

impl RuntimeOptions {
    #[must_use]
    pub(crate) fn from_config(config: &Config) -> Self {
        let auth = AuthOptions {
            key: config.auth_key.clone(),
            authentication_timeout_ms: config.authentication_timeout_ms,
        };
        let room = RoomOptions {
            max_users: config.room_size,
        };
        let user = UserOptions {
            timeout_ms: config.user_timeout_ms,
            ping_interval_ms: config.ping_interval_ms,
        };
        let recording_policy = RecordingPolicyOptions {
            audio_enabled: config.feature_flags.audio_recording,
            video_enabled: config.feature_flags.video_recording,
            transcription_enabled: config.feature_flags.transcription
                && (config.feature_flags.audio_recording || config.feature_flags.video_recording),
        };
        let core = CoreOptions::new(
            MediaOptions {
                public_ip: config.public_ip,
                rtc_port_range: config.rtc_port_range,
                bitrate_limits: SessionBitrateLimits::new(
                    config.max_bitrate_in_bps,
                    config.max_bitrate_out_bps,
                ),
                video_bitrate_limits: config.video_bitrate_limits,
            },
            RoutingOptions {
                media_worker_count: config.rtc_media_worker_count,
            },
            CodecOptions {
                flags: config.codec_flags,
                preferences: config.codec_preferences,
            },
            ObservabilityOptions {
                transport_diagnostics_enabled: true,
                transport_metrics_enabled: true,
            },
        );
        let http = HttpOptions {
            bind_address: config.bind_address,
            auth: auth.clone(),
            diagnostics: config.diagnostics.clone(),
            trust_proxy_headers: config.trust_proxy_headers,
        };
        let websocket = SocketOptions {
            auth,
            user,
            trust_proxy_headers: config.trust_proxy_headers,
        };
        Self {
            room,
            recording_policy,
            core,
            http,
            websocket,
        }
    }

    #[must_use]
    pub(crate) const fn feature_flags(&self) -> RuntimeFeatureFlags {
        RuntimeFeatureFlags {
            transcription: self.recording_policy.transcription_enabled,
            audio_recording: self.recording_policy.audio_enabled,
            video_recording: self.recording_policy.video_enabled,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::RuntimeOptions;
    use crate::config::{
        CodecPreferences, Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange,
        RuntimeFeatureFlags, TelemetryConfig, VideoBitrateLimits,
    };

    #[test]
    fn runtime_options_group_config_by_runtime_boundary() {
        let config = Config {
            auth_key: "dGVzdC1rZXk=".to_owned(),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8090)),
            authentication_timeout_ms: 1_500,
            room_size: 42,
            diagnostics: DiagnosticsConfig {
                auth_token: Some("operator-secret".to_owned()),
            },
            user_timeout_ms: 7_000,
            ping_interval_ms: 11_000,
            trust_proxy_headers: true,
            feature_flags: RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: false,
            },
            codec_flags: MediaCodecFlags::default().with_h264(true),
            codec_preferences: CodecPreferences::default(),
            telemetry: TelemetryConfig::default(),
            public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            max_bitrate_in_bps: 1_234_000,
            max_bitrate_out_bps: 5_678_000,
            video_bitrate_limits: VideoBitrateLimits::new(4_321_000),
            rtc_port_range: RtcPortRange::new(50_000, 50_099),
            rtc_media_worker_count: 4,
        };

        let options = RuntimeOptions::from_config(&config);

        assert_eq!(options.http.auth.key, config.auth_key.as_str());
        assert_eq!(options.room.max_users, 42);
        assert_eq!(options.websocket.user.timeout_ms, 7_000);
        assert!(options.recording_policy.transcription_enabled);
        assert!(options.recording_policy.audio_enabled);
        assert!(!options.recording_policy.video_enabled);
        assert_eq!(options.feature_flags(), config.feature_flags);
        assert_eq!(options.core.media.public_ip, config.public_ip);
        assert_eq!(options.core.media.rtc_port_range, config.rtc_port_range);
        assert_eq!(
            options.core.media.bitrate_limits.max_bitrate_in_bps(),
            config.max_bitrate_in_bps
        );
        assert_eq!(
            options.core.media.video_bitrate_limits,
            config.video_bitrate_limits
        );
        assert_eq!(options.core.routing.media_worker_count, 4);
        assert_eq!(options.core.codecs.flags, config.codec_flags);
        assert_eq!(options.core.codecs.preferences, config.codec_preferences);
        assert_eq!(options.http.bind_address, config.bind_address);
        assert_eq!(options.http.auth.key, config.auth_key.as_str());
        assert_eq!(options.http.diagnostics, config.diagnostics);
        assert!(options.http.trust_proxy_headers);
        assert_eq!(
            options.websocket.auth.authentication_timeout_ms,
            config.authentication_timeout_ms
        );
        assert_eq!(options.websocket.user.ping_interval_ms, 11_000);
        assert!(options.websocket.trust_proxy_headers);
    }

    #[test]
    fn transcription_feature_is_part_of_recording_policy() {
        let mut config = Config {
            auth_key: "dGVzdC1rZXk=".to_owned(),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8090)),
            authentication_timeout_ms: 1_500,
            room_size: 42,
            diagnostics: DiagnosticsConfig::default(),
            user_timeout_ms: 7_000,
            ping_interval_ms: 11_000,
            trust_proxy_headers: true,
            feature_flags: RuntimeFeatureFlags {
                transcription: true,
                audio_recording: false,
                video_recording: false,
            },
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: CodecPreferences::default(),
            telemetry: TelemetryConfig::default(),
            public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            max_bitrate_in_bps: 1_234_000,
            max_bitrate_out_bps: 5_678_000,
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(50_000, 50_099),
            rtc_media_worker_count: 4,
        };

        let options = RuntimeOptions::from_config(&config);

        assert!(!options.recording_policy.transcription_enabled);
        assert_eq!(
            options.feature_flags(),
            RuntimeFeatureFlags {
                transcription: false,
                audio_recording: false,
                video_recording: false,
            }
        );

        config.feature_flags.audio_recording = true;
        let options = RuntimeOptions::from_config(&config);

        assert!(options.recording_policy.transcription_enabled);
        assert_eq!(
            options.feature_flags(),
            RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: false,
            }
        );
    }
}
