use crate::{
    config::{AuthConfig, Config, DiagnosticsConfig, HttpConfig, RuntimeFeatureFlags, UserConfig},
    core::{CodecOptions, CoreOptions, MediaOptions, ObservabilityOptions, RoutingOptions},
    runtime::SessionBitrateLimits,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeOptions {
    pub(crate) core: CoreOptions,
    effective_feature_flags: RuntimeFeatureFlags,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeConfig {
    pub(crate) auth: AuthConfig,
    pub(crate) http: HttpConfig,
    pub(crate) user: UserConfig,
    pub(crate) diagnostics: DiagnosticsConfig,
}

impl RuntimeOptions {
    #[must_use]
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            core: CoreOptions::new(
                MediaOptions {
                    public_ip: config.transport.public_ip,
                    rtc_port_range: config.transport.rtc_port_range,
                    bitrate_limits: SessionBitrateLimits::new(
                        config.transport.max_bitrate_in,
                        config.transport.max_bitrate_out,
                    ),
                    video_bitrate_limits: config.transport.video_bitrate_limits,
                },
                RoutingOptions {
                    media_worker_count: config.transport.rtc_media_worker_count,
                    room_sharding_policy: config.transport.room_sharding_policy,
                },
                CodecOptions {
                    flags: config.codecs.flags,
                    preferences: config.codecs.preferences,
                },
                ObservabilityOptions {
                    transport_diagnostics_enabled: true,
                    transport_metrics_enabled: true,
                },
            ),
            effective_feature_flags: effective_feature_flags(config.features),
        }
    }

    #[must_use]
    pub(crate) const fn effective_feature_flags(&self) -> RuntimeFeatureFlags {
        self.effective_feature_flags
    }
}

impl RuntimeConfig {
    #[must_use]
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            auth: config.auth.clone(),
            http: config.http.clone(),
            user: config.user,
            diagnostics: config.diagnostics.clone(),
        }
    }
}

const fn effective_feature_flags(features: RuntimeFeatureFlags) -> RuntimeFeatureFlags {
    RuntimeFeatureFlags {
        transcription: features.transcription
            && (features.audio_recording || features.video_recording),
        audio_recording: features.audio_recording,
        video_recording: features.video_recording,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::RuntimeOptions;
    use crate::{
        config::{
            AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig,
            HttpConfig, MediaCodecFlags, RoomShardingPolicy, RtcPortRange, RuntimeFeatureFlags,
            TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
        },
        core::server::room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        },
    };

    fn test_config() -> Config {
        Config {
            auth: AuthConfig {
                key: "dGVzdC1rZXk=".to_owned(),
                authentication_timeout_ms: 1_500,
            },
            http: HttpConfig {
                bind_address: SocketAddr::from(([127, 0, 0, 1], 8090)),
                trust_proxy_headers: true,
            },
            user: UserConfig {
                room_size: 42,
                timeout_ms: 7_000,
                ping_interval_ms: 11_000,
                outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
                outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
            },
            transport: TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
                max_bitrate_in: Bitrate::from_kbps(1_234),
                max_bitrate_out: Bitrate::from_kbps(5_678),
                video_bitrate_limits: VideoBitrateLimits::new(Bitrate::from_kbps(4_321)),
                rtc_port_range: RtcPortRange::new(50_000, 50_099),
                rtc_media_worker_count: 4,
                room_sharding_policy: RoomShardingPolicy::bounded_local_spillover(2),
            },
            codecs: CodecConfig {
                flags: MediaCodecFlags::default().with_h264(true),
                preferences: CodecPreferences::default(),
            },
            features: RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: false,
            },
            telemetry: TelemetryConfig::default(),
            diagnostics: DiagnosticsConfig {
                auth_token: Some("operator-secret".to_owned()),
            },
        }
    }

    #[test]
    fn runtime_options_project_core_settings() {
        let config = test_config();

        let options = RuntimeOptions::from_config(&config);

        assert_eq!(options.core.media.public_ip, config.transport.public_ip);
        assert_eq!(
            options.core.media.rtc_port_range,
            config.transport.rtc_port_range
        );
        assert_eq!(
            options.core.media.bitrate_limits.max_bitrate_in(),
            config.transport.max_bitrate_in
        );
        assert_eq!(
            options.core.media.video_bitrate_limits,
            config.transport.video_bitrate_limits
        );
        assert_eq!(options.core.routing.media_worker_count, 4);
        assert_eq!(
            options
                .core
                .routing
                .room_sharding_policy
                .max_local_routers(),
            2
        );
        assert_eq!(options.core.codecs.flags, config.codecs.flags);
        assert_eq!(options.core.codecs.preferences, config.codecs.preferences);
    }

    #[test]
    fn effective_feature_flags_disable_transcription_without_recording() {
        let mut config = test_config();
        config.features = RuntimeFeatureFlags {
            transcription: true,
            audio_recording: false,
            video_recording: false,
        };

        let options = RuntimeOptions::from_config(&config);

        assert_eq!(
            options.effective_feature_flags(),
            RuntimeFeatureFlags {
                transcription: false,
                audio_recording: false,
                video_recording: false,
            }
        );

        config.features.audio_recording = true;
        let options = RuntimeOptions::from_config(&config);

        assert_eq!(
            options.effective_feature_flags(),
            RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: false,
            }
        );
    }
}
