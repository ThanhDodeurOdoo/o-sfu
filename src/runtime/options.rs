use crate::{
    config::{AuthConfig, Config, DiagnosticsConfig, HttpConfig, RuntimeFeatureFlags, UserConfig},
    core::prelude::{
        CodecOptions, CoreOptions, MediaOptions, ObservabilityOptions, RoomMediaLimits,
        RoutingOptions,
    },
    runtime::SessionBitrateLimits,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeOptions {
    pub(crate) core: CoreOptions,
    pub(crate) room_media_limits: RoomMediaLimits,
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
                    rtc_udp_io_backend: config.transport.rtc_udp_io_backend,
                    bitrate_limits: SessionBitrateLimits::new(
                        config.transport.max_bitrate_in,
                        config.transport.max_bitrate_out,
                    ),
                    video_bitrate_limits: config.transport.video_bitrate_limits,
                },
                RoutingOptions {
                    media_worker_count: config.transport.rtc_media_worker_count,
                    room_worker_policy: config.transport.room_worker_policy,
                },
                CodecOptions {
                    flags: config.codecs.flags,
                    preferences: config.codecs.preferences,
                },
                ObservabilityOptions {
                    transport_diagnostics_enabled: true,
                    transport_metrics_enabled: true,
                    media_quality_interval: config.telemetry.media_quality_interval,
                },
            ),
            room_media_limits: config.transport.room_media_limits,
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
#[path = "TESTS/options.rs"]
mod tests;
