use std::fmt;

use super::settings::Config;

pub(crate) struct ConfigLogView<'a> {
    config: &'a Config,
    process_id: u32,
}

impl<'a> ConfigLogView<'a> {
    #[must_use]
    pub(crate) const fn new(config: &'a Config, process_id: u32) -> Self {
        Self { config, process_id }
    }
}

fn write_top_field(
    formatter: &mut fmt::Formatter<'_>,
    key: &str,
    value: impl fmt::Display,
) -> fmt::Result {
    writeln!(formatter, "  - {key}={value}")
}

fn write_section(formatter: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
    writeln!(formatter, "  - {name}:")
}

fn write_field(
    formatter: &mut fmt::Formatter<'_>,
    key: &str,
    value: impl fmt::Display,
) -> fmt::Result {
    writeln!(formatter, "    - {key}={value}")
}

impl fmt::Display for ConfigLogView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "booted runtime systems:")?;
        write_top_field(formatter, "pid", self.process_id)?;
        write_top_field(formatter, "bind_address", config.http.bind_address)?;
        write_top_field(formatter, "public_ip", config.transport.public_ip)?;
        self.write_telemetry(formatter)?;
        self.write_timing_and_admission(formatter)?;
        self.write_rtc_transport(formatter)?;
        self.write_feature_flags(formatter)?;
        self.write_codec_flags(formatter)
    }
}

impl ConfigLogView<'_> {
    fn write_telemetry(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        write_section(formatter, "telemetry")?;
        write_field(
            formatter,
            "service_name",
            config.telemetry.resource.service_name.as_str(),
        )?;
        write_field(
            formatter,
            "deployment_environment",
            config.telemetry.resource.deployment_environment.as_str(),
        )?;
        write_field(
            formatter,
            "service_instance_id",
            config
                .telemetry
                .resource
                .resolved_instance_id(self.process_id),
        )?;
        write_field(
            formatter,
            "log_format",
            config.telemetry.log_format.as_str(),
        )?;
        write_field(
            formatter,
            "trace_export_otlp_endpoint",
            config
                .telemetry
                .trace_export
                .otlp_endpoint
                .as_deref()
                .unwrap_or("disabled"),
        )
    }

    fn write_timing_and_admission(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        write_section(formatter, "timing_and_admission")?;
        write_field(
            formatter,
            "authentication_timeout_ms",
            config.auth.authentication_timeout_ms,
        )?;
        write_field(
            formatter,
            "max_pre_auth_websocket_sessions",
            config.auth.max_pre_auth_websocket_sessions,
        )?;
        write_field(
            formatter,
            "max_pre_auth_websocket_sessions_per_origin",
            config.auth.max_pre_auth_websocket_sessions_per_origin,
        )?;
        write_field(formatter, "user_timeout_ms", config.user.timeout_ms)?;
        write_field(formatter, "ping_interval_ms", config.user.ping_interval_ms)?;
        write_field(
            formatter,
            "user_outbound_queue_capacity",
            config.user.outbound_queue_capacity,
        )?;
        write_field(
            formatter,
            "user_outbound_queue_byte_capacity",
            config.user.outbound_queue_byte_capacity,
        )?;
        write_field(formatter, "room_size", config.user.room_size)?;
        write_field(
            formatter,
            "trust_proxy_headers",
            config.http.trust_proxy_headers,
        )?;
        write_field(
            formatter,
            "diagnostics_access",
            if config.diagnostics.auth_token.is_some() {
                "bearer_token"
            } else if config.http.bind_address.ip().is_loopback() {
                "loopback_only"
            } else {
                "disabled"
            },
        )
    }

    fn write_rtc_transport(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        write_section(formatter, "rtc_transport")?;
        write_field(
            formatter,
            "max_bitrate_in_bps",
            config.transport.max_bitrate_in.as_bps(),
        )?;
        write_field(
            formatter,
            "max_bitrate_out_bps",
            config.transport.max_bitrate_out.as_bps(),
        )?;
        write_field(
            formatter,
            "max_video_bitrate_bps",
            config
                .transport
                .video_bitrate_limits
                .max_video_bitrate()
                .as_bps(),
        )?;
        write_field(
            formatter,
            "rtc_port_range_min",
            config.transport.rtc_port_range.min(),
        )?;
        write_field(
            formatter,
            "rtc_port_range_max",
            config.transport.rtc_port_range.max(),
        )?;
        write_field(
            formatter,
            "rtc_media_worker_count",
            config.transport.rtc_media_worker_count,
        )?;
        write_field(
            formatter,
            "room_max_local_routers",
            config.transport.room_worker_policy.max_local_routers(),
        )
    }

    fn write_feature_flags(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        write_section(formatter, "feature_flags")?;
        write_field(formatter, "transcription", config.features.transcription)?;
        write_field(
            formatter,
            "audio_recording",
            config.features.audio_recording,
        )?;
        write_field(
            formatter,
            "video_recording",
            config.features.video_recording,
        )
    }

    fn write_codec_flags(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        write_section(formatter, "codec_flags")?;
        write_field(formatter, "opus", config.codecs.flags.opus_enabled())?;
        write_field(formatter, "pcmu", config.codecs.flags.pcmu_enabled())?;
        write_field(formatter, "pcma", config.codecs.flags.pcma_enabled())?;
        write_field(formatter, "vp8", config.codecs.flags.vp8_enabled())?;
        write_field(formatter, "h264", config.codecs.flags.h264_enabled())?;
        write_field(formatter, "h265", config.codecs.flags.h265_enabled())?;
        write_field(formatter, "vp9", config.codecs.flags.vp9_enabled())?;
        write_field(formatter, "av1", config.codecs.flags.av1_enabled())?;
        write_field(
            formatter,
            "audio_preference",
            config
                .codecs
                .preferences
                .audio_order()
                .map(o_sfu_core::AudioCodecPreference::wire_name)
                .join(","),
        )?;
        write_field(
            formatter,
            "video_preference",
            config
                .codecs
                .preferences
                .video_order()
                .map(o_sfu_core::VideoCodecPreference::wire_name)
                .join(","),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::ConfigLogView;
    use crate::{
        config::{
            AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig,
            HttpConfig, MediaCodecFlags, RoomWorkerPolicy, RtcPortRange, RuntimeFeatureFlags,
            TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
        },
        core::server::room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        },
    };

    fn test_config(bind_address: SocketAddr) -> Config {
        Config {
            auth: AuthConfig {
                key: "test-key".to_owned(),
                authentication_timeout_ms: 10_000,
                max_pre_auth_websocket_sessions: 512,
                max_pre_auth_websocket_sessions_per_origin: 16,
            },
            http: HttpConfig {
                bind_address,
                trust_proxy_headers: false,
            },
            user: UserConfig {
                room_size: 100,
                timeout_ms: 10_000,
                ping_interval_ms: 60_000,
                outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
                outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
            },
            transport: TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                max_bitrate_in: Bitrate::from_mbps(8),
                max_bitrate_out: Bitrate::from_mbps(10),
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                rtc_media_worker_count: 1,
                room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            },
            codecs: CodecConfig {
                flags: MediaCodecFlags::default(),
                preferences: CodecPreferences::default(),
            },
            features: RuntimeFeatureFlags::default(),
            telemetry: TelemetryConfig::default(),
            diagnostics: DiagnosticsConfig::default(),
        }
    }

    #[test]
    fn config_log_view_reports_loopback_only_diagnostics_for_local_bind_without_token() {
        let config = test_config(SocketAddr::from(([127, 0, 0, 1], 8070)));
        let rendered = ConfigLogView::new(&config, 42).to_string();
        assert!(rendered.contains("diagnostics_access=loopback_only"));
        assert!(rendered.contains("max_pre_auth_websocket_sessions=512"));
        assert!(rendered.contains("max_pre_auth_websocket_sessions_per_origin=16"));
        assert!(rendered.contains("service_instance_id=pid-42"));
    }

    #[test]
    fn config_log_view_reports_disabled_diagnostics_for_non_loopback_bind_without_token() {
        let config = test_config(SocketAddr::from(([10, 0, 0, 1], 8070)));
        let rendered = ConfigLogView::new(&config, 7).to_string();
        assert!(rendered.contains("diagnostics_access=disabled"));
    }
}
