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

impl fmt::Display for ConfigLogView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "booted runtime systems:")?;
        writeln!(formatter, "  - pid={}", self.process_id)?;
        writeln!(formatter, "  - bind_address={}", config.http.bind_address)?;
        writeln!(formatter, "  - public_ip={}", config.transport.public_ip)?;
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
        writeln!(formatter, "  - telemetry:")?;
        writeln!(
            formatter,
            "    - service_name={}",
            config.telemetry.resource.service_name
        )?;
        writeln!(
            formatter,
            "    - deployment_environment={}",
            config.telemetry.resource.deployment_environment
        )?;
        writeln!(
            formatter,
            "    - service_instance_id={}",
            config
                .telemetry
                .resource
                .resolved_instance_id(self.process_id)
        )?;
        writeln!(
            formatter,
            "    - log_format={}",
            config.telemetry.log_format.as_str()
        )?;
        writeln!(
            formatter,
            "    - trace_export_otlp_endpoint={}",
            config
                .telemetry
                .trace_export
                .otlp_endpoint
                .as_deref()
                .unwrap_or("disabled")
        )
    }

    fn write_timing_and_admission(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "  - timing_and_admission:")?;
        writeln!(
            formatter,
            "    - authentication_timeout_ms={}",
            config.auth.authentication_timeout_ms
        )?;
        writeln!(
            formatter,
            "    - user_timeout_ms={}",
            config.user.timeout_ms
        )?;
        writeln!(
            formatter,
            "    - ping_interval_ms={}",
            config.user.ping_interval_ms
        )?;
        writeln!(formatter, "    - room_size={}", config.user.room_size)?;
        writeln!(
            formatter,
            "    - trust_proxy_headers={}",
            config.http.trust_proxy_headers
        )?;
        writeln!(
            formatter,
            "    - diagnostics_access={}",
            if config.diagnostics.auth_token.is_some() {
                "bearer_token"
            } else if config.http.bind_address.ip().is_loopback() {
                "loopback_only"
            } else {
                "disabled"
            }
        )
    }

    fn write_rtc_transport(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "  - rtc_transport:")?;
        writeln!(
            formatter,
            "    - max_bitrate_in_bps={}",
            config.transport.max_bitrate_in_bps
        )?;
        writeln!(
            formatter,
            "    - max_bitrate_out_bps={}",
            config.transport.max_bitrate_out_bps
        )?;
        writeln!(
            formatter,
            "    - max_video_bitrate_bps={}",
            config
                .transport
                .video_bitrate_limits
                .max_video_bitrate_bps()
        )?;
        writeln!(
            formatter,
            "    - rtc_port_range_min={}",
            config.transport.rtc_port_range.min()
        )?;
        writeln!(
            formatter,
            "    - rtc_port_range_max={}",
            config.transport.rtc_port_range.max()
        )?;
        writeln!(
            formatter,
            "    - rtc_media_worker_count={}",
            config.transport.rtc_media_worker_count
        )
    }

    fn write_feature_flags(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "  - feature_flags:")?;
        writeln!(
            formatter,
            "    - transcription={}",
            config.features.transcription
        )?;
        writeln!(
            formatter,
            "    - audio_recording={}",
            config.features.audio_recording
        )?;
        writeln!(
            formatter,
            "    - video_recording={}",
            config.features.video_recording
        )
    }

    fn write_codec_flags(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "  - codec_flags:")?;
        writeln!(
            formatter,
            "    - opus={}",
            config.codecs.flags.opus_enabled()
        )?;
        writeln!(
            formatter,
            "    - pcmu={}",
            config.codecs.flags.pcmu_enabled()
        )?;
        writeln!(
            formatter,
            "    - pcma={}",
            config.codecs.flags.pcma_enabled()
        )?;
        writeln!(formatter, "    - vp8={}", config.codecs.flags.vp8_enabled())?;
        writeln!(
            formatter,
            "    - h264={}",
            config.codecs.flags.h264_enabled()
        )?;
        writeln!(
            formatter,
            "    - h265={}",
            config.codecs.flags.h265_enabled()
        )?;
        writeln!(formatter, "    - vp9={}", config.codecs.flags.vp9_enabled())?;
        writeln!(formatter, "    - av1={}", config.codecs.flags.av1_enabled())?;
        writeln!(
            formatter,
            "    - audio_preference={}",
            config
                .codecs
                .preferences
                .audio_order()
                .map(o_sfu_core::AudioCodecPreference::wire_name)
                .join(",")
        )?;
        write!(
            formatter,
            "    - video_preference={}",
            config
                .codecs
                .preferences
                .video_order()
                .map(o_sfu_core::VideoCodecPreference::wire_name)
                .join(",")
        )
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::ConfigLogView;
    use crate::config::{
        AuthConfig, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TelemetryConfig, TransportConfig,
        UserConfig, VideoBitrateLimits,
    };

    fn test_config(bind_address: SocketAddr) -> Config {
        Config {
            auth: AuthConfig {
                key: "test-key".to_owned(),
                authentication_timeout_ms: 10_000,
            },
            http: HttpConfig {
                bind_address,
                trust_proxy_headers: false,
            },
            user: UserConfig {
                room_size: 100,
                timeout_ms: 10_000,
                ping_interval_ms: 60_000,
            },
            transport: TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                max_bitrate_in_bps: 8_000_000,
                max_bitrate_out_bps: 10_000_000,
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                rtc_media_worker_count: 1,
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
        assert!(rendered.contains("service_instance_id=pid-42"));
    }

    #[test]
    fn config_log_view_reports_disabled_diagnostics_for_non_loopback_bind_without_token() {
        let config = test_config(SocketAddr::from(([10, 0, 0, 1], 8070)));
        let rendered = ConfigLogView::new(&config, 7).to_string();
        assert!(rendered.contains("diagnostics_access=disabled"));
    }
}
