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
        writeln!(formatter, "  - bind_address={}", config.bind_address)?;
        writeln!(formatter, "  - public_ip={}", config.public_ip)?;
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
            config.authentication_timeout_ms
        )?;
        writeln!(
            formatter,
            "    - session_timeout_ms={}",
            config.session_timeout_ms
        )?;
        writeln!(
            formatter,
            "    - ping_interval_ms={}",
            config.ping_interval_ms
        )?;
        writeln!(formatter, "    - channel_size={}", config.channel_size)?;
        writeln!(
            formatter,
            "    - trust_proxy_headers={}",
            config.trust_proxy_headers
        )?;
        writeln!(
            formatter,
            "    - diagnostics_access={}",
            if config.diagnostics.auth_token.is_some() {
                "bearer_token"
            } else if config.bind_address.ip().is_loopback() {
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
            config.max_bitrate_in_bps
        )?;
        writeln!(
            formatter,
            "    - max_bitrate_out_bps={}",
            config.max_bitrate_out_bps
        )?;
        writeln!(
            formatter,
            "    - rtc_port_range_min={}",
            config.rtc_port_range.min()
        )?;
        writeln!(
            formatter,
            "    - rtc_port_range_max={}",
            config.rtc_port_range.max()
        )?;
        writeln!(
            formatter,
            "    - rtc_media_worker_count={}",
            config.rtc_media_worker_count
        )
    }

    fn write_feature_flags(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "  - feature_flags:")?;
        writeln!(
            formatter,
            "    - transcription={}",
            config.feature_flags.transcription
        )?;
        writeln!(
            formatter,
            "    - audio_recording={}",
            config.feature_flags.audio_recording
        )?;
        writeln!(
            formatter,
            "    - video_recording={}",
            config.feature_flags.video_recording
        )
    }

    fn write_codec_flags(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "  - codec_flags:")?;
        writeln!(
            formatter,
            "    - opus={}",
            config.codec_flags.opus_enabled()
        )?;
        writeln!(
            formatter,
            "    - pcmu={}",
            config.codec_flags.pcmu_enabled()
        )?;
        writeln!(
            formatter,
            "    - pcma={}",
            config.codec_flags.pcma_enabled()
        )?;
        writeln!(formatter, "    - vp8={}", config.codec_flags.vp8_enabled())?;
        writeln!(
            formatter,
            "    - h264={}",
            config.codec_flags.h264_enabled()
        )?;
        writeln!(
            formatter,
            "    - h265={}",
            config.codec_flags.h265_enabled()
        )?;
        writeln!(formatter, "    - vp9={}", config.codec_flags.vp9_enabled())?;
        write!(formatter, "    - av1={}", config.codec_flags.av1_enabled())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::ConfigLogView;
    use crate::config::{
        Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
        TelemetryConfig,
    };

    fn test_config(bind_address: SocketAddr) -> Config {
        Config {
            auth_key: "test-key".to_owned(),
            bind_address,
            authentication_timeout_ms: 10_000,
            channel_size: 100,
            diagnostics: DiagnosticsConfig::default(),
            session_timeout_ms: 10_000,
            ping_interval_ms: 60_000,
            trust_proxy_headers: false,
            feature_flags: RuntimeFeatureFlags::default(),
            codec_flags: MediaCodecFlags::default(),
            telemetry: TelemetryConfig::default(),
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            max_bitrate_in_bps: 8_000_000,
            max_bitrate_out_bps: 10_000_000,
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            rtc_media_worker_count: 1,
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
