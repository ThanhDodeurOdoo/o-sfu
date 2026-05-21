use std::fmt;

use super::{
    feature_flags::runtime_feature_flag_log_fields, settings::Config,
    telemetry::telemetry_log_fields,
};

pub(super) struct ConfigLogField {
    key: &'static str,
    value: String,
}

impl ConfigLogField {
    #[must_use]
    pub(super) fn new(key: &'static str, value: impl fmt::Display) -> Self {
        Self {
            key,
            value: value.to_string(),
        }
    }
}

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

fn write_top_field(formatter: &mut fmt::Formatter<'_>, field: ConfigLogField) -> fmt::Result {
    let ConfigLogField { key, value } = field;
    writeln!(formatter, "  - {key}={value}")
}

fn write_section(
    formatter: &mut fmt::Formatter<'_>,
    name: &str,
    fields: impl IntoIterator<Item = ConfigLogField>,
) -> fmt::Result {
    writeln!(formatter, "  - {name}:")?;
    fields
        .into_iter()
        .try_for_each(|field| write_field(formatter, field))
}

fn write_field(formatter: &mut fmt::Formatter<'_>, field: ConfigLogField) -> fmt::Result {
    let ConfigLogField { key, value } = field;
    writeln!(formatter, "    - {key}={value}")
}

impl fmt::Display for ConfigLogView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = self.config;
        writeln!(formatter, "booted runtime systems:")?;
        for field in config.top_log_fields(self.process_id) {
            write_top_field(formatter, field)?;
        }
        write_section(
            formatter,
            "telemetry",
            telemetry_log_fields(&config.telemetry, self.process_id),
        )?;
        write_section(
            formatter,
            "timing_and_admission",
            config.timing_and_admission_log_fields(),
        )?;
        write_section(formatter, "rtc_transport", config.transport.log_fields())?;
        write_section(
            formatter,
            "feature_flags",
            runtime_feature_flag_log_fields(config.features),
        )?;
        write_section(formatter, "codec_flags", config.codecs.log_fields())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::ConfigLogView;
    use crate::{
        config::{
            AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig,
            HttpConfig, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange,
            RuntimeFeatureFlags, TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
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
                room_media_limits: RoomMediaLimits::default(),
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
