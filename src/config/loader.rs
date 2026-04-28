use std::{env, net::SocketAddr};

use anyhow::{Context, Result, ensure};

use super::{
    ConfigLogView, codec_flags::load_media_codec_flags, codec_preferences::load_codec_preferences,
    diagnostics::load_diagnostics_config, feature_flags::load_runtime_feature_flags,
    parsing::parse_optional_env, settings::Config, telemetry::load_telemetry_config,
    transport::load_transport_config,
};

impl Config {
    /// # Errors
    ///
    /// Returns an error when `AUTH_KEY` is missing, `BIND_ADDRESS` is invalid,
    /// `AUTHENTICATION_TIMEOUT_MS` is invalid, `ROOM_SIZE` is zero,
    /// `USER_TIMEOUT_MS` is invalid, `PING_INTERVAL_MS` is invalid, `PROXY`
    /// is invalid, `PUBLIC_IP` is missing or invalid, or
    /// `RTC_MIN_PORT`/`RTC_MAX_PORT` are invalid.
    pub fn from_env() -> Result<Self> {
        Self::from_var_lookup(|key| env::var(key).ok())
    }

    #[must_use]
    pub(crate) const fn log_view(&self, process_id: u32) -> ConfigLogView<'_> {
        ConfigLogView::new(self, process_id)
    }

    fn from_var_lookup(mut get_var: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind_address: SocketAddr = get_var("BIND_ADDRESS")
            .unwrap_or_else(|| "0.0.0.0:8070".to_owned())
            .parse()
            .context("BIND_ADDRESS must be a valid socket address")?;
        let auth_key = get_var("AUTH_KEY").context("AUTH_KEY env variable is required")?;
        let authentication_timeout_ms = parse_optional_env(
            &mut get_var,
            "AUTHENTICATION_TIMEOUT_MS",
            "AUTHENTICATION_TIMEOUT_MS must be a valid u64",
        )?
        .unwrap_or(10_000);
        let room_size =
            parse_optional_env(&mut get_var, "ROOM_SIZE", "ROOM_SIZE must be a valid usize")?
                .unwrap_or(100);
        let user_timeout_ms = parse_optional_env(
            &mut get_var,
            "USER_TIMEOUT_MS",
            "USER_TIMEOUT_MS must be a valid u64",
        )?
        .unwrap_or(10_000);
        let ping_interval_ms = parse_optional_env(
            &mut get_var,
            "PING_INTERVAL_MS",
            "PING_INTERVAL_MS must be a valid u64",
        )?
        .unwrap_or(60_000);
        let trust_proxy_headers = parse_optional_env(
            &mut get_var,
            "PROXY",
            "PROXY must be either `true` or `false`",
        )?
        .unwrap_or(false);
        let feature_flags = load_runtime_feature_flags(&mut get_var)?;
        let codec_flags = load_media_codec_flags(&mut get_var)?;
        let codec_preferences = load_codec_preferences(&mut get_var)?;
        let diagnostics = load_diagnostics_config(&mut get_var)?;
        let telemetry = load_telemetry_config(&mut get_var)?;
        let transport = load_transport_config(&mut get_var)?;
        ensure!(room_size > 0, "ROOM_SIZE must be greater than zero");
        ensure!(
            user_timeout_ms > 0,
            "USER_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            ping_interval_ms > 0,
            "PING_INTERVAL_MS must be greater than zero"
        );
        Ok(Self {
            auth_key,
            bind_address,
            authentication_timeout_ms,
            room_size,
            diagnostics,
            user_timeout_ms,
            ping_interval_ms,
            trust_proxy_headers,
            feature_flags,
            codec_flags,
            codec_preferences,
            telemetry,
            public_ip: transport.public_ip,
            max_bitrate_in_bps: transport.max_bitrate_in_bps,
            max_bitrate_out_bps: transport.max_bitrate_out_bps,
            video_bitrate_limits: transport.video_bitrate_limits,
            rtc_port_range: transport.rtc_port_range,
            rtc_media_worker_count: transport.rtc_media_worker_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{
        CodecPreferences, Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange,
        RuntimeFeatureFlags, TelemetryConfig, VideoBitrateLimits,
    };

    #[test]
    fn config_requires_auth_key() {
        let error = Config::from_var_lookup(|_| None).err();
        assert!(error.is_some());
        let Some(error) = error else {
            return;
        };
        assert!(error.to_string().contains("AUTH_KEY"));
    }

    #[test]
    fn config_uses_defaults_and_explicit_values() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.bind_address.to_string(), "0.0.0.0:8070");
        assert_eq!(config.auth_key, "dGVzdC1rZXk=");
        assert_eq!(config.authentication_timeout_ms, 10_000);
        assert_eq!(config.room_size, 100);
        assert_eq!(config.user_timeout_ms, 10_000);
        assert_eq!(config.ping_interval_ms, 60_000);
        assert!(!config.trust_proxy_headers);
        assert_eq!(config.feature_flags, RuntimeFeatureFlags::default());
        assert_eq!(config.codec_flags, MediaCodecFlags::default());
        assert_eq!(config.codec_preferences, CodecPreferences::default());
        assert_eq!(config.diagnostics, DiagnosticsConfig::default());
        assert_eq!(config.telemetry, TelemetryConfig::default());
        assert_eq!(config.public_ip.to_string(), "127.0.0.1");
        assert_eq!(config.max_bitrate_in_bps, 8_000_000);
        assert_eq!(config.max_bitrate_out_bps, 10_000_000);
        assert_eq!(config.video_bitrate_limits, VideoBitrateLimits::default());
        assert_eq!(config.rtc_port_range, RtcPortRange::new(40_000, 49_999));
        assert_eq!(config.rtc_media_worker_count, 1);
    }

    #[test]
    fn config_accepts_proxy_flag() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "PROXY" => Some("true".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert!(config.trust_proxy_headers);
    }

    #[test]
    fn config_rejects_zero_room_size() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "ROOM_SIZE" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_user_timeout() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "USER_TIMEOUT_MS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_ping_interval() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "PING_INTERVAL_MS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }
}
