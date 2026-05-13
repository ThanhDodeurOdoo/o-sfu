use std::{env, net::SocketAddr};

use anyhow::{Context, Result, ensure};

use super::{
    AuthConfig, CodecConfig, ConfigLogView, HttpConfig, UserConfig,
    codec_flags::load_media_codec_flags, codec_preferences::load_codec_preferences,
    diagnostics::load_diagnostics_config, feature_flags::load_runtime_feature_flags,
    parsing::parse_optional_env, settings::Config, telemetry::load_telemetry_config,
    transport::load_transport_config,
};
use crate::core::server::room::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
};

impl Config {
    /// # Errors
    ///
    /// Returns an error when `AUTH_KEY` is missing, `BIND_ADDRESS` is invalid,
    /// `AUTHENTICATION_TIMEOUT_MS` is invalid, `ROOM_SIZE` is zero,
    /// `USER_TIMEOUT_MS` is invalid, `PING_INTERVAL_MS` is invalid,
    /// `USER_OUTBOUND_QUEUE_BYTE_CAPACITY` is invalid, `PROXY` is invalid,
    /// `PUBLIC_IP` is missing or invalid, or `RTC_MIN_PORT`/`RTC_MAX_PORT`
    /// are invalid.
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
        let outbound_queue_capacity = parse_optional_env(
            &mut get_var,
            "USER_OUTBOUND_QUEUE_CAPACITY",
            "USER_OUTBOUND_QUEUE_CAPACITY must be a valid usize",
        )?
        .unwrap_or(DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY);
        let outbound_queue_byte_capacity = parse_optional_env(
            &mut get_var,
            "USER_OUTBOUND_QUEUE_BYTE_CAPACITY",
            "USER_OUTBOUND_QUEUE_BYTE_CAPACITY must be a valid usize",
        )?
        .unwrap_or(DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY);
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
        ensure!(
            outbound_queue_capacity > 0,
            "USER_OUTBOUND_QUEUE_CAPACITY must be greater than zero"
        );
        ensure!(
            outbound_queue_byte_capacity > 0,
            "USER_OUTBOUND_QUEUE_BYTE_CAPACITY must be greater than zero"
        );
        Ok(Self {
            auth: AuthConfig {
                key: auth_key,
                authentication_timeout_ms,
            },
            http: HttpConfig {
                bind_address,
                trust_proxy_headers,
            },
            user: UserConfig {
                room_size,
                timeout_ms: user_timeout_ms,
                ping_interval_ms,
                outbound_queue_capacity,
                outbound_queue_byte_capacity,
            },
            transport,
            codecs: CodecConfig {
                flags: codec_flags,
                preferences: codec_preferences,
            },
            features: feature_flags,
            telemetry,
            diagnostics,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{
            Bitrate, CodecPreferences, Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange,
            RuntimeFeatureFlags, TelemetryConfig, VideoBitrateLimits,
        },
        core::server::room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        },
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
        assert_eq!(config.http.bind_address.to_string(), "0.0.0.0:8070");
        assert_eq!(config.auth.key, "dGVzdC1rZXk=");
        assert_eq!(config.auth.authentication_timeout_ms, 10_000);
        assert_eq!(config.user.room_size, 100);
        assert_eq!(config.user.timeout_ms, 10_000);
        assert_eq!(config.user.ping_interval_ms, 60_000);
        assert_eq!(
            config.user.outbound_queue_capacity,
            DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY
        );
        assert_eq!(
            config.user.outbound_queue_byte_capacity,
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY
        );
        assert!(!config.http.trust_proxy_headers);
        assert_eq!(config.features, RuntimeFeatureFlags::default());
        assert_eq!(config.codecs.flags, MediaCodecFlags::default());
        assert_eq!(config.codecs.preferences, CodecPreferences::default());
        assert_eq!(config.diagnostics, DiagnosticsConfig::default());
        assert_eq!(config.telemetry, TelemetryConfig::default());
        assert_eq!(config.transport.public_ip.to_string(), "127.0.0.1");
        assert_eq!(config.transport.max_bitrate_in, Bitrate::from_mbps(8));
        assert_eq!(config.transport.max_bitrate_out, Bitrate::from_mbps(10));
        assert_eq!(
            config.transport.video_bitrate_limits,
            VideoBitrateLimits::default()
        );
        assert_eq!(
            config.transport.rtc_port_range,
            RtcPortRange::new(40_000, 49_999)
        );
        assert_eq!(config.transport.rtc_media_worker_count, 1);
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
        assert!(config.http.trust_proxy_headers);
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

    #[test]
    fn config_rejects_zero_user_outbound_queue_capacity() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "USER_OUTBOUND_QUEUE_CAPACITY" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_user_outbound_queue_byte_capacity() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "USER_OUTBOUND_QUEUE_BYTE_CAPACITY" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }
}
