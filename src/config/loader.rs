use std::env;

use anyhow::Result;

use super::{
    CodecConfig, auth::load_auth_config, codec_flags::load_media_codec_flags,
    codec_preferences::load_codec_preferences, diagnostics::load_diagnostics_config,
    feature_flags::load_runtime_feature_flags, http::load_http_config, settings::Config,
    telemetry::load_telemetry_config, transport::load_transport_config, user::load_user_config,
};

impl Config {
    /// # Errors
    ///
    /// Returns an error when a required operator environment variable is
    /// missing, an environment value cannot be parsed, a constrained value is
    /// outside its accepted range, telemetry export is requested without the
    /// required cargo feature, or transport cross-field validation rejects the
    /// advertised address, RTC port range, worker count, or room policy.
    pub fn from_env() -> Result<Self> {
        Self::from_var_lookup(|key| env::var(key).ok())
    }

    fn from_var_lookup(mut get_var: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let http = load_http_config(&mut get_var)?;
        let auth = load_auth_config(&mut get_var)?;
        let user = load_user_config(&mut get_var)?;
        let feature_flags = load_runtime_feature_flags(&mut get_var)?;
        let codec_flags = load_media_codec_flags(&mut get_var)?;
        let codec_preferences = load_codec_preferences(&mut get_var)?;
        let diagnostics = load_diagnostics_config(&mut get_var)?;
        let telemetry = load_telemetry_config(&mut get_var)?;
        let transport = load_transport_config(&mut get_var)?;
        Ok(Self {
            auth,
            http,
            user,
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
    use super::super::transport::default_rtc_media_worker_count;
    use crate::{
        config::{
            Bitrate, CodecPreferences, Config, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
            DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, DiagnosticsConfig, MediaCodecFlags,
            RtcPortRange, RuntimeFeatureFlags, TelemetryConfig, VideoBitrateLimits,
        },
        core::server::room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        },
    };

    fn config_from(overrides: &[(&str, &str)]) -> anyhow::Result<Config> {
        Config::from_var_lookup(|key| {
            overrides
                .iter()
                .find(|(name, _value)| *name == key)
                .map(|(_name, value)| (*value).to_owned())
                .or_else(|| match key {
                    "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
                    "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
                    _ => None,
                })
        })
    }

    fn config_error_from(overrides: &[(&str, &str)]) -> Option<String> {
        config_from(overrides).err().map(|error| error.to_string())
    }

    #[test]
    fn config_requires_auth_key() {
        let error = Config::from_var_lookup(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(error.as_deref(), Some("AUTH_KEY env variable is required"));
    }

    #[test]
    fn config_uses_defaults_and_explicit_values() -> anyhow::Result<()> {
        let config = config_from(&[])?;
        assert_eq!(config.http.bind_address.to_string(), "0.0.0.0:8070");
        assert_eq!(config.auth.key, "dGVzdC1rZXk=");
        assert_eq!(config.auth.authentication_timeout_ms, 10_000);
        assert_eq!(
            config.auth.max_pre_auth_websocket_sessions,
            DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS
        );
        assert_eq!(
            config.auth.max_pre_auth_websocket_sessions_per_origin,
            DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN
        );
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
        assert_eq!(
            config.transport.rtc_media_worker_count,
            default_rtc_media_worker_count()
        );
        Ok(())
    }

    #[test]
    fn config_accepts_proxy_flag() -> anyhow::Result<()> {
        let config = config_from(&[("PROXY", "true")])?;
        assert!(config.http.trust_proxy_headers);
        Ok(())
    }

    #[test]
    fn config_accepts_explicit_http_auth_and_user_settings() -> anyhow::Result<()> {
        let config = config_from(&[
            ("BIND_ADDRESS", "127.0.0.1:9000"),
            ("AUTHENTICATION_TIMEOUT_MS", "1500"),
            ("MAX_PRE_AUTH_WEBSOCKET_SESSIONS", "12"),
            ("MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN", "3"),
            ("ROOM_SIZE", "4"),
            ("USER_TIMEOUT_MS", "5000"),
            ("PING_INTERVAL_MS", "1000"),
            ("USER_OUTBOUND_QUEUE_CAPACITY", "16"),
            ("USER_OUTBOUND_QUEUE_BYTE_CAPACITY", "8192"),
        ])?;
        assert_eq!(config.http.bind_address.to_string(), "127.0.0.1:9000");
        assert_eq!(config.auth.authentication_timeout_ms, 1500);
        assert_eq!(config.auth.max_pre_auth_websocket_sessions, 12);
        assert_eq!(config.auth.max_pre_auth_websocket_sessions_per_origin, 3);
        assert_eq!(config.user.room_size, 4);
        assert_eq!(config.user.timeout_ms, 5000);
        assert_eq!(config.user.ping_interval_ms, 1000);
        assert_eq!(config.user.outbound_queue_capacity, 16);
        assert_eq!(config.user.outbound_queue_byte_capacity, 8192);
        Ok(())
    }

    #[test]
    fn config_rejects_invalid_proxy_flag() {
        let error = config_error_from(&[("PROXY", "maybe")]);

        assert_eq!(
            error.as_deref(),
            Some("PROXY must be either `true` or `false`")
        );
    }

    #[test]
    fn config_rejects_zero_auth_and_user_limits() {
        let cases = [
            ("ROOM_SIZE", "ROOM_SIZE must be greater than zero"),
            (
                "USER_TIMEOUT_MS",
                "USER_TIMEOUT_MS must be greater than zero",
            ),
            (
                "PING_INTERVAL_MS",
                "PING_INTERVAL_MS must be greater than zero",
            ),
            (
                "MAX_PRE_AUTH_WEBSOCKET_SESSIONS",
                "MAX_PRE_AUTH_WEBSOCKET_SESSIONS must be greater than zero",
            ),
            (
                "MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN",
                "MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN must be greater than zero",
            ),
            (
                "USER_OUTBOUND_QUEUE_CAPACITY",
                "USER_OUTBOUND_QUEUE_CAPACITY must be greater than zero",
            ),
            (
                "USER_OUTBOUND_QUEUE_BYTE_CAPACITY",
                "USER_OUTBOUND_QUEUE_BYTE_CAPACITY must be greater than zero",
            ),
        ];

        for (key, message) in cases {
            let error = config_error_from(&[(key, "0")]);
            assert_eq!(error.as_deref(), Some(message), "{key}");
        }
    }
}
