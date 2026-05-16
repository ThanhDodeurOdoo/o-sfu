use std::{env, net::SocketAddr};

use anyhow::Result;

use super::{
    AuthConfig, CodecConfig, ConfigLogView, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
    DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN, HttpConfig, UserConfig,
    codec_flags::load_media_codec_flags,
    codec_preferences::load_codec_preferences,
    diagnostics::load_diagnostics_config,
    feature_flags::load_runtime_feature_flags,
    parsing::{
        parse_bool_env_or_default, parse_env_or_default, parse_positive_env_or_default,
        required_env,
    },
    settings::Config,
    telemetry::load_telemetry_config,
    transport::load_transport_config,
};
use crate::core::server::room::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
};

impl Config {
    /// # Errors
    ///
    /// Returns an error when `AUTH_KEY` is missing, `BIND_ADDRESS` is invalid,
    /// `AUTHENTICATION_TIMEOUT_MS` is invalid,
    /// `MAX_PRE_AUTH_WEBSOCKET_SESSIONS` is invalid,
    /// `MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN` is invalid,
    /// `ROOM_SIZE` is zero, `USER_TIMEOUT_MS` is invalid, `PING_INTERVAL_MS` is invalid,
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
        let bind_address = parse_env_or_default(
            &mut get_var,
            "BIND_ADDRESS",
            SocketAddr::from(([0, 0, 0, 0], 8070)),
        )?;
        let auth = load_auth_config(&mut get_var)?;
        let room_size = parse_positive_env_or_default(&mut get_var, "ROOM_SIZE", 100)?;
        let user_timeout_ms =
            parse_positive_env_or_default(&mut get_var, "USER_TIMEOUT_MS", 10_000)?;
        let ping_interval_ms =
            parse_positive_env_or_default(&mut get_var, "PING_INTERVAL_MS", 60_000)?;
        let outbound_queue_capacity = parse_positive_env_or_default(
            &mut get_var,
            "USER_OUTBOUND_QUEUE_CAPACITY",
            DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        )?;
        let outbound_queue_byte_capacity = parse_positive_env_or_default(
            &mut get_var,
            "USER_OUTBOUND_QUEUE_BYTE_CAPACITY",
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        )?;
        let trust_proxy_headers = parse_bool_env_or_default(&mut get_var, "PROXY", false)?;
        let feature_flags = load_runtime_feature_flags(&mut get_var)?;
        let codec_flags = load_media_codec_flags(&mut get_var)?;
        let codec_preferences = load_codec_preferences(&mut get_var)?;
        let diagnostics = load_diagnostics_config(&mut get_var)?;
        let telemetry = load_telemetry_config(&mut get_var)?;
        let transport = load_transport_config(&mut get_var)?;
        Ok(Self {
            auth,
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

fn load_auth_config(get_var: &mut impl FnMut(&str) -> Option<String>) -> Result<AuthConfig> {
    let key = required_env(get_var, "AUTH_KEY")?;
    let authentication_timeout_ms =
        parse_env_or_default(&mut *get_var, "AUTHENTICATION_TIMEOUT_MS", 10_000)?;
    let max_pre_auth_websocket_sessions = parse_positive_env_or_default(
        &mut *get_var,
        "MAX_PRE_AUTH_WEBSOCKET_SESSIONS",
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
    )?;
    let max_pre_auth_websocket_sessions_per_origin = parse_positive_env_or_default(
        &mut *get_var,
        "MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN",
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
    )?;
    Ok(AuthConfig {
        key,
        authentication_timeout_ms,
        max_pre_auth_websocket_sessions,
        max_pre_auth_websocket_sessions_per_origin,
    })
}

#[cfg(test)]
mod tests {
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
    fn config_rejects_zero_pre_auth_websocket_sessions() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_PRE_AUTH_WEBSOCKET_SESSIONS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_pre_auth_websocket_sessions_per_origin() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN" => Some("0".to_owned()),
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
