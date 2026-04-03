use std::{env, net::SocketAddr, str::FromStr};

use anyhow::{Context, Result, anyhow, ensure};

use crate::signaling::DEFAULT_AUTHENTICATION_TIMEOUT_MS;

const DEFAULT_CHANNEL_SIZE: usize = 100;
const TRANSPORT_BACKEND_STUB: &str = "stub";
const TRANSPORT_BACKEND_WEBRTC_RS: &str = "webrtc-rs";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportBackend {
    Stub,
    WebRtcRs,
}

impl FromStr for TransportBackend {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            TRANSPORT_BACKEND_STUB => Ok(Self::Stub),
            TRANSPORT_BACKEND_WEBRTC_RS => Ok(Self::WebRtcRs),
            _ => Err(anyhow!(
                "TRANSPORT_BACKEND must be either `{TRANSPORT_BACKEND_STUB}` or `{TRANSPORT_BACKEND_WEBRTC_RS}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub auth_key: String,
    pub bind_address: SocketAddr,
    pub authentication_timeout_ms: u64,
    pub channel_size: usize,
    pub transport_backend: TransportBackend,
}

impl Config {
    /// # Errors
    ///
    /// Returns an error when `AUTH_KEY` is missing, `BIND_ADDRESS` is invalid,
    /// `AUTHENTICATION_TIMEOUT_MS` is invalid, `CHANNEL_SIZE` is zero,
    /// or `TRANSPORT_BACKEND` is invalid.
    pub fn from_env() -> Result<Self> {
        Self::from_var_lookup(|key| env::var(key).ok())
    }

    fn from_var_lookup(mut get_var: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind_address = get_var("BIND_ADDRESS")
            .unwrap_or_else(|| "0.0.0.0:8080".to_owned())
            .parse()
            .context("BIND_ADDRESS must be a valid socket address")?;
        let auth_key = get_var("AUTH_KEY").context("AUTH_KEY env variable is required")?;
        let authentication_timeout_ms = parse_optional_env(
            &mut get_var,
            "AUTHENTICATION_TIMEOUT_MS",
            "AUTHENTICATION_TIMEOUT_MS must be a valid u64",
        )?
        .unwrap_or(DEFAULT_AUTHENTICATION_TIMEOUT_MS);
        let channel_size = parse_optional_env(
            &mut get_var,
            "CHANNEL_SIZE",
            "CHANNEL_SIZE must be a valid usize",
        )?
        .unwrap_or(DEFAULT_CHANNEL_SIZE);
        let transport_backend = parse_optional_env(
            &mut get_var,
            "TRANSPORT_BACKEND",
            "TRANSPORT_BACKEND must be either `stub` or `webrtc-rs`",
        )?
        .unwrap_or(TransportBackend::Stub);
        ensure!(channel_size > 0, "CHANNEL_SIZE must be greater than zero");
        Ok(Self {
            auth_key,
            bind_address,
            authentication_timeout_ms,
            channel_size,
            transport_backend,
        })
    }
}

fn parse_optional_env<T>(
    mut get_var: impl FnMut(&str) -> Option<String>,
    key: &str,
    error_message: &str,
) -> Result<Option<T>>
where
    T: FromStr,
{
    get_var(key)
        .map(|value| {
            value
                .parse()
                .map_err(|_error| anyhow!(error_message.to_owned()))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::{Config, TransportBackend};

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
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.bind_address.to_string(), "0.0.0.0:8080");
        assert_eq!(config.auth_key, "dGVzdC1rZXk=");
        assert_eq!(config.authentication_timeout_ms, 10_000);
        assert_eq!(config.channel_size, 100);
        assert_eq!(config.transport_backend, TransportBackend::Stub);
    }

    #[test]
    fn config_rejects_zero_channel_size() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "CHANNEL_SIZE" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_accepts_webrtc_rs_transport_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "TRANSPORT_BACKEND" => Some("webrtc-rs".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.transport_backend, TransportBackend::WebRtcRs);
    }

    #[test]
    fn config_rejects_unknown_transport_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "TRANSPORT_BACKEND" => Some("unknown".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }
}
