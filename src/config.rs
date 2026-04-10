use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow, ensure};

use crate::signaling::DEFAULT_AUTHENTICATION_TIMEOUT_MS;

const DEFAULT_CHANNEL_SIZE: usize = 100;
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_PING_INTERVAL_MS: u64 = 60_000;
const DEFAULT_RTC_MIN_PORT: u16 = 40_000;
const DEFAULT_RTC_MAX_PORT: u16 = 49_999;
const DEFAULT_RTC_MEDIA_WORKER_COUNT: usize = 1;
const TRANSPORT_BACKEND_STUB: &str = "stub";
const TRANSPORT_BACKEND_RTC: &str = "rtc";
const STUB_PUBLIC_IP_DEFAULT: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtcPortRange {
    min: u16,
    max: u16,
}

impl RtcPortRange {
    #[must_use]
    pub const fn new(min: u16, max: u16) -> Self {
        Self { min, max }
    }

    #[must_use]
    pub const fn min(self) -> u16 {
        self.min
    }

    #[must_use]
    pub const fn max(self) -> u16 {
        self.max
    }

    #[must_use]
    pub const fn port_count(self) -> u16 {
        self.max - self.min + 1
    }

    pub fn ports(self) -> impl Iterator<Item = u16> {
        self.min..=self.max
    }

    #[must_use]
    pub fn split_for_workers(self, worker_count: usize) -> Option<Vec<Self>> {
        if worker_count == 0 || worker_count > usize::from(self.port_count()) {
            return None;
        }
        let total_ports = usize::from(self.port_count());
        let base_ports_per_worker = total_ports / worker_count;
        let extra_ports = total_ports % worker_count;
        let mut next_min = u32::from(self.min);
        let mut ranges = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let worker_port_count = base_ports_per_worker + usize::from(worker_idx < extra_ports);
            let worker_port_count = u32::try_from(worker_port_count).ok()?;
            let max_inclusive = next_min + worker_port_count - 1;
            ranges.push(Self::new(
                u16::try_from(next_min).ok()?,
                u16::try_from(max_inclusive).ok()?,
            ));
            next_min = max_inclusive + 1;
        }
        Some(ranges)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportBackend {
    Stub,
    Rtc,
}

impl FromStr for TransportBackend {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            TRANSPORT_BACKEND_STUB => Ok(Self::Stub),
            TRANSPORT_BACKEND_RTC => Ok(Self::Rtc),
            _ => Err(anyhow!(
                "TRANSPORT_BACKEND must be either `{TRANSPORT_BACKEND_STUB}` or `{TRANSPORT_BACKEND_RTC}`"
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
    pub session_timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub public_ip: IpAddr,
    pub rtc_port_range: RtcPortRange,
    pub rtc_media_worker_count: usize,
    pub transport_backend: TransportBackend,
}

impl Config {
    /// # Errors
    ///
    /// Returns an error when `AUTH_KEY` is missing, `BIND_ADDRESS` is invalid,
    /// `AUTHENTICATION_TIMEOUT_MS` is invalid, `CHANNEL_SIZE` is zero,
    /// `SESSION_TIMEOUT_MS` is invalid, `PING_INTERVAL_MS` is invalid,
    /// `PUBLIC_IP` is invalid, `RTC_MIN_PORT`/`RTC_MAX_PORT` are invalid,
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
        let session_timeout_ms = parse_optional_env(
            &mut get_var,
            "SESSION_TIMEOUT_MS",
            "SESSION_TIMEOUT_MS must be a valid u64",
        )?
        .unwrap_or(DEFAULT_SESSION_TIMEOUT_MS);
        let ping_interval_ms = parse_optional_env(
            &mut get_var,
            "PING_INTERVAL_MS",
            "PING_INTERVAL_MS must be a valid u64",
        )?
        .unwrap_or(DEFAULT_PING_INTERVAL_MS);
        let (public_ip, rtc_port_range, rtc_media_worker_count, transport_backend) =
            load_transport_config(&mut get_var)?;
        ensure!(channel_size > 0, "CHANNEL_SIZE must be greater than zero");
        ensure!(
            session_timeout_ms > 0,
            "SESSION_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            ping_interval_ms > 0,
            "PING_INTERVAL_MS must be greater than zero"
        );
        Ok(Self {
            auth_key,
            bind_address,
            authentication_timeout_ms,
            channel_size,
            session_timeout_ms,
            ping_interval_ms,
            public_ip,
            rtc_port_range,
            rtc_media_worker_count,
            transport_backend,
        })
    }
}

fn load_transport_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<(IpAddr, RtcPortRange, usize, TransportBackend)> {
    let public_ip = parse_optional_env(
        &mut get_var,
        "PUBLIC_IP",
        "PUBLIC_IP must be a valid IP address",
    )?;
    let rtc_min_port = parse_optional_env(
        &mut get_var,
        "RTC_MIN_PORT",
        "RTC_MIN_PORT must be a valid u16",
    )?
    .unwrap_or(DEFAULT_RTC_MIN_PORT);
    let rtc_max_port = parse_optional_env(
        &mut get_var,
        "RTC_MAX_PORT",
        "RTC_MAX_PORT must be a valid u16",
    )?
    .unwrap_or(DEFAULT_RTC_MAX_PORT);
    let rtc_media_worker_count = parse_optional_env(
        &mut get_var,
        "RTC_MEDIA_WORKER_COUNT",
        "RTC_MEDIA_WORKER_COUNT must be a valid usize",
    )?
    .unwrap_or(DEFAULT_RTC_MEDIA_WORKER_COUNT);
    let transport_backend = parse_optional_env(
        &mut get_var,
        "TRANSPORT_BACKEND",
        "TRANSPORT_BACKEND must be either `stub` or `rtc`",
    )?
    .unwrap_or(TransportBackend::Stub);
    ensure!(
        rtc_min_port <= rtc_max_port,
        "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
    );
    ensure!(
        rtc_media_worker_count > 0,
        "RTC_MEDIA_WORKER_COUNT must be greater than zero"
    );
    let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
    ensure!(
        rtc_media_worker_count <= usize::from(rtc_port_range.port_count()),
        "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
    );
    let public_ip = match (transport_backend, public_ip) {
        (_, Some(public_ip)) => public_ip,
        (TransportBackend::Stub, None) => STUB_PUBLIC_IP_DEFAULT,
        (TransportBackend::Rtc, None) => {
            return Err(anyhow!(
                "PUBLIC_IP env variable is required when TRANSPORT_BACKEND=rtc"
            ));
        }
    };
    Ok((
        public_ip,
        rtc_port_range,
        rtc_media_worker_count,
        transport_backend,
    ))
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
    use super::{Config, RtcPortRange, STUB_PUBLIC_IP_DEFAULT, TransportBackend};

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
        assert_eq!(config.session_timeout_ms, 10_000);
        assert_eq!(config.ping_interval_ms, 60_000);
        assert_eq!(config.public_ip, STUB_PUBLIC_IP_DEFAULT);
        assert_eq!(config.rtc_port_range, RtcPortRange::new(40_000, 49_999));
        assert_eq!(config.rtc_media_worker_count, 1);
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
    fn config_rejects_zero_session_timeout() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "SESSION_TIMEOUT_MS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_ping_interval() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PING_INTERVAL_MS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_accepts_rtc_transport_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("203.0.113.10".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.transport_backend, TransportBackend::Rtc);
    }

    #[test]
    fn config_requires_public_ip_for_rtc_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
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

    #[test]
    fn config_rejects_inverted_rtc_port_range() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "RTC_MIN_PORT" => Some("5000".to_owned()),
            "RTC_MAX_PORT" => Some("4000".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_rtc_media_worker_count() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_more_rtc_workers_than_ports() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "RTC_MIN_PORT" => Some("4000".to_owned()),
            "RTC_MAX_PORT" => Some("4001".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("3".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn rtc_port_range_splits_ports_across_workers() {
        let ranges = RtcPortRange::new(40_000, 40_004).split_for_workers(3);
        assert_eq!(
            ranges,
            Some(vec![
                RtcPortRange::new(40_000, 40_001),
                RtcPortRange::new(40_002, 40_003),
                RtcPortRange::new(40_004, 40_004),
            ])
        );
    }
}
