use std::{net::IpAddr, num::NonZeroUsize};

use anyhow::{Context, Result, anyhow, ensure};
use o_sfu_core::RtcPortRange;

use super::parsing::parse_optional_env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LoadedTransportConfig {
    pub(super) public_ip: IpAddr,
    pub(super) max_bitrate_in_bps: u64,
    pub(super) max_bitrate_out_bps: u64,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) rtc_media_worker_count: usize,
}

pub(super) fn load_transport_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<LoadedTransportConfig> {
    if get_var("TRANSPORT_BACKEND").is_some() {
        return Err(anyhow!(
            "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport"
        ));
    }
    let public_ip: IpAddr = get_var("PUBLIC_IP")
        .context("PUBLIC_IP env variable is required")?
        .parse()
        .context("PUBLIC_IP must be a valid IP address")?;
    let rtc_min_port = parse_optional_env(
        &mut get_var,
        "RTC_MIN_PORT",
        "RTC_MIN_PORT must be a valid u16",
    )?
    .unwrap_or(40_000);
    let max_bitrate_in_bps = parse_optional_env(
        &mut get_var,
        "MAX_BITRATE_IN",
        "MAX_BITRATE_IN must be a valid u64",
    )?
    .unwrap_or(8_000_000);
    let max_bitrate_out_bps = parse_optional_env(
        &mut get_var,
        "MAX_BITRATE_OUT",
        "MAX_BITRATE_OUT must be a valid u64",
    )?
    .unwrap_or(10_000_000);
    let rtc_max_port = parse_optional_env(
        &mut get_var,
        "RTC_MAX_PORT",
        "RTC_MAX_PORT must be a valid u16",
    )?
    .unwrap_or(49_999);
    let rtc_media_worker_count = parse_optional_env(
        &mut get_var,
        "RTC_MEDIA_WORKER_COUNT",
        "RTC_MEDIA_WORKER_COUNT must be a valid usize",
    )?
    .unwrap_or(1);
    ensure!(
        rtc_min_port <= rtc_max_port,
        "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
    );
    ensure!(
        NonZeroUsize::new(rtc_media_worker_count).is_some(),
        "RTC_MEDIA_WORKER_COUNT must be greater than zero"
    );
    ensure!(
        max_bitrate_in_bps > 0,
        "MAX_BITRATE_IN must be greater than zero"
    );
    ensure!(
        max_bitrate_out_bps > 0,
        "MAX_BITRATE_OUT must be greater than zero"
    );
    let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
    ensure!(
        rtc_media_worker_count <= usize::from(rtc_port_range.port_count()),
        "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
    );
    ensure!(
        !public_ip.is_unspecified(),
        "PUBLIC_IP must be a concrete advertised address"
    );
    ensure!(
        !public_ip.is_multicast(),
        "PUBLIC_IP cannot be a multicast address"
    );
    Ok(LoadedTransportConfig {
        public_ip,
        max_bitrate_in_bps,
        max_bitrate_out_bps,
        rtc_port_range,
        rtc_media_worker_count,
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::{LoadedTransportConfig, RtcPortRange, load_transport_config};

    #[test]
    fn load_transport_config_accepts_public_ip_and_defaults() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("203.0.113.10".to_owned()),
            _ => None,
        });
        assert_eq!(
            config.ok(),
            Some(LoadedTransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
                max_bitrate_in_bps: 8_000_000,
                max_bitrate_out_bps: 10_000_000,
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                rtc_media_worker_count: 1,
            })
        );
    }

    #[test]
    fn load_transport_config_accepts_explicit_bitrate_limits() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_BITRATE_IN" => Some("1234567".to_owned()),
            "MAX_BITRATE_OUT" => Some("7654321".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.max_bitrate_in_bps, 1_234_567);
        assert_eq!(config.max_bitrate_out_bps, 7_654_321);
    }

    #[test]
    fn load_transport_config_requires_public_ip() {
        let config = load_transport_config(|_| None);
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_removed_transport_backend_env() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_unspecified_public_ip() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("0.0.0.0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_multicast_public_ip() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("239.1.1.1".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_inverted_rtc_port_range() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MIN_PORT" => Some("5000".to_owned()),
            "RTC_MAX_PORT" => Some("4000".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_zero_rtc_media_worker_count() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_zero_max_bitrate_in() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_BITRATE_IN" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_zero_max_bitrate_out() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_BITRATE_OUT" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_more_rtc_workers_than_ports() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
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
