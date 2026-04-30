use std::{net::IpAddr, num::NonZeroUsize};

use anyhow::{Context, Result, anyhow, ensure};
use o_sfu_core::{RoomShardingPolicy, RtcPortRange, VideoBitrateLimits};

use super::{TransportConfig, parsing::parse_optional_env};

pub(super) fn load_transport_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<TransportConfig> {
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
    let max_video_bitrate_bps = parse_optional_env(
        &mut get_var,
        "MAX_VIDEO_BITRATE",
        "MAX_VIDEO_BITRATE must be a valid u64",
    )?
    .unwrap_or(VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE_BPS);
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
    let room_max_local_routers = parse_optional_env(
        &mut get_var,
        "ROOM_MAX_LOCAL_ROUTERS",
        "ROOM_MAX_LOCAL_ROUTERS must be a valid usize",
    )?
    .unwrap_or(1);
    let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
    validate_transport_config(TransportConfigValidation {
        public_ip,
        rtc_min_port,
        rtc_max_port,
        rtc_media_worker_count,
        room_max_local_routers,
        max_bitrate_in_bps,
        max_bitrate_out_bps,
        max_video_bitrate_bps,
        rtc_port_range,
    })?;
    let room_sharding_policy = room_sharding_policy(room_max_local_routers);
    Ok(TransportConfig {
        public_ip,
        max_bitrate_in_bps,
        max_bitrate_out_bps,
        video_bitrate_limits: VideoBitrateLimits::new(max_video_bitrate_bps),
        rtc_port_range,
        rtc_media_worker_count,
        room_sharding_policy,
    })
}

/// Translate the operator local-router cap into the core room policy.
///
/// `ROOM_MAX_LOCAL_ROUTERS=1` keeps the strict historical topology. Any larger
/// value opts rooms into bounded deterministic local spillover, with the numeric
/// value kept as the per-room local router cap.
fn room_sharding_policy(room_max_local_routers: usize) -> RoomShardingPolicy {
    if room_max_local_routers == 1 {
        return RoomShardingPolicy::strict_single_router();
    }
    RoomShardingPolicy::bounded_local_spillover(room_max_local_routers)
}

#[derive(Debug, Clone, Copy)]
struct TransportConfigValidation {
    public_ip: IpAddr,
    rtc_min_port: u16,
    rtc_max_port: u16,
    rtc_media_worker_count: usize,
    /// Maximum number of local router placements a room may reserve.
    ///
    /// This must not exceed the worker count because each placement needs one
    /// concrete media-worker owner for transport session keys.
    room_max_local_routers: usize,
    max_bitrate_in_bps: u64,
    max_bitrate_out_bps: u64,
    max_video_bitrate_bps: u64,
    rtc_port_range: RtcPortRange,
}

fn validate_transport_config(input: TransportConfigValidation) -> Result<()> {
    ensure!(
        input.rtc_min_port <= input.rtc_max_port,
        "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
    );
    ensure!(
        NonZeroUsize::new(input.rtc_media_worker_count).is_some(),
        "RTC_MEDIA_WORKER_COUNT must be greater than zero"
    );
    ensure!(
        NonZeroUsize::new(input.room_max_local_routers).is_some(),
        "ROOM_MAX_LOCAL_ROUTERS must be greater than zero"
    );
    ensure!(
        input.max_bitrate_in_bps > 0,
        "MAX_BITRATE_IN must be greater than zero"
    );
    ensure!(
        input.max_bitrate_out_bps > 0,
        "MAX_BITRATE_OUT must be greater than zero"
    );
    ensure!(
        input.max_video_bitrate_bps > 0,
        "MAX_VIDEO_BITRATE must be greater than zero"
    );
    ensure!(
        input.rtc_media_worker_count <= usize::from(input.rtc_port_range.port_count()),
        "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
    );
    ensure!(
        input.room_max_local_routers <= input.rtc_media_worker_count,
        "ROOM_MAX_LOCAL_ROUTERS must be less than or equal to RTC_MEDIA_WORKER_COUNT"
    );
    ensure!(
        !input.public_ip.is_unspecified(),
        "PUBLIC_IP must be a concrete advertised address"
    );
    ensure!(
        !input.public_ip.is_multicast(),
        "PUBLIC_IP cannot be a multicast address"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use o_sfu_core::RoomSpilloverMode;

    use super::{
        RoomShardingPolicy, RtcPortRange, TransportConfig, VideoBitrateLimits,
        load_transport_config,
    };

    #[test]
    fn load_transport_config_accepts_public_ip_and_defaults() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("203.0.113.10".to_owned()),
            _ => None,
        });
        assert_eq!(
            config.ok(),
            Some(TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
                max_bitrate_in_bps: 8_000_000,
                max_bitrate_out_bps: 10_000_000,
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                rtc_media_worker_count: 1,
                room_sharding_policy: RoomShardingPolicy::strict_single_router(),
            })
        );
    }

    #[test]
    fn load_transport_config_accepts_explicit_bitrate_limits() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_BITRATE_IN" => Some("1234567".to_owned()),
            "MAX_BITRATE_OUT" => Some("7654321".to_owned()),
            "MAX_VIDEO_BITRATE" => Some("2345678".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.max_bitrate_in_bps, 1_234_567);
        assert_eq!(config.max_bitrate_out_bps, 7_654_321);
        assert_eq!(
            config.video_bitrate_limits,
            VideoBitrateLimits::new(2_345_678)
        );
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
    fn load_transport_config_accepts_room_spillover_policy() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("3".to_owned()),
            "ROOM_MAX_LOCAL_ROUTERS" => Some("2".to_owned()),
            _ => None,
        });

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.room_sharding_policy.max_local_routers(), 2);
        assert_eq!(
            config.room_sharding_policy.spillover(),
            RoomSpilloverMode::BoundedLocalSpillover
        );
    }

    #[test]
    fn load_transport_config_rejects_zero_room_router_cap() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("2".to_owned()),
            "ROOM_MAX_LOCAL_ROUTERS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_keeps_explicit_single_router_strict() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("2".to_owned()),
            "ROOM_MAX_LOCAL_ROUTERS" => Some("1".to_owned()),
            _ => None,
        });

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(
            config.room_sharding_policy,
            RoomShardingPolicy::strict_single_router()
        );
    }

    #[test]
    fn load_transport_config_rejects_more_room_routers_than_rtc_workers() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("2".to_owned()),
            "ROOM_MAX_LOCAL_ROUTERS" => Some("3".to_owned()),
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
    fn load_transport_config_rejects_zero_max_video_bitrate() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "MAX_VIDEO_BITRATE" => Some("0".to_owned()),
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
