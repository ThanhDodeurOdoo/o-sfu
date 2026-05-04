use std::{net::IpAddr, num::NonZeroUsize};

use anyhow::{Context, Result, anyhow, ensure};
use o_sfu_core::{LocalSpilloverPolicy, RoomShardingPolicy, RtcPortRange, VideoBitrateLimits};

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
    let room_spillover_mode = get_var("ROOM_SPILLOVER_MODE");
    let local_spillover_policy = load_local_spillover_policy(&mut get_var)?;
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
    let room_sharding_policy = room_sharding_policy(
        room_max_local_routers,
        room_spillover_mode.as_deref(),
        local_spillover_policy,
    )?;
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
/// value uses the load-triggered production policy unless
/// `ROOM_SPILLOVER_MODE=bounded` explicitly requests the deterministic topology
/// exercise mode.
fn room_sharding_policy(
    room_max_local_routers: usize,
    mode: Option<&str>,
    local_spillover_policy: LocalSpilloverPolicy,
) -> Result<RoomShardingPolicy> {
    if room_max_local_routers == 1 || mode == Some("strict") {
        return Ok(RoomShardingPolicy::strict_single_router());
    }
    match mode.unwrap_or("load") {
        "load" | "load-triggered" => Ok(RoomShardingPolicy::load_triggered_local_spillover(
            room_max_local_routers,
            local_spillover_policy,
        )),
        "bounded" => Ok(RoomShardingPolicy::bounded_local_spillover(
            room_max_local_routers,
        )),
        other => Err(anyhow!(
            "ROOM_SPILLOVER_MODE must be one of strict, load, load-triggered or bounded, got {other}"
        )),
    }
}

fn load_local_spillover_policy(
    get_var: &mut impl FnMut(&str) -> Option<String>,
) -> Result<LocalSpilloverPolicy> {
    let min_receiver_count = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_MIN_RECEIVERS",
        "ROOM_SPILLOVER_MIN_RECEIVERS must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_MIN_RECEIVER_COUNT);
    let max_active_consumers_per_router = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER",
        "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER);
    let max_fanout_per_source = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE",
        "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_MAX_FANOUT_PER_SOURCE);
    let egress_bitrate_threshold_bps = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_EGRESS_BITRATE_BPS",
        "ROOM_SPILLOVER_EGRESS_BITRATE_BPS must be a valid u64",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_EGRESS_BITRATE_THRESHOLD_BPS);
    let packet_loop_lag_threshold_ms = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_PACKET_LOOP_LAG_MS",
        "ROOM_SPILLOVER_PACKET_LOOP_LAG_MS must be a valid u64",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS);
    let command_backlog_threshold = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_COMMAND_BACKLOG",
        "ROOM_SPILLOVER_COMMAND_BACKLOG must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_COMMAND_BACKLOG_THRESHOLD);
    let relay_mailbox_depth_threshold = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH",
        "ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD);
    let worker_pressure_threshold = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_WORKER_PRESSURE",
        "ROOM_SPILLOVER_WORKER_PRESSURE must be a valid u8",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_WORKER_PRESSURE_THRESHOLD);
    let activation_window = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_ACTIVATION_WINDOW",
        "ROOM_SPILLOVER_ACTIVATION_WINDOW must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_ACTIVATION_WINDOW);
    let cooldown_window = parse_optional_env(
        &mut *get_var,
        "ROOM_SPILLOVER_COOLDOWN_WINDOW",
        "ROOM_SPILLOVER_COOLDOWN_WINDOW must be a valid usize",
    )?
    .unwrap_or(LocalSpilloverPolicy::DEFAULT_COOLDOWN_WINDOW);
    validate_local_spillover_policy(LocalSpilloverConfigValidation {
        min_receiver_count,
        max_active_consumers_per_router,
        max_fanout_per_source,
        worker_pressure_threshold,
        activation_window,
        cooldown_window,
    })?;
    Ok(LocalSpilloverPolicy::conservative()
        .with_min_receiver_count(min_receiver_count)
        .with_max_active_consumers_per_router(max_active_consumers_per_router)
        .with_max_fanout_per_source(max_fanout_per_source)
        .with_egress_bitrate_threshold_bps(egress_bitrate_threshold_bps)
        .with_packet_loop_lag_threshold_ms(packet_loop_lag_threshold_ms)
        .with_command_backlog_threshold(command_backlog_threshold)
        .with_relay_mailbox_depth_threshold(relay_mailbox_depth_threshold)
        .with_worker_pressure_threshold(worker_pressure_threshold)
        .with_activation_window(activation_window)
        .with_cooldown_window(cooldown_window))
}

#[derive(Debug, Clone, Copy)]
struct LocalSpilloverConfigValidation {
    min_receiver_count: usize,
    max_active_consumers_per_router: usize,
    max_fanout_per_source: usize,
    worker_pressure_threshold: u8,
    activation_window: usize,
    cooldown_window: usize,
}

fn validate_local_spillover_policy(input: LocalSpilloverConfigValidation) -> Result<()> {
    ensure!(
        NonZeroUsize::new(input.min_receiver_count).is_some(),
        "ROOM_SPILLOVER_MIN_RECEIVERS must be greater than zero"
    );
    ensure!(
        NonZeroUsize::new(input.max_active_consumers_per_router).is_some(),
        "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER must be greater than zero"
    );
    ensure!(
        NonZeroUsize::new(input.max_fanout_per_source).is_some(),
        "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE must be greater than zero"
    );
    ensure!(
        input.worker_pressure_threshold <= 100,
        "ROOM_SPILLOVER_WORKER_PRESSURE must be less than or equal to 100"
    );
    ensure!(
        NonZeroUsize::new(input.activation_window).is_some(),
        "ROOM_SPILLOVER_ACTIVATION_WINDOW must be greater than zero"
    );
    ensure!(
        NonZeroUsize::new(input.cooldown_window).is_some(),
        "ROOM_SPILLOVER_COOLDOWN_WINDOW must be greater than zero"
    );
    Ok(())
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

    use o_sfu_core::{LocalSpilloverPolicy, RoomSpilloverMode};

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
            "ROOM_SPILLOVER_MIN_RECEIVERS" => Some("8".to_owned()),
            "ROOM_SPILLOVER_ACTIVATION_WINDOW" => Some("1".to_owned()),
            _ => None,
        });

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.room_sharding_policy.max_local_routers(), 2);
        let spillover = config.room_sharding_policy.spillover();
        assert!(matches!(
            spillover,
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_)
        ));
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) = spillover else {
            return;
        };
        assert_eq!(policy.min_receiver_count(), 8);
        assert_eq!(policy.activation_window(), 1);
    }

    #[test]
    fn load_transport_config_accepts_explicit_bounded_spillover_mode() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("3".to_owned()),
            "ROOM_MAX_LOCAL_ROUTERS" => Some("2".to_owned()),
            "ROOM_SPILLOVER_MODE" => Some("bounded".to_owned()),
            _ => None,
        });

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(
            config.room_sharding_policy.spillover(),
            RoomSpilloverMode::BoundedLocalSpillover
        );
    }

    #[test]
    fn load_transport_config_rejects_invalid_spillover_mode() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" | "ROOM_MAX_LOCAL_ROUTERS" => Some("2".to_owned()),
            "ROOM_SPILLOVER_MODE" => Some("invalid".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_rejects_zero_spillover_activation_window() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" | "ROOM_MAX_LOCAL_ROUTERS" => Some("2".to_owned()),
            "ROOM_SPILLOVER_ACTIVATION_WINDOW" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_load_policy_defaults_are_conservative() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" | "ROOM_MAX_LOCAL_ROUTERS" => Some("2".to_owned()),
            _ => None,
        });

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        let spillover = config.room_sharding_policy.spillover();
        assert!(matches!(
            spillover,
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_)
        ));
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) = spillover else {
            return;
        };
        assert_eq!(policy, LocalSpilloverPolicy::conservative());
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
