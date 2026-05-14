use std::{net::IpAddr, num::NonZeroUsize};

use anyhow::{Context, Result, anyhow, ensure};
use o_sfu_core::{
    Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    RoomWorkerPolicy, RtcPortRange, VideoBitrateLimits,
};

use super::{TransportConfig, parsing::parse_env_or_default};

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
    let rtc_min_port = parse_env_or_default(&mut get_var, "RTC_MIN_PORT", "u16", 40_000)?;
    let max_bitrate_in_bps =
        parse_env_or_default(&mut get_var, "MAX_BITRATE_IN", "u64", 8_000_000)?;
    let max_bitrate_out_bps =
        parse_env_or_default(&mut get_var, "MAX_BITRATE_OUT", "u64", 10_000_000)?;
    let max_video_bitrate_bps = parse_env_or_default(
        &mut get_var,
        "MAX_VIDEO_BITRATE",
        "u64",
        VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE.as_bps(),
    )?;
    let rtc_max_port = parse_env_or_default(&mut get_var, "RTC_MAX_PORT", "u16", 49_999)?;
    let rtc_media_worker_count =
        parse_env_or_default(&mut get_var, "RTC_MEDIA_WORKER_COUNT", "usize", 1)?;
    let room_max_local_routers =
        parse_env_or_default(&mut get_var, "ROOM_MAX_LOCAL_ROUTERS", "usize", 1)?;
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
    let room_worker_policy = room_worker_policy(
        room_max_local_routers,
        room_spillover_mode.as_deref(),
        local_spillover_policy,
    )?;
    Ok(TransportConfig {
        public_ip,
        max_bitrate_in: Bitrate::from_bps(max_bitrate_in_bps),
        max_bitrate_out: Bitrate::from_bps(max_bitrate_out_bps),
        video_bitrate_limits: VideoBitrateLimits::new(Bitrate::from_bps(max_video_bitrate_bps)),
        rtc_port_range,
        rtc_media_worker_count,
        room_worker_policy,
    })
}

/// Translate the operator local-router cap into the core room policy.
///
/// `ROOM_MAX_LOCAL_ROUTERS=1` keeps the strict historical topology. Any larger
/// value uses the load-triggered production policy unless
/// `ROOM_SPILLOVER_MODE=bounded` explicitly requests the deterministic topology
/// exercise mode.
fn room_worker_policy(
    room_max_local_routers: usize,
    mode: Option<&str>,
    local_spillover_policy: LocalSpilloverPolicy,
) -> Result<RoomWorkerPolicy> {
    if room_max_local_routers == 1 || mode == Some("strict") {
        return Ok(RoomWorkerPolicy::strict_single_router());
    }
    match mode.unwrap_or("load") {
        "load" | "load-triggered" => Ok(RoomWorkerPolicy::load_triggered_local_spillover(
            room_max_local_routers,
            local_spillover_policy,
        )),
        "bounded" => Ok(RoomWorkerPolicy::bounded_local_spillover(
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
    let parts = LocalSpilloverPolicyParts {
        min_receiver_count: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_MIN_RECEIVERS",
            "usize",
            LocalSpilloverPolicy::DEFAULT_MIN_RECEIVER_COUNT,
        )?,
        max_active_consumers_per_router: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER",
            "usize",
            LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER,
        )?,
        max_fanout_per_source: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE",
            "usize",
            LocalSpilloverPolicy::DEFAULT_MAX_FANOUT_PER_SOURCE,
        )?,
        egress_bitrate_threshold: Bitrate::from_bps(parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_EGRESS_BITRATE_BPS",
            "u64",
            LocalSpilloverPolicy::DEFAULT_EGRESS_BITRATE_THRESHOLD.as_bps(),
        )?),
        packet_loop_lag_threshold_ms: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_PACKET_LOOP_LAG_MS",
            "u64",
            LocalSpilloverPolicy::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS,
        )?,
        command_backlog_threshold: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_COMMAND_BACKLOG",
            "usize",
            LocalSpilloverPolicy::DEFAULT_COMMAND_BACKLOG_THRESHOLD,
        )?,
        relay_mailbox_depth_threshold: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH",
            "usize",
            LocalSpilloverPolicy::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD,
        )?,
        worker_pressure_threshold: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_WORKER_PRESSURE",
            "u8",
            LocalSpilloverPolicy::DEFAULT_WORKER_PRESSURE_THRESHOLD,
        )?,
        activation_window: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_ACTIVATION_WINDOW",
            "usize",
            LocalSpilloverPolicy::DEFAULT_ACTIVATION_WINDOW,
        )?,
        cooldown_window: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_COOLDOWN_WINDOW",
            "usize",
            LocalSpilloverPolicy::DEFAULT_COOLDOWN_WINDOW,
        )?,
    };
    LocalSpilloverPolicy::try_new(parts).map_err(local_spillover_policy_error)
}

fn local_spillover_policy_error(error: LocalSpilloverPolicyError) -> anyhow::Error {
    match error {
        LocalSpilloverPolicyError::MinReceiverCountZero => {
            anyhow!("ROOM_SPILLOVER_MIN_RECEIVERS must be greater than zero")
        }
        LocalSpilloverPolicyError::MaxActiveConsumersPerRouterZero => {
            anyhow!("ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER must be greater than zero")
        }
        LocalSpilloverPolicyError::MaxFanoutPerSourceZero => {
            anyhow!("ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE must be greater than zero")
        }
        LocalSpilloverPolicyError::WorkerPressureThresholdTooHigh => {
            anyhow!("ROOM_SPILLOVER_WORKER_PRESSURE must be less than or equal to 100")
        }
        LocalSpilloverPolicyError::ActivationWindowZero => {
            anyhow!("ROOM_SPILLOVER_ACTIVATION_WINDOW must be greater than zero")
        }
        LocalSpilloverPolicyError::CooldownWindowZero => {
            anyhow!("ROOM_SPILLOVER_COOLDOWN_WINDOW must be greater than zero")
        }
    }
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
    ensure_non_zero_usize_env(input.rtc_media_worker_count, "RTC_MEDIA_WORKER_COUNT")?;
    ensure_non_zero_usize_env(input.room_max_local_routers, "ROOM_MAX_LOCAL_ROUTERS")?;
    ensure_positive_u64_env(input.max_bitrate_in_bps, "MAX_BITRATE_IN")?;
    ensure_positive_u64_env(input.max_bitrate_out_bps, "MAX_BITRATE_OUT")?;
    ensure_positive_u64_env(input.max_video_bitrate_bps, "MAX_VIDEO_BITRATE")?;
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

fn ensure_non_zero_usize_env(value: usize, key: &str) -> Result<()> {
    ensure!(
        NonZeroUsize::new(value).is_some(),
        "{key} must be greater than zero"
    );
    Ok(())
}

fn ensure_positive_u64_env(value: u64, key: &str) -> Result<()> {
    ensure!(value > 0, "{key} must be greater than zero");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use anyhow::Result;
    use o_sfu_core::{LocalSpilloverPolicy, RoomSpilloverMode};

    use super::{
        Bitrate, RoomWorkerPolicy, RtcPortRange, TransportConfig, VideoBitrateLimits,
        load_transport_config,
    };

    fn load_transport_config_with_defaults(overrides: &[(&str, &str)]) -> Result<TransportConfig> {
        load_transport_config(|key| {
            overrides
                .iter()
                .find_map(|(name, value)| (*name == key).then(|| (*value).to_owned()))
                .or_else(|| match key {
                    "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
                    _ => None,
                })
        })
    }

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
                max_bitrate_in: Bitrate::from_mbps(8),
                max_bitrate_out: Bitrate::from_mbps(10),
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                rtc_media_worker_count: 1,
                room_worker_policy: RoomWorkerPolicy::strict_single_router(),
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
        assert_eq!(config.max_bitrate_in, Bitrate::from_bps(1_234_567));
        assert_eq!(config.max_bitrate_out, Bitrate::from_bps(7_654_321));
        assert_eq!(
            config.video_bitrate_limits,
            VideoBitrateLimits::new(Bitrate::from_bps(2_345_678))
        );
    }

    #[test]
    fn load_transport_config_requires_public_ip() {
        let config = load_transport_config(|_| None);
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_accepts_room_spillover_policy() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("3".to_owned()),
            "ROOM_MAX_LOCAL_ROUTERS" => Some("2".to_owned()),
            "ROOM_SPILLOVER_MIN_RECEIVERS" => Some("8".to_owned()),
            "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER" => Some("9".to_owned()),
            "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE" => Some("10".to_owned()),
            "ROOM_SPILLOVER_EGRESS_BITRATE_BPS" => Some("1200".to_owned()),
            "ROOM_SPILLOVER_PACKET_LOOP_LAG_MS" => Some("7".to_owned()),
            "ROOM_SPILLOVER_COMMAND_BACKLOG" => Some("11".to_owned()),
            "ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH" => Some("12".to_owned()),
            "ROOM_SPILLOVER_WORKER_PRESSURE" => Some("50".to_owned()),
            "ROOM_SPILLOVER_ACTIVATION_WINDOW" => Some("1".to_owned()),
            "ROOM_SPILLOVER_COOLDOWN_WINDOW" => Some("4".to_owned()),
            _ => None,
        });

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.room_worker_policy.max_local_routers(), 2);
        let spillover = config.room_worker_policy.spillover();
        assert!(matches!(
            spillover,
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_)
        ));
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) = spillover else {
            return;
        };
        assert_eq!(policy.min_receiver_count(), 8);
        assert_eq!(policy.max_active_consumers_per_router(), 9);
        assert_eq!(policy.max_fanout_per_source(), 10);
        assert_eq!(policy.egress_bitrate_threshold(), Bitrate::from_bps(1_200));
        assert_eq!(policy.packet_loop_lag_threshold_ms(), 7);
        assert_eq!(policy.command_backlog_threshold(), 11);
        assert_eq!(policy.relay_mailbox_depth_threshold(), 12);
        assert_eq!(policy.worker_pressure_threshold(), 50);
        assert_eq!(policy.activation_window(), 1);
        assert_eq!(policy.cooldown_window(), 4);
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
            config.room_worker_policy.spillover(),
            RoomSpilloverMode::BoundedLocalSpillover
        );
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
        let spillover = config.room_worker_policy.spillover();
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
            config.room_worker_policy,
            RoomWorkerPolicy::strict_single_router()
        );
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

    #[test]
    fn load_transport_config_rejects_invalid_values() {
        let cases: &[(&str, &[(&str, &str)])] = &[
            ("removed transport backend", &[("TRANSPORT_BACKEND", "rtc")]),
            ("unspecified public IP", &[("PUBLIC_IP", "0.0.0.0")]),
            ("multicast public IP", &[("PUBLIC_IP", "239.1.1.1")]),
            (
                "inverted RTC port range",
                &[("RTC_MIN_PORT", "5000"), ("RTC_MAX_PORT", "4000")],
            ),
            (
                "zero RTC media worker count",
                &[("RTC_MEDIA_WORKER_COUNT", "0")],
            ),
            (
                "invalid spillover mode",
                &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_MODE", "invalid"),
                ],
            ),
            (
                "zero spillover activation window",
                &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_ACTIVATION_WINDOW", "0"),
                ],
            ),
            (
                "zero spillover receiver threshold",
                &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_MIN_RECEIVERS", "0"),
                ],
            ),
            (
                "out of range spillover worker pressure",
                &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_WORKER_PRESSURE", "101"),
                ],
            ),
            (
                "zero room router cap",
                &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "0"),
                ],
            ),
            (
                "more room routers than RTC workers",
                &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "3"),
                ],
            ),
            ("zero max incoming bitrate", &[("MAX_BITRATE_IN", "0")]),
            ("zero max outgoing bitrate", &[("MAX_BITRATE_OUT", "0")]),
            ("zero max video bitrate", &[("MAX_VIDEO_BITRATE", "0")]),
            (
                "more RTC workers than ports",
                &[
                    ("RTC_MIN_PORT", "4000"),
                    ("RTC_MAX_PORT", "4001"),
                    ("RTC_MEDIA_WORKER_COUNT", "3"),
                ],
            ),
        ];

        for (name, overrides) in cases {
            let config = load_transport_config_with_defaults(overrides);
            assert!(config.is_err(), "{name}");
        }
    }

    #[test]
    fn load_transport_config_preserves_numeric_parse_errors() {
        let cases = [
            ("RTC_MIN_PORT", "abc", "RTC_MIN_PORT must be a valid u16"),
            (
                "MAX_BITRATE_IN",
                "abc",
                "MAX_BITRATE_IN must be a valid u64",
            ),
            (
                "ROOM_MAX_LOCAL_ROUTERS",
                "abc",
                "ROOM_MAX_LOCAL_ROUTERS must be a valid usize",
            ),
            (
                "ROOM_SPILLOVER_WORKER_PRESSURE",
                "abc",
                "ROOM_SPILLOVER_WORKER_PRESSURE must be a valid u8",
            ),
        ];

        for (key, value, message) in cases {
            let error = load_transport_config_with_defaults(&[(key, value)])
                .err()
                .map(|error| error.to_string());
            assert_eq!(error.as_deref(), Some(message), "{key}");
        }
    }
}
