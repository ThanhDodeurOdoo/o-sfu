use std::{net::IpAddr, num::NonZeroUsize, thread};

use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{
    Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, VideoBitrateLimits,
};

use super::{
    TransportConfig,
    env::{env_block, positive},
};

pub(super) fn load_transport_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<TransportConfig> {
    if get_var("TRANSPORT_BACKEND").is_some() {
        return Err(anyhow!(
            "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport"
        ));
    }
    let env = TransportEnv::load(&mut get_var)?;
    let local_spillover_policy = LocalSpilloverEnv::load(&mut get_var)?.into_policy()?;
    let media_env = RoomMediaLimitsEnv::load(&mut get_var)?;
    let room_media_limits = RoomMediaLimits::try_new(
        media_env.active_audio_speakers,
        media_env.video_downloads_per_receiver,
    )?;
    let rtc_port_range = RtcPortRange::new(env.rtc_min_port, env.rtc_max_port);
    validate_transport_config(&env, rtc_port_range)?;
    let room_worker_policy = room_worker_policy(
        env.room_max_local_routers,
        env.room_spillover_mode.as_deref(),
        local_spillover_policy,
    )?;
    Ok(TransportConfig {
        public_ip: env.public_ip,
        max_bitrate_in: Bitrate::from_bps(env.max_bitrate_in_bps),
        max_bitrate_out: Bitrate::from_bps(env.max_bitrate_out_bps),
        video_bitrate_limits: VideoBitrateLimits::new(Bitrate::from_bps(env.max_video_bitrate_bps)),
        rtc_port_range,
        rtc_media_worker_count: env.rtc_media_worker_count,
        room_worker_policy,
        room_media_limits,
    })
}

env_block! {
    struct TransportEnv {
        public_ip: IpAddr = required("PUBLIC_IP");
        rtc_min_port: u16 = default("RTC_MIN_PORT", 40_000);
        max_bitrate_in_bps: u64 = default("MAX_BITRATE_IN", 8_000_000).check(positive);
        max_bitrate_out_bps: u64 = default("MAX_BITRATE_OUT", 10_000_000).check(positive);
        max_video_bitrate_bps: u64 = default(
            "MAX_VIDEO_BITRATE",
            VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE.as_bps()
        ).check(positive);
        rtc_max_port: u16 = default("RTC_MAX_PORT", 49_999);
        rtc_media_worker_count: usize = default(
            "RTC_MEDIA_WORKER_COUNT",
            default_rtc_media_worker_count()
        ).check(positive);
        room_max_local_routers: usize = default("ROOM_MAX_LOCAL_ROUTERS", 1).check(positive);
        room_spillover_mode: Option<String> = optional("ROOM_SPILLOVER_MODE");
    }
}

env_block! {
    struct LocalSpilloverEnv {
        min_receivers: usize = default(
            "ROOM_SPILLOVER_MIN_RECEIVERS",
            LocalSpilloverPolicy::DEFAULT_MIN_RECEIVER_COUNT
        ).check(positive);
        max_consumers_per_router: usize = default(
            "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER",
            LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER
        ).check(positive);
        max_fanout_per_source: usize = default(
            "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE",
            LocalSpilloverPolicy::DEFAULT_MAX_FANOUT_PER_SOURCE
        ).check(positive);
        egress_bitrate_bps: u64 = default(
            "ROOM_SPILLOVER_EGRESS_BITRATE_BPS",
            LocalSpilloverPolicy::DEFAULT_EGRESS_BITRATE_THRESHOLD.as_bps()
        );
        packet_loop_lag_ms: u64 = default(
            "ROOM_SPILLOVER_PACKET_LOOP_LAG_MS",
            LocalSpilloverPolicy::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS
        );
        command_backlog: usize = default(
            "ROOM_SPILLOVER_COMMAND_BACKLOG",
            LocalSpilloverPolicy::DEFAULT_COMMAND_BACKLOG_THRESHOLD
        );
        relay_mailbox_depth: usize = default(
            "ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH",
            LocalSpilloverPolicy::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD
        );
        worker_pressure: u8 = default(
            "ROOM_SPILLOVER_WORKER_PRESSURE",
            LocalSpilloverPolicy::DEFAULT_WORKER_PRESSURE_THRESHOLD
        );
        activation_window: usize = default(
            "ROOM_SPILLOVER_ACTIVATION_WINDOW",
            LocalSpilloverPolicy::DEFAULT_ACTIVATION_WINDOW
        ).check(positive);
        cooldown_window: usize = default(
            "ROOM_SPILLOVER_COOLDOWN_WINDOW",
            LocalSpilloverPolicy::DEFAULT_COOLDOWN_WINDOW
        ).check(positive);
    }
}

env_block! {
    struct RoomMediaLimitsEnv {
        active_audio_speakers: usize = default(
            "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS",
            RoomMediaLimits::DEFAULT_MAX_ACTIVE_AUDIO_SPEAKERS
        ).check(positive);
        video_downloads_per_receiver: usize = default(
            "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER",
            RoomMediaLimits::DEFAULT_MAX_VIDEO_DOWNLOADS_PER_RECEIVER
        ).check(positive);
    }
}

impl LocalSpilloverEnv {
    fn into_policy(self) -> Result<LocalSpilloverPolicy> {
        let parts = LocalSpilloverPolicyParts {
            min_receiver_count: self.min_receivers,
            max_active_consumers_per_router: self.max_consumers_per_router,
            max_fanout_per_source: self.max_fanout_per_source,
            egress_bitrate_threshold: Bitrate::from_bps(self.egress_bitrate_bps),
            packet_loop_lag_threshold_ms: self.packet_loop_lag_ms,
            command_backlog_threshold: self.command_backlog,
            relay_mailbox_depth_threshold: self.relay_mailbox_depth,
            worker_pressure_threshold: self.worker_pressure,
            activation_window: self.activation_window,
            cooldown_window: self.cooldown_window,
        };
        LocalSpilloverPolicy::try_new(parts).map_err(local_spillover_policy_error)
    }
}

pub(in crate::config) fn default_rtc_media_worker_count() -> usize {
    thread::available_parallelism().map_or(1, NonZeroUsize::get)
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
    match mode {
        _ if room_max_local_routers == 1 => Ok(RoomWorkerPolicy::strict_single_router()),
        None | Some("load" | "load-triggered") => {
            Ok(RoomWorkerPolicy::load_triggered_local_spillover(
                room_max_local_routers,
                local_spillover_policy,
            ))
        }
        Some("bounded") => Ok(RoomWorkerPolicy::bounded_local_spillover(
            room_max_local_routers,
        )),
        Some("strict") => Ok(RoomWorkerPolicy::strict_single_router()),
        Some(other) => Err(anyhow!(
            "ROOM_SPILLOVER_MODE must be one of strict, load, load-triggered or bounded, got {other}"
        )),
    }
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

fn validate_transport_config(env: &TransportEnv, rtc_port_range: RtcPortRange) -> Result<()> {
    ensure!(
        env.rtc_min_port <= env.rtc_max_port,
        "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
    );
    ensure!(
        env.rtc_media_worker_count <= usize::from(rtc_port_range.port_count()),
        "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
    );
    ensure!(
        env.room_max_local_routers <= env.rtc_media_worker_count,
        "ROOM_MAX_LOCAL_ROUTERS must be less than or equal to RTC_MEDIA_WORKER_COUNT"
    );
    ensure!(
        !env.public_ip.is_unspecified(),
        "PUBLIC_IP must be a concrete advertised address"
    );
    ensure!(
        !env.public_ip.is_multicast(),
        "PUBLIC_IP cannot be a multicast address"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use anyhow::Result;
    use o_sfu_core::prelude::{LocalSpilloverPolicy, RoomSpilloverMode};

    use super::{
        Bitrate, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, TransportConfig,
        VideoBitrateLimits, default_rtc_media_worker_count, load_transport_config,
    };

    fn load_transport_config_with_defaults(overrides: &[(&str, &str)]) -> Result<TransportConfig> {
        load_transport_config(|key| {
            overrides
                .iter()
                .find(|(name, _value)| *name == key)
                .map(|(_name, value)| (*value).to_owned())
                .or_else(|| match key {
                    "PUBLIC_IP" => Some("127.0.0.1".to_owned()),
                    _ => None,
                })
        })
    }

    struct InvalidTransportCase<'a> {
        name: &'a str,
        overrides: &'a [(&'a str, &'a str)],
        message: &'a str,
    }

    fn assert_invalid_transport_cases(cases: &[InvalidTransportCase<'_>]) {
        for case in cases {
            let error = load_transport_config_with_defaults(case.overrides)
                .err()
                .map(|error| error.to_string());
            assert_eq!(error.as_deref(), Some(case.message), "{}", case.name);
        }
    }

    #[test]
    fn load_transport_config_accepts_public_ip_and_defaults() {
        let config = load_transport_config(|key| match key {
            "PUBLIC_IP" => Some("203.0.113.10".to_owned()),
            _ => None,
        });
        let worker_count = default_rtc_media_worker_count();
        assert_eq!(
            config.ok(),
            Some(TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
                max_bitrate_in: Bitrate::from_mbps(8),
                max_bitrate_out: Bitrate::from_mbps(10),
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(40_000, 49_999),
                rtc_media_worker_count: worker_count,
                room_worker_policy: RoomWorkerPolicy::strict_single_router(),
                room_media_limits: RoomMediaLimits::default(),
            })
        );
    }

    #[test]
    fn load_transport_config_accepts_explicit_bitrate_limits() -> Result<()> {
        let config = load_transport_config_with_defaults(&[
            ("MAX_BITRATE_IN", "1234567"),
            ("MAX_BITRATE_OUT", "7654321"),
            ("MAX_VIDEO_BITRATE", "2345678"),
        ])?;
        assert_eq!(config.max_bitrate_in, Bitrate::from_bps(1_234_567));
        assert_eq!(config.max_bitrate_out, Bitrate::from_bps(7_654_321));
        assert_eq!(
            config.video_bitrate_limits,
            VideoBitrateLimits::new(Bitrate::from_bps(2_345_678))
        );
        Ok(())
    }

    #[test]
    fn load_transport_config_requires_public_ip() {
        let config = load_transport_config(|_| None);
        assert!(config.is_err());
    }

    #[test]
    fn load_transport_config_accepts_room_spillover_policy() -> Result<()> {
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "3"),
            ("ROOM_MAX_LOCAL_ROUTERS", "2"),
            ("ROOM_SPILLOVER_MIN_RECEIVERS", "8"),
            ("ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER", "9"),
            ("ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE", "10"),
            ("ROOM_SPILLOVER_EGRESS_BITRATE_BPS", "1200"),
            ("ROOM_SPILLOVER_PACKET_LOOP_LAG_MS", "7"),
            ("ROOM_SPILLOVER_COMMAND_BACKLOG", "11"),
            ("ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH", "12"),
            ("ROOM_SPILLOVER_WORKER_PRESSURE", "50"),
            ("ROOM_SPILLOVER_ACTIVATION_WINDOW", "1"),
            ("ROOM_SPILLOVER_COOLDOWN_WINDOW", "4"),
        ])?;

        assert_eq!(config.room_worker_policy.max_local_routers(), 2);
        let spillover = config.room_worker_policy.spillover();
        assert!(matches!(
            spillover,
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_)
        ));
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) = spillover else {
            anyhow::bail!("spillover policy should be load triggered");
        };
        let policy = policy.parts();
        assert_eq!(policy.min_receiver_count, 8);
        assert_eq!(policy.max_active_consumers_per_router, 9);
        assert_eq!(policy.max_fanout_per_source, 10);
        assert_eq!(policy.egress_bitrate_threshold, Bitrate::from_bps(1_200));
        assert_eq!(policy.packet_loop_lag_threshold_ms, 7);
        assert_eq!(policy.command_backlog_threshold, 11);
        assert_eq!(policy.relay_mailbox_depth_threshold, 12);
        assert_eq!(policy.worker_pressure_threshold, 50);
        assert_eq!(policy.activation_window, 1);
        assert_eq!(policy.cooldown_window, 4);
        Ok(())
    }

    #[test]
    fn load_transport_config_accepts_room_media_limits() -> Result<()> {
        let config = load_transport_config_with_defaults(&[
            ("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "3"),
            ("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "8"),
        ])?;

        assert_eq!(config.room_media_limits.max_active_audio_speakers(), 3);
        assert_eq!(
            config.room_media_limits.max_video_downloads_per_receiver(),
            8
        );
        Ok(())
    }

    #[test]
    fn load_transport_config_accepts_explicit_bounded_spillover_mode() -> Result<()> {
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "3"),
            ("ROOM_MAX_LOCAL_ROUTERS", "2"),
            ("ROOM_SPILLOVER_MODE", "bounded"),
        ])?;

        assert_eq!(
            config.room_worker_policy.spillover(),
            RoomSpilloverMode::BoundedLocalSpillover
        );
        Ok(())
    }

    #[test]
    fn load_transport_config_load_policy_defaults_are_conservative() -> Result<()> {
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "2"),
            ("ROOM_MAX_LOCAL_ROUTERS", "2"),
        ])?;

        let spillover = config.room_worker_policy.spillover();
        assert!(matches!(
            spillover,
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_)
        ));
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) = spillover else {
            anyhow::bail!("spillover policy should be load triggered");
        };
        assert_eq!(policy, LocalSpilloverPolicy::conservative());
        Ok(())
    }

    #[test]
    fn load_transport_config_keeps_explicit_single_router_strict() -> Result<()> {
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "2"),
            ("ROOM_MAX_LOCAL_ROUTERS", "1"),
        ])?;

        assert_eq!(
            config.room_worker_policy,
            RoomWorkerPolicy::strict_single_router()
        );
        Ok(())
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
    fn load_transport_config_rejects_invalid_transport_values() {
        assert_invalid_transport_cases(&[
            InvalidTransportCase {
                name: "removed transport backend",
                overrides: &[("TRANSPORT_BACKEND", "rtc")],
                message: "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport",
            },
            InvalidTransportCase {
                name: "unspecified public IP",
                overrides: &[("PUBLIC_IP", "0.0.0.0")],
                message: "PUBLIC_IP must be a concrete advertised address",
            },
            InvalidTransportCase {
                name: "multicast public IP",
                overrides: &[("PUBLIC_IP", "239.1.1.1")],
                message: "PUBLIC_IP cannot be a multicast address",
            },
            InvalidTransportCase {
                name: "inverted RTC port range",
                overrides: &[("RTC_MIN_PORT", "5000"), ("RTC_MAX_PORT", "4000")],
                message: "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT",
            },
            InvalidTransportCase {
                name: "zero RTC media worker count",
                overrides: &[("RTC_MEDIA_WORKER_COUNT", "0")],
                message: "RTC_MEDIA_WORKER_COUNT must be greater than zero",
            },
            InvalidTransportCase {
                name: "more RTC workers than ports",
                overrides: &[
                    ("RTC_MIN_PORT", "4000"),
                    ("RTC_MAX_PORT", "4001"),
                    ("RTC_MEDIA_WORKER_COUNT", "3"),
                ],
                message: "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count",
            },
        ]);
    }

    #[test]
    fn load_transport_config_rejects_invalid_room_policy_values() {
        assert_invalid_transport_cases(&[
            InvalidTransportCase {
                name: "invalid spillover mode",
                overrides: &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_MODE", "invalid"),
                ],
                message: "ROOM_SPILLOVER_MODE must be one of strict, load, load-triggered or bounded, got invalid",
            },
            InvalidTransportCase {
                name: "zero spillover activation window",
                overrides: &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_ACTIVATION_WINDOW", "0"),
                ],
                message: "ROOM_SPILLOVER_ACTIVATION_WINDOW must be greater than zero",
            },
            InvalidTransportCase {
                name: "zero spillover receiver threshold",
                overrides: &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_MIN_RECEIVERS", "0"),
                ],
                message: "ROOM_SPILLOVER_MIN_RECEIVERS must be greater than zero",
            },
            InvalidTransportCase {
                name: "out of range spillover worker pressure",
                overrides: &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "2"),
                    ("ROOM_SPILLOVER_WORKER_PRESSURE", "101"),
                ],
                message: "ROOM_SPILLOVER_WORKER_PRESSURE must be less than or equal to 100",
            },
            InvalidTransportCase {
                name: "zero room router cap",
                overrides: &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "0"),
                ],
                message: "ROOM_MAX_LOCAL_ROUTERS must be greater than zero",
            },
            InvalidTransportCase {
                name: "more room routers than RTC workers",
                overrides: &[
                    ("RTC_MEDIA_WORKER_COUNT", "2"),
                    ("ROOM_MAX_LOCAL_ROUTERS", "3"),
                ],
                message: "ROOM_MAX_LOCAL_ROUTERS must be less than or equal to RTC_MEDIA_WORKER_COUNT",
            },
        ]);
    }

    #[test]
    fn load_transport_config_rejects_invalid_bitrate_and_media_limit_values() {
        assert_invalid_transport_cases(&[
            InvalidTransportCase {
                name: "zero max incoming bitrate",
                overrides: &[("MAX_BITRATE_IN", "0")],
                message: "MAX_BITRATE_IN must be greater than zero",
            },
            InvalidTransportCase {
                name: "zero max outgoing bitrate",
                overrides: &[("MAX_BITRATE_OUT", "0")],
                message: "MAX_BITRATE_OUT must be greater than zero",
            },
            InvalidTransportCase {
                name: "zero max video bitrate",
                overrides: &[("MAX_VIDEO_BITRATE", "0")],
                message: "MAX_VIDEO_BITRATE must be greater than zero",
            },
            InvalidTransportCase {
                name: "zero active audio speaker limit",
                overrides: &[("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "0")],
                message: "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS must be greater than zero",
            },
            InvalidTransportCase {
                name: "zero video download limit",
                overrides: &[("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "0")],
                message: "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER must be greater than zero",
            },
        ]);
    }

    #[test]
    fn load_transport_config_preserves_legacy_error_precedence() {
        let error = load_transport_config_with_defaults(&[
            ("MAX_BITRATE_IN", "0"),
            ("RTC_MAX_PORT", "abc"),
        ])
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("MAX_BITRATE_IN must be greater than zero")
        );

        let error = load_transport_config_with_defaults(&[
            ("RTC_MIN_PORT", "5000"),
            ("RTC_MAX_PORT", "4000"),
            ("ROOM_SPILLOVER_WORKER_PRESSURE", "101"),
        ])
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("ROOM_SPILLOVER_WORKER_PRESSURE must be less than or equal to 100")
        );
    }

    #[test]
    fn load_transport_config_ignores_spillover_mode_for_strict_single_router() -> Result<()> {
        let config = load_transport_config_with_defaults(&[("ROOM_SPILLOVER_MODE", "invalid")])?;

        assert_eq!(
            config.room_worker_policy,
            RoomWorkerPolicy::strict_single_router()
        );
        Ok(())
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
            (
                "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS",
                "abc",
                "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS must be a valid usize",
            ),
            (
                "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER",
                "abc",
                "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER must be a valid usize",
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
