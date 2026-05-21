use std::net::IpAddr;

use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{
    Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, VideoBitrateLimits,
};

use super::{
    TransportConfig,
    log_view::ConfigLogField,
    parsing::{parse_env_or_default, parse_positive_env_or_default, parse_required_env},
};

pub(super) fn load_transport_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<TransportConfig> {
    if get_var("TRANSPORT_BACKEND").is_some() {
        return Err(anyhow!(
            "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport"
        ));
    }
    let public_ip = parse_required_env(&mut get_var, "PUBLIC_IP")?;
    let rtc_min_port = parse_env_or_default(&mut get_var, "RTC_MIN_PORT", 40_000)?;
    let max_bitrate_in_bps =
        parse_positive_env_or_default(&mut get_var, "MAX_BITRATE_IN", 8_000_000)?;
    let max_bitrate_out_bps =
        parse_positive_env_or_default(&mut get_var, "MAX_BITRATE_OUT", 10_000_000)?;
    let max_video_bitrate_bps = parse_positive_env_or_default(
        &mut get_var,
        "MAX_VIDEO_BITRATE",
        VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE.as_bps(),
    )?;
    let rtc_max_port = parse_env_or_default(&mut get_var, "RTC_MAX_PORT", 49_999)?;
    let rtc_media_worker_count =
        parse_positive_env_or_default(&mut get_var, "RTC_MEDIA_WORKER_COUNT", 1)?;
    let room_max_local_routers =
        parse_positive_env_or_default(&mut get_var, "ROOM_MAX_LOCAL_ROUTERS", 1)?;
    let room_spillover_mode = get_var("ROOM_SPILLOVER_MODE");
    let local_spillover_policy = load_local_spillover_policy(&mut get_var)?;
    let room_media_limits = load_room_media_limits(&mut get_var)?;
    let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
    validate_transport_config(TransportConfigValidation {
        public_ip,
        rtc_min_port,
        rtc_max_port,
        rtc_media_worker_count,
        room_max_local_routers,
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
        room_media_limits,
    })
}

impl TransportConfig {
    #[must_use]
    pub(super) fn log_fields(&self) -> [ConfigLogField; 9] {
        [
            ConfigLogField::new("max_bitrate_in_bps", self.max_bitrate_in.as_bps()),
            ConfigLogField::new("max_bitrate_out_bps", self.max_bitrate_out.as_bps()),
            ConfigLogField::new(
                "max_video_bitrate_bps",
                self.video_bitrate_limits.max_video_bitrate().as_bps(),
            ),
            ConfigLogField::new("rtc_port_range_min", self.rtc_port_range.min()),
            ConfigLogField::new("rtc_port_range_max", self.rtc_port_range.max()),
            ConfigLogField::new("rtc_media_worker_count", self.rtc_media_worker_count),
            ConfigLogField::new(
                "room_max_local_routers",
                self.room_worker_policy.max_local_routers(),
            ),
            ConfigLogField::new(
                "room_max_active_audio_speakers",
                self.room_media_limits.max_active_audio_speakers(),
            ),
            ConfigLogField::new(
                "room_max_video_downloads_per_receiver",
                self.room_media_limits.max_video_downloads_per_receiver(),
            ),
        ]
    }
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
        min_receiver_count: parse_positive_env_or_default(
            get_var,
            "ROOM_SPILLOVER_MIN_RECEIVERS",
            LocalSpilloverPolicy::DEFAULT_MIN_RECEIVER_COUNT,
        )?,
        max_active_consumers_per_router: parse_positive_env_or_default(
            get_var,
            "ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER",
            LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER,
        )?,
        max_fanout_per_source: parse_positive_env_or_default(
            get_var,
            "ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE",
            LocalSpilloverPolicy::DEFAULT_MAX_FANOUT_PER_SOURCE,
        )?,
        egress_bitrate_threshold: Bitrate::from_bps(parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_EGRESS_BITRATE_BPS",
            LocalSpilloverPolicy::DEFAULT_EGRESS_BITRATE_THRESHOLD.as_bps(),
        )?),
        packet_loop_lag_threshold_ms: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_PACKET_LOOP_LAG_MS",
            LocalSpilloverPolicy::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS,
        )?,
        command_backlog_threshold: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_COMMAND_BACKLOG",
            LocalSpilloverPolicy::DEFAULT_COMMAND_BACKLOG_THRESHOLD,
        )?,
        relay_mailbox_depth_threshold: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH",
            LocalSpilloverPolicy::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD,
        )?,
        worker_pressure_threshold: parse_env_or_default(
            get_var,
            "ROOM_SPILLOVER_WORKER_PRESSURE",
            LocalSpilloverPolicy::DEFAULT_WORKER_PRESSURE_THRESHOLD,
        )?,
        activation_window: parse_positive_env_or_default(
            get_var,
            "ROOM_SPILLOVER_ACTIVATION_WINDOW",
            LocalSpilloverPolicy::DEFAULT_ACTIVATION_WINDOW,
        )?,
        cooldown_window: parse_positive_env_or_default(
            get_var,
            "ROOM_SPILLOVER_COOLDOWN_WINDOW",
            LocalSpilloverPolicy::DEFAULT_COOLDOWN_WINDOW,
        )?,
    };
    LocalSpilloverPolicy::try_new(parts).map_err(local_spillover_policy_error)
}

fn load_room_media_limits(
    get_var: &mut impl FnMut(&str) -> Option<String>,
) -> Result<RoomMediaLimits> {
    Ok(RoomMediaLimits::try_new(
        parse_positive_env_or_default(
            get_var,
            "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS",
            RoomMediaLimits::DEFAULT_MAX_ACTIVE_AUDIO_SPEAKERS,
        )?,
        parse_positive_env_or_default(
            get_var,
            "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER",
            RoomMediaLimits::DEFAULT_MAX_VIDEO_DOWNLOADS_PER_RECEIVER,
        )?,
    )?)
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
    rtc_port_range: RtcPortRange,
}

fn validate_transport_config(input: TransportConfigValidation) -> Result<()> {
    ensure!(
        input.rtc_min_port <= input.rtc_max_port,
        "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
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

    use anyhow::Result;
    use o_sfu_core::prelude::{LocalSpilloverPolicy, RoomSpilloverMode};

    use super::{
        Bitrate, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, TransportConfig,
        VideoBitrateLimits, load_transport_config,
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
                room_media_limits: RoomMediaLimits::default(),
            })
        );
    }

    #[test]
    fn load_transport_config_accepts_explicit_bitrate_limits() {
        let config = load_transport_config_with_defaults(&[
            ("MAX_BITRATE_IN", "1234567"),
            ("MAX_BITRATE_OUT", "7654321"),
            ("MAX_VIDEO_BITRATE", "2345678"),
        ]);
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
        ]);

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
    fn load_transport_config_accepts_room_media_limits() {
        let config = load_transport_config_with_defaults(&[
            ("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "3"),
            ("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "8"),
        ]);

        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.room_media_limits.max_active_audio_speakers(), 3);
        assert_eq!(
            config.room_media_limits.max_video_downloads_per_receiver(),
            8
        );
    }

    #[test]
    fn load_transport_config_accepts_explicit_bounded_spillover_mode() {
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "3"),
            ("ROOM_MAX_LOCAL_ROUTERS", "2"),
            ("ROOM_SPILLOVER_MODE", "bounded"),
        ]);

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
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "2"),
            ("ROOM_MAX_LOCAL_ROUTERS", "2"),
        ]);

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
        let config = load_transport_config_with_defaults(&[
            ("RTC_MEDIA_WORKER_COUNT", "2"),
            ("ROOM_MAX_LOCAL_ROUTERS", "1"),
        ]);

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
                "zero active audio speaker limit",
                &[("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "0")],
            ),
            (
                "zero video download limit",
                &[("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "0")],
            ),
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
