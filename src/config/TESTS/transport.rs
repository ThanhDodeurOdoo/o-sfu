use std::net::{IpAddr, Ipv4Addr};

use anyhow::Result;
use o_sfu_core::prelude::{LocalSpilloverPolicy, RoomSpilloverMode};

use super::{
    Bitrate, Env, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend,
    TransportConfig, VideoBitrateLimits, default_rtc_media_worker_count,
};

fn load_transport_config(get_var: impl Fn(&str) -> Option<String>) -> Result<TransportConfig> {
    let env = Env::new(get_var);
    TransportConfig::from_env(&env)
}

fn load_transport_config_with_defaults(overrides: &[(&str, &str)]) -> Result<TransportConfig> {
    load_transport_config(|key| {
        overrides
            .iter()
            .find(|(name, _value)| *name == key)
            .map(|(_name, value)| (*value).to_owned())
            .or_else(|| match key {
                "ANNOUNCED_IP" => Some("127.0.0.1".to_owned()),
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
        "ANNOUNCED_IP" => Some("203.0.113.10".to_owned()),
        _ => None,
    });
    let worker_count = default_rtc_media_worker_count();
    assert_eq!(
        config.ok(),
        Some(TransportConfig {
            announced_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
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
fn load_transport_config_accepts_explicit_rtc_udp_io_backend() -> Result<()> {
    let config = load_transport_config_with_defaults(&[("RTC_UDP_IO_BACKEND", "tokio")])?;
    assert_eq!(config.rtc_udp_io_backend, RtcUdpIoBackend::Tokio);

    #[cfg(target_os = "linux")]
    {
        let config = load_transport_config_with_defaults(&[("RTC_UDP_IO_BACKEND", "io_uring")])?;
        assert_eq!(config.rtc_udp_io_backend, RtcUdpIoBackend::IoUring);
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[test]
fn load_transport_config_rejects_io_uring_backend_on_non_linux() {
    assert_invalid_transport_cases(&[InvalidTransportCase {
        name: "non-Linux io_uring backend",
        overrides: &[("RTC_UDP_IO_BACKEND", "io_uring")],
        message: "RTC_UDP_IO_BACKEND=io_uring is only supported on Linux",
    }]);
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
            name: "invalid RTC UDP IO backend",
            overrides: &[("RTC_UDP_IO_BACKEND", "epoll")],
            message: "RTC_UDP_IO_BACKEND must be one of tokio or io_uring, got epoll",
        },
        InvalidTransportCase {
            name: "unspecified public IP",
            overrides: &[("ANNOUNCED_IP", "0.0.0.0")],
            message: "ANNOUNCED_IP must be a concrete advertised address",
        },
        InvalidTransportCase {
            name: "multicast public IP",
            overrides: &[("ANNOUNCED_IP", "239.1.1.1")],
            message: "ANNOUNCED_IP cannot be a multicast address",
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
            name: "invalid spillover mode with default router cap",
            overrides: &[("ROOM_SPILLOVER_MODE", "invalid")],
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
    let error =
        load_transport_config_with_defaults(&[("MAX_BITRATE_IN", "0"), ("RTC_MAX_PORT", "abc")])
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
fn load_transport_config_preserves_numeric_parse_errors() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            name: "invalid RTC min port",
            overrides: &[("RTC_MIN_PORT", "abc")],
            message: "RTC_MIN_PORT must be a valid u16",
        },
        InvalidTransportCase {
            name: "invalid max incoming bitrate",
            overrides: &[("MAX_BITRATE_IN", "abc")],
            message: "MAX_BITRATE_IN must be a valid u64",
        },
        InvalidTransportCase {
            name: "invalid room router cap",
            overrides: &[("ROOM_MAX_LOCAL_ROUTERS", "abc")],
            message: "ROOM_MAX_LOCAL_ROUTERS must be a valid usize",
        },
        InvalidTransportCase {
            name: "invalid spillover worker pressure",
            overrides: &[("ROOM_SPILLOVER_WORKER_PRESSURE", "abc")],
            message: "ROOM_SPILLOVER_WORKER_PRESSURE must be a valid u8",
        },
        InvalidTransportCase {
            name: "invalid active audio speaker limit",
            overrides: &[("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "abc")],
            message: "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS must be a valid usize",
        },
        InvalidTransportCase {
            name: "invalid video download limit",
            overrides: &[("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "abc")],
            message: "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER must be a valid usize",
        },
    ]);
}
