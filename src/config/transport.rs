use std::{net::IpAddr, num::NonZeroUsize, thread};

use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{
    Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend, VideoBitrateLimits,
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
        rtc_udp_io_backend: rtc_udp_io_backend(env.rtc_udp_io_backend.as_deref())?,
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
        rtc_udp_io_backend: Option<String> = optional("RTC_UDP_IO_BACKEND");
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

pub fn default_rtc_media_worker_count() -> usize {
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

fn rtc_udp_io_backend(value: Option<&str>) -> Result<RtcUdpIoBackend> {
    match value {
        None | Some("tokio") => Ok(RtcUdpIoBackend::Tokio),
        Some("io_uring") => {
            ensure!(
                cfg!(target_os = "linux"),
                "RTC_UDP_IO_BACKEND=io_uring is only supported on Linux"
            );
            Ok(RtcUdpIoBackend::IoUring)
        }
        Some(other) => Err(anyhow!(
            "RTC_UDP_IO_BACKEND must be one of tokio or io_uring, got {other}"
        )),
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
#[path = "TESTS/transport.rs"]
mod tests;
