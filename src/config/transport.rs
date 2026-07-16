use std::{net::IpAddr, num::NonZeroUsize, thread};

use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{
    Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend, VideoBitrateLimits,
};

use super::{
    TransportConfig,
    env::{Env, EnvParse, EnvValue, positive},
};

enum RoomSpilloverSetting {
    Strict,
    LoadTriggered,
    Bounded,
}

impl EnvParse for RoomSpilloverSetting {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key();
        match value.as_str() {
            "strict" => Ok(Self::Strict),
            "load" | "load-triggered" => Ok(Self::LoadTriggered),
            "bounded" => Ok(Self::Bounded),
            other => Err(anyhow!(
                "{key} must be one of strict, load, load-triggered or bounded, got {other}"
            )),
        }
    }
}

impl EnvParse for RtcUdpIoBackend {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key();
        match value.as_str() {
            "tokio" => Ok(Self::Tokio),
            "io_uring" => {
                ensure!(
                    cfg!(target_os = "linux"),
                    "{key}=io_uring is only supported on Linux"
                );
                Ok(Self::IoUring)
            }
            other => Err(anyhow!(
                "{key} must be one of tokio or io_uring, got {other}"
            )),
        }
    }
}

impl TransportConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        if env.var::<String>("TRANSPORT_BACKEND").optional()?.is_some() {
            return Err(anyhow!(
                "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport"
            ));
        }
        let announced_ip = env
            .var::<IpAddr>("ANNOUNCED_IP")
            .alias("PUBLIC_IP")
            .required()?;
        let rtc_min_port = env.var("RTC_MIN_PORT").default(40_000)?;
        let max_bitrate_in_bps = env
            .var("MAX_BITRATE_IN")
            .check(positive)
            .default(8_000_000)?;
        let max_bitrate_out_bps = env
            .var("MAX_BITRATE_OUT")
            .check(positive)
            .default(10_000_000)?;
        let max_video_bitrate_bps = env
            .var("MAX_VIDEO_BITRATE")
            .check(positive)
            .default(VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE.as_bps())?;
        let rtc_max_port = env.var("RTC_MAX_PORT").default(49_999)?;
        let rtc_udp_io_backend = env
            .var("RTC_UDP_IO_BACKEND")
            .default(RtcUdpIoBackend::Tokio)?;
        let rtc_media_worker_count = env
            .var("RTC_MEDIA_WORKER_COUNT")
            .check(positive)
            .default(default_rtc_media_worker_count())?;
        let room_max_local_routers = env
            .var("ROOM_MAX_LOCAL_ROUTERS")
            .check(positive)
            .default(1)?;
        let room_spillover_mode = env.var("ROOM_SPILLOVER_MODE").optional()?;
        let local_spillover_policy = local_spillover_policy_from_env(env)?;
        let room_media_limits = room_media_limits_from_env(env)?;
        let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
        ensure!(
            rtc_port_range.min() <= rtc_port_range.max(),
            "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
        );
        ensure!(
            rtc_media_worker_count <= usize::from(rtc_port_range.port_count()),
            "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
        );
        ensure!(
            room_max_local_routers <= rtc_media_worker_count,
            "ROOM_MAX_LOCAL_ROUTERS must be less than or equal to RTC_MEDIA_WORKER_COUNT"
        );
        ensure!(
            !announced_ip.is_unspecified(),
            "ANNOUNCED_IP must be a concrete advertised address"
        );
        ensure!(
            !announced_ip.is_multicast(),
            "ANNOUNCED_IP cannot be a multicast address"
        );
        let room_worker_policy = select_room_worker_policy(
            room_max_local_routers,
            room_spillover_mode,
            local_spillover_policy,
        );
        Ok(Self {
            announced_ip,
            max_bitrate_in: Bitrate::from_bps(max_bitrate_in_bps),
            max_bitrate_out: Bitrate::from_bps(max_bitrate_out_bps),
            video_bitrate_limits: VideoBitrateLimits::new(Bitrate::from_bps(max_video_bitrate_bps)),
            rtc_port_range,
            rtc_udp_io_backend,
            rtc_media_worker_count,
            room_worker_policy,
            room_media_limits,
        })
    }
}

fn local_spillover_policy_from_env(env: &Env<'_>) -> Result<LocalSpilloverPolicy> {
    let parts = LocalSpilloverPolicyParts {
        min_receiver_count: env
            .var("ROOM_SPILLOVER_MIN_RECEIVERS")
            .check(positive)
            .default(LocalSpilloverPolicy::DEFAULT_MIN_RECEIVER_COUNT)?,
        max_active_consumers_per_router: env
            .var("ROOM_SPILLOVER_MAX_CONSUMERS_PER_ROUTER")
            .check(positive)
            .default(LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER)?,
        max_fanout_per_source: env
            .var("ROOM_SPILLOVER_MAX_FANOUT_PER_SOURCE")
            .check(positive)
            .default(LocalSpilloverPolicy::DEFAULT_MAX_FANOUT_PER_SOURCE)?,
        egress_bitrate_threshold: Bitrate::from_bps(
            env.var("ROOM_SPILLOVER_EGRESS_BITRATE_BPS")
                .default(LocalSpilloverPolicy::DEFAULT_EGRESS_BITRATE_THRESHOLD.as_bps())?,
        ),
        packet_loop_lag_threshold_ms: env
            .var("ROOM_SPILLOVER_PACKET_LOOP_LAG_MS")
            .default(LocalSpilloverPolicy::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS)?,
        command_backlog_threshold: env
            .var("ROOM_SPILLOVER_COMMAND_BACKLOG")
            .default(LocalSpilloverPolicy::DEFAULT_COMMAND_BACKLOG_THRESHOLD)?,
        relay_mailbox_depth_threshold: env
            .var("ROOM_SPILLOVER_RELAY_MAILBOX_DEPTH")
            .default(LocalSpilloverPolicy::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD)?,
        worker_pressure_threshold: env
            .var("ROOM_SPILLOVER_WORKER_PRESSURE")
            .default(LocalSpilloverPolicy::DEFAULT_WORKER_PRESSURE_THRESHOLD)?,
        activation_window: env
            .var("ROOM_SPILLOVER_ACTIVATION_WINDOW")
            .check(positive)
            .default(LocalSpilloverPolicy::DEFAULT_ACTIVATION_WINDOW)?,
    };
    LocalSpilloverPolicy::try_new(parts).map_err(local_spillover_policy_error)
}

fn room_media_limits_from_env(env: &Env<'_>) -> Result<RoomMediaLimits> {
    let active_audio_speakers = env
        .var("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS")
        .check(positive)
        .default(RoomMediaLimits::DEFAULT_MAX_ACTIVE_AUDIO_SPEAKERS)?;
    let video_downloads_per_receiver = env
        .var("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER")
        .check(positive)
        .default(RoomMediaLimits::DEFAULT_MAX_VIDEO_DOWNLOADS_PER_RECEIVER)?;
    Ok(RoomMediaLimits::try_new(
        active_audio_speakers,
        video_downloads_per_receiver,
    )?)
}

pub fn default_rtc_media_worker_count() -> usize {
    thread::available_parallelism().map_or(1, NonZeroUsize::get)
}

/// Translate operator spillover settings into the core room policy.
///
/// `ROOM_MAX_LOCAL_ROUTERS=1` keeps the strict historical topology for every
/// valid mode. `strict` also forces that topology when the cap is larger.
fn select_room_worker_policy(
    room_max_local_routers: usize,
    spillover_mode: Option<RoomSpilloverSetting>,
    local_spillover_policy: LocalSpilloverPolicy,
) -> RoomWorkerPolicy {
    match (
        room_max_local_routers,
        spillover_mode.unwrap_or(RoomSpilloverSetting::LoadTriggered),
    ) {
        (1, _) | (_, RoomSpilloverSetting::Strict) => RoomWorkerPolicy::strict_single_router(),
        (_, RoomSpilloverSetting::LoadTriggered) => {
            RoomWorkerPolicy::load_triggered_local_spillover(
                room_max_local_routers,
                local_spillover_policy,
            )
        }
        (_, RoomSpilloverSetting::Bounded) => {
            RoomWorkerPolicy::bounded_local_spillover(room_max_local_routers)
        }
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
    }
}

#[cfg(test)]
#[path = "TESTS/transport.rs"]
mod tests;
