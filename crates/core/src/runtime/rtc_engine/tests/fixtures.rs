#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]
pub(super) use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    slice,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) use o_sfu_router::{
    MediaStream as RouterRtpParameters, StreamBinding as RouterRtpEncoding,
};
pub(super) use str0m::media::{MediaKind as Str0mMediaKind, Mid};
pub(super) use tokio::time::sleep;

pub(super) use super::super::{
    RtcTransportWorker,
    shared_payload::SharedPayload,
    test_support::{DebugPacketGate, test_transport_session_key},
};
pub(super) use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{
            ActiveSpeakerSource, MediaTransportDeps, RtcTransportConfig, SourcePolicySignal,
            TransportAdapterError, TransportSessionKey,
        },
        metrics::{RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

pub(super) fn transport_key(
    room_instance_id: u64,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    transport_key_on_worker(room_instance_id, 0, connection_id, user_id)
}

pub(super) fn transport_key_on_worker(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    test_transport_session_key(room_instance_id, media_worker_id, connection_id, user_id)
}

pub(super) fn sample_router_rtp_parameters(mid: &str, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc)],
    )
    .with_mid(mid.to_owned())
}

pub(super) fn sample_router_rtp_parameters_with_rid(
    mid: &str,
    ssrc: u32,
    rid: &str,
) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc).with_rid(rid)],
    )
    .with_mid(mid.to_owned())
}

pub(super) fn rtc_engine_with_bitrate_limits(
    max_bitrate_in: Bitrate,
    max_bitrate_out: Bitrate,
) -> RtcTransportWorker {
    rtc_engine_for_test(
        max_bitrate_in,
        max_bitrate_out,
        MediaCodecFlags::default(),
        CodecPreferences::default(),
    )
}

pub(super) fn rtc_engine_with_codec_flags(codec_flags: MediaCodecFlags) -> RtcTransportWorker {
    rtc_engine_for_test(
        Bitrate::from_mbps(8),
        Bitrate::from_mbps(10),
        codec_flags,
        CodecPreferences::default(),
    )
}

pub(super) fn rtc_engine_with_codec_policy(
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> RtcTransportWorker {
    rtc_engine_for_test(
        Bitrate::from_mbps(8),
        Bitrate::from_mbps(10),
        codec_flags,
        codec_preferences,
    )
}

fn rtc_engine_for_test(
    max_bitrate_in: Bitrate,
    max_bitrate_out: Bitrate,
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> RtcTransportWorker {
    RtcTransportWorker::new(
        &RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(max_bitrate_in, max_bitrate_out),
            video_bitrate_limits: crate::VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            codec_flags,
            codec_preferences,
        },
        &MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        },
        Arc::new(SourcePolicySignal::default()),
        0,
        0,
    )
}
