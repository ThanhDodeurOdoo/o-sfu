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
    RtcTransportShard,
    shared_payload::SharedPayload,
    state::TransportSessionHealth,
    test_support::{DebugPacketGate, DebugRouteEntry, test_transport_session_key},
};
pub(super) use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{
            ActiveSpeakerSource, MediaTransportDeps, RtcTransportConfig, SessionOffer,
            SourcePolicySignal, TransportAdapterError, TransportMediaId, TransportSessionKey,
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
    max_bitrate_in_bps: u64,
    max_bitrate_out_bps: u64,
) -> RtcTransportShard {
    rtc_engine_for_test(
        max_bitrate_in_bps,
        max_bitrate_out_bps,
        MediaCodecFlags::default(),
        CodecPreferences::default(),
    )
}

pub(super) fn rtc_engine_with_codec_flags(codec_flags: MediaCodecFlags) -> RtcTransportShard {
    rtc_engine_for_test(
        8_000_000,
        10_000_000,
        codec_flags,
        CodecPreferences::default(),
    )
}

pub(super) fn rtc_engine_with_codec_policy(
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> RtcTransportShard {
    rtc_engine_for_test(8_000_000, 10_000_000, codec_flags, codec_preferences)
}

fn rtc_engine_for_test(
    max_bitrate_in_bps: u64,
    max_bitrate_out_bps: u64,
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> RtcTransportShard {
    RtcTransportShard::new(
        &RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(max_bitrate_in_bps, max_bitrate_out_bps),
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
    )
}

pub(super) async fn prepare_transport_session(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    adapter
        .negotiation()
        .create_initial_session_offer(session_key)
        .await
}

pub(super) fn set_transport_health(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
    health: TransportSessionHealth,
) {
    adapter.debug_set_session_transport_health(session_key, health);
}

pub(super) async fn remember_remote_addr(
    adapter: &RtcTransportShard,
    source_addr: SocketAddr,
    session_key: &TransportSessionKey,
) {
    adapter
        .debug_remember_remote_addr(source_addr, session_key)
        .await;
}

pub(super) async fn remote_addr_owner(
    adapter: &RtcTransportShard,
    source_addr: SocketAddr,
) -> Option<TransportSessionKey> {
    adapter.debug_remote_addr_owner(source_addr).await
}

pub(super) async fn has_any_remote_addr_session(adapter: &RtcTransportShard) -> bool {
    adapter.debug_has_any_remote_addr_session().await
}

pub(super) async fn resolve_mid(
    adapter: &RtcTransportShard,
    transport_media_id: TransportMediaId,
) -> Option<Mid> {
    adapter.debug_resolve_mid(transport_media_id).await
}

pub(super) async fn session_stream_rx_ssrc(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
    mid: Mid,
) -> Option<u32> {
    adapter.debug_session_stream_rx_ssrc(session_key, mid).await
}

pub(super) async fn session_stream_tx_ssrc(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
    mid: Mid,
) -> Option<u32> {
    adapter.debug_session_stream_tx_ssrc(session_key, mid).await
}

pub(super) async fn session_max_bitrate_in(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
) -> Option<u64> {
    adapter.debug_session_max_bitrate_in(session_key).await
}

pub(super) async fn session_max_bitrate_out(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
) -> Option<u64> {
    adapter.debug_session_max_bitrate_out(session_key).await
}

pub(super) async fn route_entry_by_media_id(
    adapter: &RtcTransportShard,
    source_transport_media_id: TransportMediaId,
) -> Option<DebugRouteEntry> {
    adapter
        .debug_route_entry_by_media_id(source_transport_media_id)
        .await
}

pub(super) async fn record_incoming_media(
    adapter: &RtcTransportShard,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    payload_bytes: usize,
    now: Instant,
) {
    adapter
        .debug_record_incoming_media(session_key, transport_media_id, payload_bytes, now)
        .await;
}

pub(super) async fn observe_audio_activity(
    adapter: &RtcTransportShard,
    transport_media_id: TransportMediaId,
    voice_activity: Option<bool>,
    audio_level_dbov: Option<i8>,
    now: Instant,
) {
    adapter
        .debug_observe_audio_activity(transport_media_id, voice_activity, audio_level_dbov, now)
        .await;
}

pub(super) async fn relay_target_count_for_source(
    adapter: &RtcTransportShard,
    source_transport_media_id: TransportMediaId,
) -> usize {
    adapter
        .debug_relay_target_count_for_source(source_transport_media_id)
        .await
}

pub(super) async fn active_relay_target_count_for_source(
    adapter: &RtcTransportShard,
    source_transport_media_id: TransportMediaId,
) -> usize {
    adapter
        .debug_active_relay_target_count_for_source(source_transport_media_id)
        .await
}
