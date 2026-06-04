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
    test_support::{DebugPacketGate, test_transport_session_key},
    worker::RtcWorker,
};
pub(super) use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags,
    engine::{
        UserId,
        media_transport::{
            ActiveSpeakerSource, ReceiverBweTargetUpdate, SessionOffer, TransportAdapterError,
            TransportConsumerRoute, TransportMediaId, TransportSessionKey, TransportSourceKey,
        },
        metrics::test_support::RuntimeMetricsSnapshotTestExt,
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

pub(super) fn transport_consumer_route(
    consumer_key: &TransportSessionKey,
    consumer_media: TransportMediaId,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
) -> TransportConsumerRoute {
    TransportConsumerRoute::new(
        consumer_key.clone(),
        consumer_media,
        TransportSourceKey::new(src_key.clone(), src_media),
    )
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

pub(super) fn rtc_with_bitrate_limits(
    max_bitrate_in: Bitrate,
    max_bitrate_out: Bitrate,
) -> RtcWorker {
    RtcWorker::test_builder()
        .bitrate_limits(max_bitrate_in, max_bitrate_out)
        .build()
}

pub(super) fn rtc_with_codec_flags(codec_flags: MediaCodecFlags) -> RtcWorker {
    RtcWorker::test_builder().codec_flags(codec_flags).build()
}

pub(super) fn rtc_with_codec_policy(
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> RtcWorker {
    RtcWorker::test_builder()
        .codec_policy(codec_flags, codec_preferences)
        .build()
}

pub(super) async fn expect_initial_offer(
    adapter: &RtcWorker,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    adapter
        .create_initial_session_offer(session_key)
        .await
        .expect("initial offer should succeed")
}
