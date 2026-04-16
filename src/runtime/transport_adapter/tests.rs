use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use o_sfu_router::{
    MediaCodecCapability, MediaKind, MediaKind as RouterMediaKind, RtpEncoding, RtpParameters,
};
use str0m::media::Mid;

use super::RuntimeTransportAdapter;
use crate::{
    config::{MediaCodecFlags, RtcPortRange},
    runtime::{
        metrics::RuntimeMetrics,
        recording::MediaTap,
        rtc_adapter::RtcTransportAdapter,
        transport_adapter::{
            RtcTransportAdapterShardSetConfig, StubWebRtcAdapter, TransportAdapterError,
            TransportMediaId, TransportSessionKey,
        },
    },
    signaling::shared::SessionId,
};
use o_sfu_router::RtpCapabilities as RouterRtpCapabilities;

fn empty_router_capabilities() -> RouterRtpCapabilities {
    RouterRtpCapabilities::new(vec![], vec![])
}

fn sample_router_capabilities() -> RouterRtpCapabilities {
    RouterRtpCapabilities::new(
        vec![
            MediaCodecCapability::new(RouterMediaKind::Audio, "opus", 48_000)
                .with_channels(2)
                .with_preferred_payload_type(111),
        ],
        vec![],
    )
}

fn sample_audio_rtp_parameters(mid: &str, ssrc: u32) -> RtpParameters {
    RtpParameters::new(vec![], vec![], vec![RtpEncoding::new().with_ssrc(ssrc)])
        .with_mid(String::from(mid))
}

async fn bootstrap_rtc_session(
    adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
) {
    assert!(
        adapter
            .transport_bootstrap_payload(session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
}

async fn bootstrap_rtc_sessions(
    adapter: &RuntimeTransportAdapter,
    session_keys: &[&TransportSessionKey],
) {
    for session_key in session_keys {
        bootstrap_rtc_session(adapter, session_key).await;
    }
}

async fn publish_audio(
    adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
    rtp_parameters: &RtpParameters,
) -> Option<TransportMediaId> {
    let result = adapter
        .publish_media(session_key, MediaKind::Audio, rtp_parameters)
        .await;
    assert!(result.is_ok());
    result.ok()
}

async fn consume_audio(
    adapter: &RuntimeTransportAdapter,
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    rtp_parameters: &RtpParameters,
) -> Option<TransportMediaId> {
    let result = adapter
        .consume_media(
            consumer_session_key,
            MediaKind::Audio,
            source_session_key,
            source_media_id,
            rtp_parameters,
        )
        .await;
    assert!(result.is_ok());
    result.ok()
}

fn assert_relay_target_counts(
    source_shard: &RtcTransportAdapter,
    source_media_id: TransportMediaId,
    total: usize,
    active: usize,
) {
    assert_eq!(
        source_shard.debug_relay_target_count_for_source(source_media_id),
        total
    );
    assert_eq!(
        source_shard.debug_active_relay_target_count_for_source(source_media_id),
        active
    );
}

async fn assert_remote_source_owner(
    consumer_shard: &RtcTransportAdapter,
    source_media_id: TransportMediaId,
    expected: Option<&TransportSessionKey>,
) {
    assert_eq!(
        consumer_shard
            .debug_remote_source_owner(source_media_id)
            .await,
        expected.cloned()
    );
}

async fn assert_local_route_active(
    source_shard: &RtcTransportAdapter,
    source_session: &TransportSessionKey,
    local_consumer_session: &TransportSessionKey,
    local_consumer_media_id: TransportMediaId,
) {
    let local_route_entry = source_shard
        .debug_route_entry(source_session, Mid::from("aud-up"))
        .await;
    assert!(local_route_entry.is_some());
    let Some(local_route_entry) = local_route_entry else {
        return;
    };
    assert!(local_route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *local_consumer_session
            && destination.dest_transport_media_id == local_consumer_media_id
            && destination.active
    }));
}

async fn assert_remote_route_activity(
    consumer_shard: &RtcTransportAdapter,
    source_media_id: TransportMediaId,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
    active: bool,
) {
    let remote_route_entry = consumer_shard
        .debug_route_entry_by_media_id(source_media_id)
        .await;
    assert!(remote_route_entry.is_some());
    let Some(remote_route_entry) = remote_route_entry else {
        return;
    };
    assert!(remote_route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *remote_consumer_session
            && destination.dest_transport_media_id == remote_consumer_media_id
            && destination.active == active
    }));
}

fn test_rtc_adapter(worker_count: usize, rtc_port_range: RtcPortRange) -> RuntimeTransportAdapter {
    RuntimeTransportAdapter::builder()
        .rtc(RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range,
            worker_count,
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
        .build()
}

#[test]
fn stub_adapter_uses_explicit_compatibility_capability_projection() {
    let adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::new(StubWebRtcAdapter::default()));
    let offered = sample_router_capabilities();

    let projected =
        adapter.negotiated_client_rtp_capabilities("v=0\r\ns=stub-answer\r\n", &offered);

    assert_eq!(projected, Ok(offered));
}

#[test]
fn stub_adapter_rejects_answers_without_minimal_sdp_shape() {
    let adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::new(StubWebRtcAdapter::default()));

    let projected =
        adapter.negotiated_client_rtp_capabilities("invalid-answer", &sample_router_capabilities());

    assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
}

#[test]
fn rtc_adapter_rejects_answers_without_projectable_client_capabilities() {
    let adapter = RuntimeTransportAdapter::builder()
        .rtc(RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(46_100, 46_199),
            1,
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
        .build();

    let projected = adapter.negotiated_client_rtp_capabilities(
        "v=0\r\ns=invalid-answer\r\n",
        &sample_router_capabilities(),
    );

    assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn rtc_adapter_shards_channel_bootstrap_by_explicit_media_worker() {
    let adapter = RuntimeTransportAdapter::builder()
        .rtc(RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(46_000, 46_003),
            2,
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
        .build();
    let first_channel_session = TransportSessionKey::new(10, 0, 1, SessionId::Integer(1));
    let second_channel_session = TransportSessionKey::new(11, 1, 1, SessionId::Integer(2));
    let same_shard_session = TransportSessionKey::new(12, 0, 1, SessionId::Integer(3));

    let first_payload = adapter
        .transport_bootstrap_payload(&first_channel_session, &empty_router_capabilities())
        .await;
    let second_payload = adapter
        .transport_bootstrap_payload(&second_channel_session, &empty_router_capabilities())
        .await;
    let same_shard_payload = adapter
        .transport_bootstrap_payload(&same_shard_session, &empty_router_capabilities())
        .await;
    assert!(first_payload.is_ok());
    assert!(second_payload.is_ok());
    assert!(same_shard_payload.is_ok());
    let Some(first_payload) = first_payload.ok() else {
        return;
    };
    let Some(second_payload) = second_payload.ok() else {
        return;
    };
    let Some(same_shard_payload) = same_shard_payload.ok() else {
        return;
    };

    let Some(first_candidate) = first_payload.download_transport.ice_candidates.first() else {
        return;
    };
    let Some(second_candidate) = second_payload.download_transport.ice_candidates.first() else {
        return;
    };
    let Some(same_shard_candidate) = same_shard_payload.download_transport.ice_candidates.first()
    else {
        return;
    };
    let first_port = first_candidate.port;
    let second_port = second_candidate.port;
    let same_shard_port = same_shard_candidate.port;

    assert!((46_000..=46_001).contains(&first_port));
    assert!((46_002..=46_003).contains(&second_port));
    assert_eq!(same_shard_port, first_port);
}

#[tokio::test]
async fn rtc_adapter_registers_and_prunes_cross_worker_remote_sources() {
    let adapter = test_rtc_adapter(2, RtcPortRange::new(46_200, 46_299));
    let source_session = TransportSessionKey::new(20, 0, 1, SessionId::Integer(1));
    let consumer_session = TransportSessionKey::new(20, 1, 2, SessionId::Integer(2));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 41_000);
    let consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down", 42_000);

    bootstrap_rtc_session(&adapter, &source_session).await;
    bootstrap_rtc_session(&adapter, &consumer_session).await;

    let Some(source_media_id) =
        publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
    else {
        return;
    };
    let Some(consumer_media_id) = consume_audio(
        &adapter,
        &consumer_session,
        &source_session,
        source_media_id,
        &consumer_rtp_parameters,
    )
    .await
    else {
        return;
    };
    let RuntimeTransportAdapter::Rtc(shards) = &adapter else {
        return;
    };
    let source_shard = shards.shard_for_session(&source_session);
    let consumer_shard = shards.shard_for_session(&consumer_session);

    assert_eq!(
        source_shard.debug_relay_target_count_for_source(source_media_id),
        1
    );
    assert_eq!(
        source_shard.debug_active_relay_target_count_for_source(source_media_id),
        1
    );
    assert_eq!(
        consumer_shard
            .debug_remote_source_owner(source_media_id)
            .await,
        Some(source_session.clone())
    );
    let route_entry = consumer_shard
        .debug_route_entry_by_media_id(source_media_id)
        .await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert!(route_entry.source_active);
    assert!(route_entry.destinations.iter().any(|destination| {
        destination.dest_session == consumer_session
            && destination.dest_transport_media_id == consumer_media_id
    }));

    assert!(
        adapter
            .remove_media(&consumer_session, consumer_media_id)
            .await
            .is_ok()
    );
    assert_eq!(
        consumer_shard
            .debug_remote_source_owner(source_media_id)
            .await,
        None
    );
    assert!(
        consumer_shard
            .debug_route_entry_by_media_id(source_media_id)
            .await
            .is_none()
    );
    assert_eq!(
        source_shard.debug_relay_target_count_for_source(source_media_id),
        0
    );
    assert_eq!(
        source_shard.debug_active_relay_target_count_for_source(source_media_id),
        0
    );
}

#[tokio::test]
async fn rtc_adapter_keeps_independent_relay_targets_per_remote_worker() {
    let adapter = RuntimeTransportAdapter::builder()
        .rtc(RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(46_300, 46_599),
            3,
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
        .build();
    let source_session = TransportSessionKey::new(30, 0, 1, SessionId::Integer(1));
    let first_consumer_session = TransportSessionKey::new(30, 1, 2, SessionId::Integer(2));
    let second_consumer_session = TransportSessionKey::new(30, 2, 3, SessionId::Integer(3));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 51_000);
    let first_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-1", 52_000);
    let second_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-2", 53_000);

    bootstrap_rtc_sessions(
        &adapter,
        &[
            &source_session,
            &first_consumer_session,
            &second_consumer_session,
        ],
    )
    .await;

    let Some(source_media_id) =
        publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
    else {
        return;
    };
    let Some(first_consumer_media_id) = consume_audio(
        &adapter,
        &first_consumer_session,
        &source_session,
        source_media_id,
        &first_consumer_rtp_parameters,
    )
    .await
    else {
        return;
    };
    let Some(second_consumer_media_id) = consume_audio(
        &adapter,
        &second_consumer_session,
        &source_session,
        source_media_id,
        &second_consumer_rtp_parameters,
    )
    .await
    else {
        return;
    };

    let RuntimeTransportAdapter::Rtc(shards) = &adapter else {
        return;
    };
    let source_shard = shards.shard_for_session(&source_session);
    let first_consumer_shard = shards.shard_for_session(&first_consumer_session);
    let second_consumer_shard = shards.shard_for_session(&second_consumer_session);

    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 2, 2);
    assert_remote_source_owner(
        first_consumer_shard.as_ref(),
        source_media_id,
        Some(&source_session),
    )
    .await;
    assert_remote_source_owner(
        second_consumer_shard.as_ref(),
        source_media_id,
        Some(&source_session),
    )
    .await;

    assert!(
        adapter
            .remove_media(&first_consumer_session, first_consumer_media_id)
            .await
            .is_ok()
    );
    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 1);
    assert_remote_source_owner(
        second_consumer_shard.as_ref(),
        source_media_id,
        Some(&source_session),
    )
    .await;

    assert!(
        adapter
            .remove_media(&second_consumer_session, second_consumer_media_id)
            .await
            .is_ok()
    );
    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 0, 0);
}

#[tokio::test]
async fn rtc_adapter_gates_remote_relay_mailboxes_without_touching_local_routes() {
    let adapter = test_rtc_adapter(2, RtcPortRange::new(46_600, 46_699));
    let source_session = TransportSessionKey::new(40, 0, 1, SessionId::Integer(1));
    let local_consumer_session = TransportSessionKey::new(40, 0, 2, SessionId::Integer(2));
    let remote_consumer_session = TransportSessionKey::new(40, 1, 3, SessionId::Integer(3));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 61_000);
    let local_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-local", 62_000);
    let remote_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-remote", 63_000);

    bootstrap_rtc_sessions(
        &adapter,
        &[
            &source_session,
            &local_consumer_session,
            &remote_consumer_session,
        ],
    )
    .await;

    let Some(source_media_id) =
        publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
    else {
        return;
    };
    let Some(local_consumer_media_id) = consume_audio(
        &adapter,
        &local_consumer_session,
        &source_session,
        source_media_id,
        &local_consumer_rtp_parameters,
    )
    .await
    else {
        return;
    };
    let Some(remote_consumer_media_id) = consume_audio(
        &adapter,
        &remote_consumer_session,
        &source_session,
        source_media_id,
        &remote_consumer_rtp_parameters,
    )
    .await
    else {
        return;
    };

    let RuntimeTransportAdapter::Rtc(shards) = &adapter else {
        return;
    };
    let source_shard = shards.shard_for_session(&source_session);
    let remote_consumer_shard = shards.shard_for_session(&remote_consumer_session);

    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 1);

    assert!(
        adapter
            .set_consumer_active(
                &remote_consumer_session,
                remote_consumer_media_id,
                &source_session,
                source_media_id,
                false,
            )
            .await
            .is_ok()
    );

    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 0);
    assert_local_route_active(
        source_shard.as_ref(),
        &source_session,
        &local_consumer_session,
        local_consumer_media_id,
    )
    .await;
    assert_remote_route_activity(
        remote_consumer_shard.as_ref(),
        source_media_id,
        &remote_consumer_session,
        remote_consumer_media_id,
        false,
    )
    .await;

    assert!(
        adapter
            .set_consumer_active(
                &remote_consumer_session,
                remote_consumer_media_id,
                &source_session,
                source_media_id,
                true,
            )
            .await
            .is_ok()
    );
    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 1);

    assert!(
        adapter
            .remove_media(&remote_consumer_session, remote_consumer_media_id)
            .await
            .is_ok()
    );
    assert_relay_target_counts(source_shard.as_ref(), source_media_id, 0, 0);
}
