use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_protocol::shared::UserId;
use o_sfu_router::{
    MediaCapabilities as RouterRtpCapabilities, MediaCodecCapability, MediaKind,
    MediaKind as RouterMediaKind, MediaStream, StreamBinding,
};
use str0m::media::Mid;
use tokio::time::timeout;

use super::RuntimeTransportAdapter;
use crate::{
    MediaCodecFlags, RtcPortRange,
    runtime::{
        ConnectionId, RoomInstanceId,
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry as MediaTap,
        rtc_adapter::RtcTransportAdapter,
        transport_adapter::{
            ActiveSpeakerSource, MediaPort, NegotiationPort, ObservabilityPort,
            RtcTransportAdapterShardSetConfig, SessionBitrateLimits, SessionOffer, SessionPort,
            SourcePolicyPort, SourcePolicyUpdateSubscription, TransportAdapterError,
            TransportMediaId, TransportSessionKey, test_support::FakeWebRtcAdapter,
        },
    },
};

fn test_session_key(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        media_worker_id,
        ConnectionId::from_raw(connection_id),
        user_id,
    )
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

fn sample_audio_rtp_parameters(mid: &str, ssrc: u32) -> MediaStream {
    MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
        .with_mid(String::from(mid))
}

async fn prepare_rtc_session(adapter: &RuntimeTransportAdapter, session_key: &TransportSessionKey) {
    assert!(
        adapter
            .create_initial_session_offer(session_key)
            .await
            .is_ok()
    );
}

async fn prepare_rtc_sessions(
    adapter: &RuntimeTransportAdapter,
    session_keys: &[&TransportSessionKey],
) {
    for session_key in session_keys {
        prepare_rtc_session(adapter, session_key).await;
    }
}

async fn publish_audio(
    adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
    rtp_parameters: &MediaStream,
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
    rtp_parameters: &MediaStream,
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
    RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        SessionBitrateLimits::new(8_000_000, 10_000_000),
        rtc_port_range,
        worker_count,
        MediaCodecFlags::default(),
        Arc::new(DiagnosticsStore::default()),
        Arc::new(MediaTap::default()),
        Arc::new(RuntimeMetrics::default()),
    ))
}

fn first_candidate_port(offer_sdp: &str) -> Option<u16> {
    offer_sdp
        .lines()
        .find_map(|line| line.trim().strip_prefix("a=candidate:"))
        .and_then(|candidate| candidate.split_whitespace().nth(5))
        .and_then(|port| port.parse::<u16>().ok())
}

async fn create_offer_via_negotiation_port(
    negotiation_port: &impl NegotiationPort,
    session_key: &TransportSessionKey,
) -> Result<String, TransportAdapterError> {
    negotiation_port
        .create_initial_session_offer(session_key)
        .await
        .map(SessionOffer::into_sdp)
}

async fn publish_audio_via_media_port(
    media_port: &impl MediaPort,
    session_key: &TransportSessionKey,
    rtp_parameters: &MediaStream,
) -> Result<TransportMediaId, TransportAdapterError> {
    media_port
        .publish_media(session_key, MediaKind::Audio, rtp_parameters)
        .await
}

async fn observe_active_speakers(
    observability_port: &impl ObservabilityPort,
) -> Vec<ActiveSpeakerSource> {
    observability_port.active_speaker_source_snapshot().await
}

fn subscribe_source_policy(
    source_policy_port: &impl SourcePolicyPort,
) -> SourcePolicyUpdateSubscription {
    source_policy_port.source_policy_subscription()
}

#[test]
fn fake_adapter_projects_offered_capabilities_after_minimal_sdp_validation() {
    let adapter =
        RuntimeTransportAdapter::from_fake_adapter(Arc::new(FakeWebRtcAdapter::default()));
    let offered = sample_router_capabilities();

    let projected =
        adapter.negotiated_client_rtp_capabilities("v=0\r\ns=fake-answer\r\n", &offered);

    assert_eq!(projected, Ok(offered));
}

#[test]
fn fake_adapter_rejects_answers_without_minimal_sdp_shape() {
    let adapter =
        RuntimeTransportAdapter::from_fake_adapter(Arc::new(FakeWebRtcAdapter::default()));

    let projected =
        adapter.negotiated_client_rtp_capabilities("invalid-answer", &sample_router_capabilities());

    assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn runtime_transport_adapter_exposes_split_ports_to_callers() {
    let fake = Arc::new(FakeWebRtcAdapter::default());
    let adapter = RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&fake));
    let session_key = test_session_key(17, 0, 3, UserId::Integer(41));
    let audio_rtp_parameters = sample_audio_rtp_parameters("aud-up", 1234);

    let offer_result = create_offer_via_negotiation_port(&adapter, &session_key).await;
    assert!(offer_result.is_ok());
    let Ok(offer_sdp) = offer_result else {
        return;
    };
    assert!(offer_sdp.starts_with("v=0"));

    let publish_result =
        publish_audio_via_media_port(&adapter, &session_key, &audio_rtp_parameters).await;
    assert!(publish_result.is_ok());
    let Ok(published_media_id) = publish_result else {
        return;
    };

    let subscription = subscribe_source_policy(&adapter);
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        published_media_id,
        Instant::now(),
    )]);
    let update_result = timeout(Duration::from_millis(50), subscription.wait_for_update()).await;
    assert!(update_result.is_ok());
    let Ok(updated_runtime_ids) = update_result else {
        return;
    };
    let updated_runtime_ids = updated_runtime_ids.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(
        updated_runtime_ids,
        BTreeSet::from([session_key.room_instance_id()])
    );

    let active_speakers = observe_active_speakers(&adapter).await;
    assert_eq!(active_speakers.len(), 1);
    assert_eq!(
        active_speakers
            .first()
            .map(|active_speaker| active_speaker.transport_media_id()),
        Some(published_media_id)
    );
}

#[test]
fn rtc_adapter_rejects_answers_without_projectable_client_capabilities() {
    let adapter = RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        SessionBitrateLimits::new(8_000_000, 10_000_000),
        RtcPortRange::new(46_100, 46_199),
        1,
        MediaCodecFlags::default(),
        Arc::new(DiagnosticsStore::default()),
        Arc::new(MediaTap::default()),
        Arc::new(RuntimeMetrics::default()),
    ));

    let projected = adapter.negotiated_client_rtp_capabilities(
        "v=0\r\ns=invalid-answer\r\n",
        &sample_router_capabilities(),
    );

    assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn rtc_adapter_shards_room_bootstrap_by_explicit_media_worker() {
    let adapter = RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        SessionBitrateLimits::new(8_000_000, 10_000_000),
        RtcPortRange::new(46_000, 46_003),
        2,
        MediaCodecFlags::default(),
        Arc::new(DiagnosticsStore::default()),
        Arc::new(MediaTap::default()),
        Arc::new(RuntimeMetrics::default()),
    ));
    let first_room_session = test_session_key(10, 0, 1, UserId::Integer(1));
    let second_room_session = test_session_key(11, 1, 1, UserId::Integer(2));
    let same_shard_session = test_session_key(12, 0, 1, UserId::Integer(3));

    let first_offer = adapter
        .create_initial_session_offer(&first_room_session)
        .await;
    let second_offer = adapter
        .create_initial_session_offer(&second_room_session)
        .await;
    let same_shard_offer = adapter
        .create_initial_session_offer(&same_shard_session)
        .await;
    assert!(first_offer.is_ok());
    assert!(second_offer.is_ok());
    assert!(same_shard_offer.is_ok());
    let Some(first_offer) = first_offer.ok() else {
        return;
    };
    let Some(second_offer) = second_offer.ok() else {
        return;
    };
    let Some(same_shard_offer) = same_shard_offer.ok() else {
        return;
    };

    let Some(first_port) = first_candidate_port(&first_offer.into_sdp()) else {
        return;
    };
    let Some(second_port) = first_candidate_port(&second_offer.into_sdp()) else {
        return;
    };
    let Some(same_shard_port) = first_candidate_port(&same_shard_offer.into_sdp()) else {
        return;
    };

    assert!((46_000..=46_001).contains(&first_port));
    assert!((46_002..=46_003).contains(&second_port));
    assert_eq!(same_shard_port, first_port);
}

#[tokio::test]
async fn runtime_transport_semantic_facades_preserve_fake_transport_behavior() {
    let fake = Arc::new(FakeWebRtcAdapter::default());
    let adapter = RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&fake));
    let session_key = test_session_key(18, 0, 19, UserId::Integer(20));
    let speaker_source = ActiveSpeakerSource::new(TransportMediaId::new(77), Instant::now());
    fake.set_active_speaker_source_snapshot(vec![speaker_source]);

    let offer = adapter.create_initial_session_offer(&session_key).await;
    assert!(offer.is_ok());
    assert_eq!(
        adapter.negotiated_client_rtp_capabilities(
            "v=0\r\ns=fake-answer\r\n",
            &sample_router_capabilities()
        ),
        Ok(sample_router_capabilities())
    );

    let media_id = adapter
        .publish_media(
            &session_key,
            MediaKind::Audio,
            &sample_audio_rtp_parameters("aud-up", 40_000),
        )
        .await;
    assert!(media_id.is_ok());
    assert_eq!(
        adapter.active_speaker_source_snapshot().await,
        vec![speaker_source]
    );
    assert!(adapter.close_session(&session_key).await.is_ok());
}

#[tokio::test]
async fn fake_transport_source_policy_subscription_wakes_on_active_speaker_updates() {
    let fake = Arc::new(FakeWebRtcAdapter::default());
    let adapter = RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&fake));
    let subscription = adapter.source_policy_subscription();
    let dirty_room_instance_id = RoomInstanceId::from_raw(27);

    fake.mark_source_policy_dirty(dirty_room_instance_id);

    let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert_eq!(updates.ok(), Some(BTreeSet::from([dirty_room_instance_id])));
}

#[tokio::test]
async fn rtc_adapter_registers_and_prunes_cross_worker_remote_sources() {
    let adapter = test_rtc_adapter(2, RtcPortRange::new(46_200, 46_299));
    let source_session = test_session_key(20, 0, 1, UserId::Integer(1));
    let consumer_session = test_session_key(20, 1, 2, UserId::Integer(2));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 41_000);
    let consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down", 42_000);

    prepare_rtc_session(&adapter, &source_session).await;
    prepare_rtc_session(&adapter, &consumer_session).await;

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
    let source_shard = shards.shard_for_user(&source_session);
    let consumer_shard = shards.shard_for_user(&consumer_session);

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
    let adapter = RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        SessionBitrateLimits::new(8_000_000, 10_000_000),
        RtcPortRange::new(46_300, 46_599),
        3,
        MediaCodecFlags::default(),
        Arc::new(DiagnosticsStore::default()),
        Arc::new(MediaTap::default()),
        Arc::new(RuntimeMetrics::default()),
    ));
    let source_session = test_session_key(30, 0, 1, UserId::Integer(1));
    let first_consumer_session = test_session_key(30, 1, 2, UserId::Integer(2));
    let second_consumer_session = test_session_key(30, 2, 3, UserId::Integer(3));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 51_000);
    let first_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-1", 52_000);
    let second_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-2", 53_000);

    prepare_rtc_sessions(
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
    let source_shard = shards.shard_for_user(&source_session);
    let first_consumer_shard = shards.shard_for_user(&first_consumer_session);
    let second_consumer_shard = shards.shard_for_user(&second_consumer_session);

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
async fn rtc_adapter_rejects_stale_session_removal_without_dropping_consumer_handle() {
    let adapter = test_rtc_adapter(1, RtcPortRange::new(46_600, 46_649));
    let source_session = test_session_key(35, 0, 1, UserId::Integer(1));
    let consumer_session = test_session_key(35, 0, 2, UserId::Integer(2));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 54_000);
    let consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down", 55_000);

    prepare_rtc_sessions(&adapter, &[&source_session, &consumer_session]).await;

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

    assert_eq!(
        adapter
            .remove_media(&source_session, consumer_media_id)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    let route_entry = adapter.debug_route_entry_by_media_id(source_media_id).await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
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
    assert!(
        adapter
            .debug_route_entry_by_media_id(source_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn rtc_adapter_gates_remote_relay_mailboxes_without_touching_local_routes() {
    let adapter = test_rtc_adapter(2, RtcPortRange::new(46_600, 46_699));
    let source_session = test_session_key(40, 0, 1, UserId::Integer(1));
    let local_consumer_session = test_session_key(40, 0, 2, UserId::Integer(2));
    let remote_consumer_session = test_session_key(40, 1, 3, UserId::Integer(3));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 61_000);
    let local_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-local", 62_000);
    let remote_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-remote", 63_000);

    prepare_rtc_sessions(
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
    let source_shard = shards.shard_for_user(&source_session);
    let remote_consumer_shard = shards.shard_for_user(&remote_consumer_session);

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
