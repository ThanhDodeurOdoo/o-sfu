use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};

use o_sfu_router::{
    MediaCapabilities as RouterRtpCapabilities, MediaCodecCapability, MediaKind,
    MediaKind as RouterMediaKind, MediaStream, StreamBinding,
};
use str0m::media::Mid;
use tokio::time::timeout;

use super::{MediaTransport, RtcTransport, RtcTransportBuildError, RtcTransportBuilder};
use crate::{
    Bitrate, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        ConnectionId, RoomInstanceId, UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{
            ConsumerActivity, MediaTransportDeps, RelayRouteActivity, RtcTransportConfig,
            TransportAdapterError, TransportConsumerRoute, TransportMediaId,
            TransportRelayRouteAction, TransportRelayRouteEffect, TransportSessionKey,
            test_support::FakeMediaTransport,
        },
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        rtc_engine::RtcTransportWorker,
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

fn test_rtc_builder(rtc_port_range: RtcPortRange) -> RtcTransportBuilder {
    RtcTransport::builder()
        .transport_config(RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: crate::VideoBitrateLimits::default(),
            rtc_port_range,
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: crate::CodecPreferences::default(),
        })
        .deps(MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        })
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

async fn prepare_rtc_session(adapter: &MediaTransport, session_key: &TransportSessionKey) {
    assert!(
        adapter
            .create_initial_session_offer(session_key)
            .await
            .is_ok()
    );
}

async fn prepare_rtc_sessions(adapter: &MediaTransport, session_keys: &[&TransportSessionKey]) {
    for session_key in session_keys {
        prepare_rtc_session(adapter, session_key).await;
    }
}

async fn publish_audio(
    adapter: &MediaTransport,
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
    adapter: &MediaTransport,
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

async fn apply_relay_route_effect(
    adapter: &MediaTransport,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    target_session_key: &TransportSessionKey,
    action: TransportRelayRouteAction,
) {
    let effect = TransportRelayRouteEffect {
        source_session_key: source_session_key.clone(),
        source_transport_media_id: source_media_id,
        target_media_worker_id: target_session_key.media_worker_id(),
        action,
    };
    assert!(adapter.apply_relay_route_effect(&effect).await.is_ok());
}

async fn install_active_relay_route(
    adapter: &MediaTransport,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    target_session_key: &TransportSessionKey,
) {
    apply_relay_route_effect(
        adapter,
        source_session_key,
        source_media_id,
        target_session_key,
        TransportRelayRouteAction::Install,
    )
    .await;
    apply_relay_route_effect(
        adapter,
        source_session_key,
        source_media_id,
        target_session_key,
        TransportRelayRouteAction::SetActivity(RelayRouteActivity::Active),
    )
    .await;
}

async fn remove_consumer_media(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
    consumer_media_id: TransportMediaId,
) {
    assert!(
        adapter
            .remove_media(session_key, consumer_media_id)
            .await
            .is_ok()
    );
}

async fn apply_release_effect_and_assert_counts(
    adapter: &MediaTransport,
    source_worker: &RtcTransportWorker,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    target_session_key: &TransportSessionKey,
    total: usize,
    active: usize,
) {
    apply_relay_route_effect(
        adapter,
        source_session_key,
        source_media_id,
        target_session_key,
        TransportRelayRouteAction::Release,
    )
    .await;
    assert_relay_target_counts(source_worker, source_media_id, total, active).await;
}

async fn set_remote_relay_and_consumer_active(
    adapter: &MediaTransport,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    source_media_id: TransportMediaId,
    active: bool,
) {
    apply_relay_route_effect(
        adapter,
        source_session,
        source_media_id,
        remote_consumer_session,
        TransportRelayRouteAction::SetActivity(RelayRouteActivity::from_active(active)),
    )
    .await;
    let route = TransportConsumerRoute::new(
        remote_consumer_session.clone(),
        remote_consumer_media_id,
        source_session.clone(),
        source_media_id,
    );
    assert!(
        adapter
            .set_consumer_active(&route, ConsumerActivity::from_active(active))
            .await
            .is_ok()
    );
}

async fn assert_relay_target_counts(
    source_worker: &RtcTransportWorker,
    source_media_id: TransportMediaId,
    total: usize,
    active: usize,
) {
    assert_eq!(
        source_worker
            .debug_relay_target_count_for_source(source_media_id)
            .await,
        total
    );
    assert_eq!(
        source_worker
            .debug_active_relay_target_count_for_source(source_media_id)
            .await,
        active
    );
}

async fn assert_local_route_active(
    source_worker: &RtcTransportWorker,
    source_session: &TransportSessionKey,
    local_consumer_session: &TransportSessionKey,
    local_consumer_media_id: TransportMediaId,
) {
    let local_route_entry = source_worker
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
    consumer_worker: &RtcTransportWorker,
    source_media_id: TransportMediaId,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
    active: bool,
) {
    let remote_route_entry = consumer_worker
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

#[allow(
    clippy::panic,
    reason = "media transport tests use fixed valid RTC ranges and should fail immediately if their fixture is invalid"
)]
fn test_rtc_engine(worker_count: usize, rtc_port_range: RtcPortRange) -> MediaTransport {
    match test_rtc_builder(rtc_port_range)
        .worker_count(worker_count)
        .build()
    {
        Ok(transport) => MediaTransport::from_rtc_transport(transport),
        Err(error) => panic!("fixed RTC transport test config should be valid: {error}"),
    }
}

fn first_candidate_port(offer_sdp: &str) -> Option<u16> {
    offer_sdp
        .lines()
        .find_map(|line| line.trim().strip_prefix("a=candidate:"))
        .and_then(|candidate| candidate.split_whitespace().nth(5))
        .and_then(|port| port.parse::<u16>().ok())
}

#[test]
fn rtc_transport_builder_uses_one_worker_by_default() {
    let result = test_rtc_builder(RtcPortRange::new(46_200, 46_200)).build();

    assert!(result.is_ok());
}

#[test]
fn rtc_transport_builder_rejects_invalid_worker_count() {
    let result = test_rtc_builder(RtcPortRange::new(46_210, 46_211))
        .worker_count(0)
        .build();

    assert_eq!(
        result.err(),
        Some(RtcTransportBuildError::InvalidWorkerCount)
    );
}

#[test]
fn rtc_transport_builder_rejects_invalid_port_split() {
    let result = test_rtc_builder(RtcPortRange::new(46_220, 46_221))
        .worker_count(3)
        .build();

    assert_eq!(
        result.err(),
        Some(RtcTransportBuildError::InvalidPortSplit {
            worker_count: 3,
            port_count: 2,
        })
    );
}

#[tokio::test]
async fn fake_backend_failures_surface_through_media_transport_ports() {
    let fake = Arc::new(FakeMediaTransport::default());
    let adapter = MediaTransport::from_fake_transport(Arc::clone(&fake));
    let session_key = test_session_key(17, 0, 3, UserId::Integer(41));
    let offered = sample_router_capabilities();

    let projected =
        adapter.negotiated_client_rtp_capabilities("v=0\r\ns=fake-answer\r\n", &offered);

    assert_eq!(projected, Ok(offered));

    assert_eq!(
        adapter.negotiated_client_rtp_capabilities("invalid-answer", &sample_router_capabilities()),
        Err(TransportAdapterError::InvalidInput)
    );
    fake.fail_next_close_session(session_key.clone());
    assert_eq!(
        adapter.close_session(&session_key).await,
        Err(TransportAdapterError::TransportUnavailable)
    );

    let media_id = adapter
        .publish_media(
            &session_key,
            MediaKind::Audio,
            &sample_audio_rtp_parameters("aud-up", 1234),
        )
        .await;
    assert!(media_id.is_ok());
    let Ok(media_id) = media_id else {
        return;
    };
    assert_eq!(adapter.remove_media(&session_key, media_id).await, Ok(()));

    let second_media_id = adapter
        .publish_media(
            &session_key,
            MediaKind::Audio,
            &sample_audio_rtp_parameters("aud-up-2", 1235),
        )
        .await;
    assert!(second_media_id.is_ok());
    let Ok(second_media_id) = second_media_id else {
        return;
    };
    fake.fail_next_remove_media(second_media_id);
    assert_eq!(
        adapter.remove_media(&session_key, second_media_id).await,
        Err(TransportAdapterError::TransportUnavailable)
    );
}

async fn assert_remote_route_inactive(
    remote_consumer_worker: &RtcTransportWorker,
    source_media_id: TransportMediaId,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
) {
    assert_remote_route_activity(
        remote_consumer_worker,
        source_media_id,
        remote_consumer_session,
        remote_consumer_media_id,
        false,
    )
    .await;
}

async fn assert_local_active_and_remote_inactive(
    source_worker: &RtcTransportWorker,
    remote_consumer_worker: &RtcTransportWorker,
    source_session: &TransportSessionKey,
    source_media_id: TransportMediaId,
    local_consumer: (&TransportSessionKey, TransportMediaId),
    remote_consumer: (&TransportSessionKey, TransportMediaId),
) {
    assert_local_route_active(
        source_worker,
        source_session,
        local_consumer.0,
        local_consumer.1,
    )
    .await;
    assert_remote_route_inactive(
        remote_consumer_worker,
        source_media_id,
        remote_consumer.0,
        remote_consumer.1,
    )
    .await;
}

#[test]
fn rtc_engine_rejects_answers_without_projectable_client_capabilities() {
    let adapter = test_rtc_engine(1, RtcPortRange::new(46_100, 46_199));

    let projected = adapter.negotiated_client_rtp_capabilities(
        "v=0\r\ns=invalid-answer\r\n",
        &sample_router_capabilities(),
    );

    assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn rtc_engine_workers_room_bootstrap_by_explicit_media_worker() {
    let adapter = test_rtc_engine(2, RtcPortRange::new(46_000, 46_003));
    let first_room_session = test_session_key(10, 0, 1, UserId::Integer(1));
    let second_room_session = test_session_key(11, 1, 1, UserId::Integer(2));
    let same_worker_session = test_session_key(12, 0, 1, UserId::Integer(3));

    let first_offer = adapter
        .create_initial_session_offer(&first_room_session)
        .await;
    let second_offer = adapter
        .create_initial_session_offer(&second_room_session)
        .await;
    let same_worker_offer = adapter
        .create_initial_session_offer(&same_worker_session)
        .await;
    assert!(first_offer.is_ok());
    assert!(second_offer.is_ok());
    assert!(same_worker_offer.is_ok());
    let Some(first_offer) = first_offer.ok() else {
        return;
    };
    let Some(second_offer) = second_offer.ok() else {
        return;
    };
    let Some(same_worker_offer) = same_worker_offer.ok() else {
        return;
    };

    let Some(first_port) = first_candidate_port(&first_offer.into_sdp()) else {
        return;
    };
    let Some(second_port) = first_candidate_port(&second_offer.into_sdp()) else {
        return;
    };
    let Some(same_worker_port) = first_candidate_port(&same_worker_offer.into_sdp()) else {
        return;
    };

    assert!((46_000..=46_001).contains(&first_port));
    assert!((46_002..=46_003).contains(&second_port));
    assert_eq!(same_worker_port, first_port);
}

#[tokio::test]
async fn rtc_engine_allocates_disjoint_media_ids_across_workers() {
    let adapter = test_rtc_engine(2, RtcPortRange::new(46_700, 46_799));
    let first_source = test_session_key(50, 0, 1, UserId::Integer(1));
    let second_source = test_session_key(50, 1, 2, UserId::Integer(2));
    let first_rtp_parameters = sample_audio_rtp_parameters("first-aud-up", 71_000);
    let second_rtp_parameters = sample_audio_rtp_parameters("second-aud-up", 72_000);

    prepare_rtc_sessions(&adapter, &[&first_source, &second_source]).await;

    let Some(first_media_id) = publish_audio(&adapter, &first_source, &first_rtp_parameters).await
    else {
        return;
    };
    let Some(second_media_id) =
        publish_audio(&adapter, &second_source, &second_rtp_parameters).await
    else {
        return;
    };

    assert_ne!(first_media_id, second_media_id);
    assert!(first_media_id.as_u64() < 1_000_000_000);
    assert!(second_media_id.as_u64() >= 1_000_000_000);
}

#[tokio::test]
async fn fake_transport_source_policy_subscription_wakes_on_active_speaker_updates() {
    let fake = Arc::new(FakeMediaTransport::default());
    let adapter = MediaTransport::from_fake_transport(Arc::clone(&fake));
    let subscription = adapter.source_policy_subscription();
    let dirty_room_instance_id = RoomInstanceId::from_raw(27);

    fake.mark_source_policy_dirty(dirty_room_instance_id);

    let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert_eq!(updates.ok(), Some(BTreeSet::from([dirty_room_instance_id])));
}

#[tokio::test]
async fn rtc_engine_rejects_stale_session_removal_without_dropping_consumer_handle() {
    let adapter = test_rtc_engine(1, RtcPortRange::new(46_600, 46_649));
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
async fn rtc_engine_gates_remote_relay_mailboxes_without_touching_local_routes() {
    let adapter = test_rtc_engine(2, RtcPortRange::new(46_600, 46_699));
    let source_session = test_session_key(40, 0, 1, UserId::Integer(1));
    let local_consumer_session = test_session_key(40, 0, 2, UserId::Integer(2));
    let remote_consumer_session = test_session_key(40, 1, 3, UserId::Integer(3));
    let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 61_000);
    let local_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-local", 62_000);
    let remote_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-remote", 63_000);

    let sessions = [
        &source_session,
        &local_consumer_session,
        &remote_consumer_session,
    ];
    prepare_rtc_sessions(&adapter, &sessions).await;

    let Some(source_media_id) =
        publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
    else {
        return;
    };
    install_active_relay_route(
        &adapter,
        &source_session,
        source_media_id,
        &remote_consumer_session,
    )
    .await;
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

    let Some(worker_manager) = adapter.as_rtc_worker_manager() else {
        return;
    };
    let (Some(source_worker), Some(remote_consumer_worker)) = (
        worker_manager.worker_for_user(&source_session),
        worker_manager.worker_for_user(&remote_consumer_session),
    ) else {
        return;
    };

    assert_relay_target_counts(source_worker.as_ref(), source_media_id, 1, 1).await;

    set_remote_relay_and_consumer_active(
        &adapter,
        &remote_consumer_session,
        remote_consumer_media_id,
        &source_session,
        source_media_id,
        false,
    )
    .await;

    assert_relay_target_counts(source_worker.as_ref(), source_media_id, 1, 0).await;
    assert_local_active_and_remote_inactive(
        source_worker.as_ref(),
        remote_consumer_worker.as_ref(),
        &source_session,
        source_media_id,
        (&local_consumer_session, local_consumer_media_id),
        (&remote_consumer_session, remote_consumer_media_id),
    )
    .await;

    set_remote_relay_and_consumer_active(
        &adapter,
        &remote_consumer_session,
        remote_consumer_media_id,
        &source_session,
        source_media_id,
        true,
    )
    .await;
    assert_relay_target_counts(source_worker.as_ref(), source_media_id, 1, 1).await;

    remove_consumer_media(&adapter, &remote_consumer_session, remote_consumer_media_id).await;
    assert_relay_target_counts(source_worker.as_ref(), source_media_id, 1, 1).await;
    apply_release_effect_and_assert_counts(
        &adapter,
        source_worker.as_ref(),
        &source_session,
        source_media_id,
        &remote_consumer_session,
        0,
        0,
    )
    .await;
}
