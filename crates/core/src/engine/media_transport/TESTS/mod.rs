#![allow(
    clippy::panic,
    reason = "media transport tests fail loudly when fixed test setup is invalid"
)]

use std::sync::Arc;

use o_sfu_router::{MediaKind, MediaStream, StreamBinding};
use str0m::media::Mid;

use super::{MediaTransport, MediaTransportBuildError};
use crate::{
    MediaWorkerId, RtcPortRange,
    engine::{
        ConnectionId, RoomInstanceId, UserId,
        media_transport::{
            ConsumerActivity, RelayRouteActivity, SessionOffer, SourcePacketGate,
            TransportAdapterError, TransportConsumerRoute, TransportMediaId,
            TransportRelayRouteAction, TransportRelayRouteEffect, TransportSessionKey,
            TransportSourceKey,
            rtc::RtcWorker,
            test_support::{DebugPacketGate, test_media_transport_builder, test_rtc_port_range},
        },
        metrics::test_support::RuntimeMetricsSnapshotTestExt,
    },
};
#[cfg(not(target_os = "linux"))]
use crate::{
    RtcUdpIoBackend,
    engine::media_transport::test_support::{
        test_media_transport_config, test_media_transport_deps,
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
        MediaWorkerId::from_raw(media_worker_id),
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}

fn sample_rtp_parameters(mid: &str, ssrc: u32) -> MediaStream {
    MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
        .with_mid(String::from(mid))
}

async fn prepare_rtc_sessions(adapter: &MediaTransport, session_keys: &[&TransportSessionKey]) {
    for session_key in session_keys {
        let _offer = expect_initial_offer(adapter, session_key).await;
    }
}

async fn publish_audio(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
    rtp_parameters: &MediaStream,
) -> TransportMediaId {
    adapter
        .publish_media(session_key, MediaKind::Audio, rtp_parameters)
        .await
        .unwrap_or_else(|error| panic!("test audio publication should succeed: {error:?}"))
}

async fn consume_audio(
    adapter: &MediaTransport,
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    rtp_parameters: &MediaStream,
) -> TransportMediaId {
    adapter
        .consume_media(
            consumer_session_key,
            MediaKind::Audio,
            source_session_key,
            source_media_id,
            rtp_parameters,
            ConsumerActivity::Active,
        )
        .await
        .unwrap_or_else(|error| panic!("test audio consumption should succeed: {error:?}"))
}

async fn apply_relay_route_effect(
    adapter: &MediaTransport,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    target_session_key: &TransportSessionKey,
    action: TransportRelayRouteAction,
) {
    let effect = TransportRelayRouteEffect {
        source: TransportSourceKey::new(source_session_key.clone(), source_media_id),
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
        TransportSourceKey::new(source_session.clone(), source_media_id),
    );
    assert!(
        adapter
            .set_consumer_active(&route, ConsumerActivity::from_active(active))
            .await
            .is_ok()
    );
}

async fn assert_relay_target_counts(
    source_worker: &RtcWorker,
    source_media_id: TransportMediaId,
    total: usize,
    active: usize,
) {
    assert_eq!(
        source_worker
            .debug_relay_target_count(source_media_id)
            .await,
        total
    );
    assert_eq!(
        source_worker
            .debug_active_relay_target_count(source_media_id)
            .await,
        active
    );
}

async fn assert_local_route_active(
    source_worker: &RtcWorker,
    source_session: &TransportSessionKey,
    local_consumer_session: &TransportSessionKey,
    local_consumer_media_id: TransportMediaId,
) {
    let Some(local_route_entry) = source_worker
        .debug_route_entry(source_session, Mid::from("aud-up"))
        .await
    else {
        panic!("local route entry should exist");
    };
    assert!(local_route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *local_consumer_session
            && destination.dest_transport_media_id == local_consumer_media_id
            && destination.active
    }));
}

async fn assert_remote_route_activity(
    consumer_worker: &RtcWorker,
    source_media_id: TransportMediaId,
    remote_consumer_session: &TransportSessionKey,
    remote_consumer_media_id: TransportMediaId,
    active: bool,
) {
    let Some(remote_route_entry) = consumer_worker
        .debug_route_entry_by_media_id(source_media_id)
        .await
    else {
        panic!("remote route entry should exist");
    };
    assert!(remote_route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *remote_consumer_session
            && destination.dest_transport_media_id == remote_consumer_media_id
            && destination.active == active
    }));
}

fn test_media_transport(worker_count: usize, rtc_port_range: RtcPortRange) -> MediaTransport {
    match test_media_transport_builder(rtc_port_range)
        .worker_count(worker_count)
        .build()
    {
        Ok(transport) => transport,
        Err(error) => panic!("fixed RTC transport test config should be valid: {error}"),
    }
}

fn test_rtc_range(worker_count: usize) -> RtcPortRange {
    test_rtc_port_range(worker_count)
        .unwrap_or_else(|| panic!("test RTC port range should be available"))
}

fn expect_first_candidate_port(offer_sdp: &str) -> u16 {
    offer_sdp
        .lines()
        .find_map(|line| line.trim().strip_prefix("a=candidate:"))
        .and_then(|candidate| candidate.split_whitespace().nth(5))
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("RTC offer should include a parseable ICE candidate port"))
}

fn expect_worker_for_user(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
) -> Arc<RtcWorker> {
    let Some(worker) = adapter.test_api().worker_for_user(session_key) else {
        panic!("test session should be assigned to a media worker");
    };
    worker
}

async fn expect_initial_offer(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    adapter
        .create_initial_session_offer(session_key)
        .await
        .unwrap_or_else(|error| panic!("test session should create an RTC offer: {error:?}"))
}

#[test]
fn media_transport_builder_uses_one_worker_by_default() {
    let result = test_media_transport_builder(test_rtc_range(1)).build();

    assert!(result.is_ok());
}

#[tokio::test]
async fn media_transport_close_session_without_packet_loop_does_not_start_worker() {
    let adapter = test_media_transport(1, test_rtc_range(1));
    let session_key = test_session_key(1, 0, 11, UserId::Integer(11));
    let worker = expect_worker_for_user(&adapter, &session_key);

    assert!(!worker.packet_loop_started());
    assert!(adapter.close_session(&session_key).await.is_ok());
    assert!(!worker.packet_loop_started());
}

#[tokio::test]
async fn media_transport_route_control_commands_update_source_route() {
    let adapter = test_media_transport(1, test_rtc_range(1));
    let source_session = test_session_key(60, 0, 1, UserId::Integer(1));
    let consumer_session = test_session_key(60, 0, 2, UserId::Integer(2));
    let source_rtp_parameters = sample_rtp_parameters("cam-up", 81_000);
    let consumer_rtp_parameters = sample_rtp_parameters("cam-down", 82_000);

    prepare_rtc_sessions(&adapter, &[&source_session, &consumer_session]).await;

    let source_media_id = adapter
        .publish_media(&source_session, MediaKind::Video, &source_rtp_parameters)
        .await
        .unwrap_or_else(|error| panic!("test video publication should succeed: {error:?}"));
    let consumer_media_id = adapter
        .consume_media(
            &consumer_session,
            MediaKind::Video,
            &source_session,
            source_media_id,
            &consumer_rtp_parameters,
            ConsumerActivity::Active,
        )
        .await
        .unwrap_or_else(|error| panic!("test video consumption should succeed: {error:?}"));
    let route = TransportConsumerRoute::new(
        consumer_session,
        consumer_media_id,
        TransportSourceKey::new(source_session, source_media_id),
    );
    let source_worker = expect_worker_for_user(&adapter, route.source_session_key());
    let forwarded_keyframes = source_worker
        .metrics
        .snapshot()
        .rtc_keyframe_requests_forwarded();

    assert!(adapter.request_consumer_keyframe(&route).await.is_ok());
    assert_eq!(
        source_worker
            .metrics
            .snapshot()
            .rtc_keyframe_requests_forwarded(),
        forwarded_keyframes + 1
    );

    assert!(
        adapter
            .set_consumer_packet_gate(&route, SourcePacketGate::Rid("lo".into()))
            .await
            .is_ok()
    );

    let route_entry = adapter
        .test_api()
        .route_entry_by_media_id(route.source_transport_media_id())
        .await
        .unwrap_or_else(|| panic!("video route should survive packet gate update"));
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);
}

#[test]
fn media_transport_builder_rejects_invalid_worker_count() {
    let result = test_media_transport_builder(RtcPortRange::new(46_210, 46_211))
        .worker_count(0)
        .build();

    assert_eq!(
        result.err(),
        Some(MediaTransportBuildError::InvalidWorkerCount)
    );
}

#[test]
fn media_transport_builder_rejects_invalid_port_split() {
    let result = test_media_transport_builder(RtcPortRange::new(46_220, 46_221))
        .worker_count(3)
        .build();

    assert_eq!(
        result.err(),
        Some(MediaTransportBuildError::InvalidPortSplit {
            worker_count: 3,
            port_count: 2,
        })
    );
}

#[cfg(not(target_os = "linux"))]
#[test]
fn media_transport_builder_rejects_non_linux_io_uring_backend() {
    let mut config = test_media_transport_config(test_rtc_range(1));
    config.rtc_udp_io_backend = RtcUdpIoBackend::IoUring;

    let result = MediaTransport::builder()
        .transport_config(config)
        .deps(test_media_transport_deps())
        .build();

    assert_eq!(
        result.err(),
        Some(MediaTransportBuildError::UnsupportedUdpIoBackend {
            backend: RtcUdpIoBackend::IoUring,
        })
    );
}

#[test]
fn rtc_rejects_answers_without_projectable_client_capabilities() {
    let projected =
        super::fuzz_support::client_rtp_capabilities_from_answer("v=0\r\ns=invalid-answer\r\n");

    assert_eq!(projected, None);
}

#[test]
fn transport_session_key_exposes_typed_media_worker_id() {
    let session_key = test_session_key(10, 0, 1, UserId::Integer(1));
    let media_worker_id: MediaWorkerId = session_key.media_worker_id();

    assert_eq!(media_worker_id, MediaWorkerId::from_raw(0));
}

#[tokio::test]
async fn rtc_workers_room_bootstrap_by_explicit_media_worker() {
    let rtc_port_range = test_rtc_range(2);
    let adapter = test_media_transport(2, rtc_port_range);
    let first_room_session = test_session_key(10, 0, 1, UserId::Integer(1));
    let second_room_session = test_session_key(11, 1, 1, UserId::Integer(2));
    let same_worker_session = test_session_key(12, 0, 1, UserId::Integer(3));

    let first_offer = expect_initial_offer(&adapter, &first_room_session).await;
    let second_offer = expect_initial_offer(&adapter, &second_room_session).await;
    let same_worker_offer = expect_initial_offer(&adapter, &same_worker_session).await;

    let first_port = expect_first_candidate_port(&first_offer.into_sdp());
    let second_port = expect_first_candidate_port(&second_offer.into_sdp());
    let same_worker_port = expect_first_candidate_port(&same_worker_offer.into_sdp());

    let mut worker_ranges = rtc_port_range
        .split_for_workers(2)
        .unwrap_or_else(|| panic!("test RTC range should split across workers"))
        .into_iter();
    let first_worker_range = worker_ranges
        .next()
        .unwrap_or_else(|| panic!("first worker range should exist"));
    let second_worker_range = worker_ranges
        .next()
        .unwrap_or_else(|| panic!("second worker range should exist"));
    assert!(first_worker_range.ports().any(|port| port == first_port));
    assert!(second_worker_range.ports().any(|port| port == second_port));
    assert_eq!(same_worker_port, first_port);
}

#[tokio::test]
async fn rtc_rejects_noncanonical_media_worker_id() {
    let adapter = test_media_transport(2, test_rtc_range(2));
    let session = test_session_key(13, 2, 1, UserId::Integer(4));

    let offer = adapter.create_initial_session_offer(&session).await;

    assert_eq!(
        offer.err(),
        Some(TransportAdapterError::TransportUnavailable)
    );
}

#[tokio::test]
async fn rtc_allocates_disjoint_media_ids_across_workers() {
    let adapter = test_media_transport(2, test_rtc_range(2));
    let first_source = test_session_key(50, 0, 1, UserId::Integer(1));
    let second_source = test_session_key(50, 1, 2, UserId::Integer(2));
    let first_rtp_parameters = sample_rtp_parameters("first-aud-up", 71_000);
    let second_rtp_parameters = sample_rtp_parameters("second-aud-up", 72_000);

    prepare_rtc_sessions(&adapter, &[&first_source, &second_source]).await;

    let first_media_id = publish_audio(&adapter, &first_source, &first_rtp_parameters).await;
    let second_media_id = publish_audio(&adapter, &second_source, &second_rtp_parameters).await;

    assert_ne!(first_media_id, second_media_id);
    assert!(first_media_id.as_u64() < 1_000_000_000);
    assert!(second_media_id.as_u64() >= 1_000_000_000);
}

#[tokio::test]
async fn rtc_rejects_stale_session_removal_without_dropping_consumer_handle() {
    let adapter = test_media_transport(1, test_rtc_range(1));
    let source_session = test_session_key(35, 0, 1, UserId::Integer(1));
    let consumer_session = test_session_key(35, 0, 2, UserId::Integer(2));
    let producer_rtp_parameters = sample_rtp_parameters("aud-up", 54_000);
    let consumer_rtp_parameters = sample_rtp_parameters("aud-down", 55_000);

    prepare_rtc_sessions(&adapter, &[&source_session, &consumer_session]).await;

    let source_media_id = publish_audio(&adapter, &source_session, &producer_rtp_parameters).await;
    let consumer_media_id = consume_audio(
        &adapter,
        &consumer_session,
        &source_session,
        source_media_id,
        &consumer_rtp_parameters,
    )
    .await;

    assert_eq!(
        adapter
            .remove_media(&source_session, consumer_media_id)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    let Some(route_entry) = adapter
        .test_api()
        .route_entry_by_media_id(source_media_id)
        .await
    else {
        panic!("source route entry should survive stale removal");
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
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn rtc_gates_remote_relay_mailboxes_without_touching_local_routes() {
    let adapter = test_media_transport(2, test_rtc_range(2));
    let source_session = test_session_key(40, 0, 1, UserId::Integer(1));
    let local_consumer_session = test_session_key(40, 0, 2, UserId::Integer(2));
    let remote_consumer_session = test_session_key(40, 1, 3, UserId::Integer(3));
    let producer_rtp_parameters = sample_rtp_parameters("aud-up", 61_000);
    let local_consumer_rtp_parameters = sample_rtp_parameters("aud-down-local", 62_000);
    let remote_consumer_rtp_parameters = sample_rtp_parameters("aud-down-remote", 63_000);

    let sessions = [
        &source_session,
        &local_consumer_session,
        &remote_consumer_session,
    ];
    prepare_rtc_sessions(&adapter, &sessions).await;

    let source_media_id = publish_audio(&adapter, &source_session, &producer_rtp_parameters).await;
    install_active_relay_route(
        &adapter,
        &source_session,
        source_media_id,
        &remote_consumer_session,
    )
    .await;
    let local_consumer_media_id = consume_audio(
        &adapter,
        &local_consumer_session,
        &source_session,
        source_media_id,
        &local_consumer_rtp_parameters,
    )
    .await;
    let remote_consumer_media_id = consume_audio(
        &adapter,
        &remote_consumer_session,
        &source_session,
        source_media_id,
        &remote_consumer_rtp_parameters,
    )
    .await;

    let source_worker = expect_worker_for_user(&adapter, &source_session);
    let remote_consumer_worker = expect_worker_for_user(&adapter, &remote_consumer_session);

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
    assert_local_route_active(
        source_worker.as_ref(),
        &source_session,
        &local_consumer_session,
        local_consumer_media_id,
    )
    .await;
    assert_remote_route_activity(
        remote_consumer_worker.as_ref(),
        source_media_id,
        &remote_consumer_session,
        remote_consumer_media_id,
        false,
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

    assert!(
        adapter
            .remove_media(&remote_consumer_session, remote_consumer_media_id)
            .await
            .is_ok()
    );
    assert_relay_target_counts(source_worker.as_ref(), source_media_id, 1, 1).await;
    apply_relay_route_effect(
        &adapter,
        &source_session,
        source_media_id,
        &remote_consumer_session,
        TransportRelayRouteAction::Release,
    )
    .await;
    assert_relay_target_counts(source_worker.as_ref(), source_media_id, 0, 0).await;
}
