use std::{collections::BTreeSet, time::Instant};

use tokio::sync::Notify;

use super::fixtures::*;
use crate::{
    Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        diagnostics::DiagnosticsStore,
        media_transport::{
            RelayRouteActivity, TransportPlacementPressureSnapshot, TransportRelayRouteAction,
            TransportRelayRouteAction::{Install, Release, SetActivity},
        },
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

fn spillover_room_manager(local_router_count: usize) -> RoomManager {
    RoomManager::for_test_with_config(super::super::RoomManagerConfig::new(
        local_router_count,
        super::super::RoomRuntimePolicy::new(
            super::super::RoomAdmissionPolicy::new(100),
            crate::RuntimeFeatureFlags::default(),
            super::super::rtp_capabilities::router_rtp_capabilities(
                crate::MediaCodecFlags::default(),
            ),
        )
        .with_room_worker_policy(crate::RoomWorkerPolicy::bounded_local_spillover(
            local_router_count,
        )),
    ))
}

fn load_spillover_room_manager(
    local_router_count: usize,
    policy: LocalSpilloverPolicy,
) -> RoomManager {
    RoomManager::for_test_with_config(super::super::RoomManagerConfig::new(
        local_router_count,
        super::super::RoomRuntimePolicy::new(
            super::super::RoomAdmissionPolicy::new(100),
            crate::RuntimeFeatureFlags::default(),
            super::super::rtp_capabilities::router_rtp_capabilities(
                crate::MediaCodecFlags::default(),
            ),
        )
        .with_room_worker_policy(crate::RoomWorkerPolicy::load_triggered_local_spillover(
            local_router_count,
            policy,
        )),
    ))
}

async fn assert_user_placement(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    expected_router_id: RouterId,
    expected_media_worker_id: usize,
) {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should be joined");
    };
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(user_id)
            .await,
        Some(expected_router_id)
    );
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_media_worker_id(user_id)
            .await,
        Some(expected_media_worker_id)
    );
    assert_eq!(
        room.transport_user_key(user_id, connection_id)
            .media_worker_id(),
        expected_media_worker_id
    );
}

async fn join_users_for_placement(
    room: &Arc<super::super::Room>,
    raw_user_ids: &[i64],
) -> Vec<(UserId, ConnectionId)> {
    let mut users = Vec::with_capacity(raw_user_ids.len());
    for raw_user_id in raw_user_ids {
        let user_id = UserId::Integer(*raw_user_id);
        let (tx, _rx) = test_sender();
        let connection_id = join_user_with_sender(room, user_id.clone(), tx).await;
        users.push((user_id, connection_id));
    }
    users
}

async fn join_ready_users(
    room: &Arc<super::super::Room>,
    raw_user_ids: &[i64],
) -> Vec<(UserId, ConnectionId)> {
    let users = join_users_for_placement(room, raw_user_ids).await;
    for (user_id, _connection_id) in &users {
        make_session_ready(room, user_id).await;
    }
    users
}

async fn manager_join_user(
    manager: &RoomManager,
    room: &Arc<super::super::Room>,
    raw_user_id: i64,
    media_transport: &MediaTransport,
) -> ConnectionId {
    let (tx, _rx) = test_sender();
    let (_room, connection_id) = manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: UserId::Integer(raw_user_id),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            media_transport,
        )
        .await
        .expect("user should join through manager");
    connection_id
}

async fn camera_media_id(room: &Arc<super::super::Room>, user_id: &UserId) -> TransportMediaId {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should have a connection");
    };
    room.test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, TestSourceKind::ScalableVideo)
        .await
        .expect("camera producer should expose a transport media id")
}

type RelayRoute<'a> = (&'a UserId, TransportMediaId, usize);

fn relay_matches(
    event: &FakeMediaTransportEvent,
    route: RelayRoute<'_>,
    action: TransportRelayRouteAction,
) -> bool {
    matches!(
        event,
        FakeMediaTransportEvent::RelayRouteEffectApplied {
            source_user_id: event_source_user_id,
            source_transport_media_id,
            target_media_worker_id: event_target_media_worker_id,
            action: event_action,
        } if event_source_user_id == route.0
            && *source_transport_media_id == route.1
            && *event_target_media_worker_id == route.2
            && *event_action == action
    )
}

fn relay_count(
    fake: &FakeMediaTransport,
    route: RelayRoute<'_>,
    action: TransportRelayRouteAction,
) -> usize {
    fake.snapshot_events()
        .iter()
        .filter(|event| relay_matches(event, route, action))
        .count()
}

fn assert_relay_count(
    fake: &FakeMediaTransport,
    route: RelayRoute<'_>,
    action: TransportRelayRouteAction,
    expected: usize,
) {
    assert_eq!(relay_count(fake, route, action), expected);
}

async fn set_camera_active(
    room: &Arc<super::super::Room>,
    consumer_id: &UserId,
    consumer_connection_id: ConnectionId,
    publisher_id: &UserId,
    active: bool,
    media_transport: &MediaTransport,
) {
    let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(active),
        ..TestSubscriptionStates::default()
    });
    room.update_subscription_runtime(
        consumer_id,
        consumer_connection_id,
        publisher_id,
        &intents,
        media_transport,
    )
    .await;
}

async fn wait_video(fake: &FakeMediaTransport, consumer_id: &UserId, publisher_id: &UserId) {
    wait_for_fake_event(fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id,
                source_user_id,
                media_kind: MediaKind::Video,
            } if consumer_user_id == consumer_id && source_user_id == publisher_id
        )
    })
    .await;
}

async fn wait_relay(
    fake: &FakeMediaTransport,
    route: RelayRoute<'_>,
    action: TransportRelayRouteAction,
) {
    wait_for_fake_event(fake, |event| relay_matches(event, route, action)).await;
}

async fn leave(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) {
    assert!(
        room.remove_user(user_id, connection_id, media_transport)
            .await
    );
}

fn joined_connection(users: &[(UserId, ConnectionId)], expected_user_id: &UserId) -> ConnectionId {
    users
        .iter()
        .find_map(|(user_id, connection_id)| {
            (user_id == expected_user_id).then_some(*connection_id)
        })
        .expect("user should have joined")
}

#[tokio::test]
async fn room_manager_is_idempotent_by_issuer() {
    let manager = RoomManager::for_test();
    let config = RoomConfig::default();
    let first = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &config, None)
        .await;
    let second = manager
        .serve_room("issuer-a", "ignored", &config, None)
        .await;
    let third = manager
        .serve_room("issuer-b", TEST_ROOM_KEY, &config, None)
        .await;
    assert_eq!(first.uuid(), second.uuid());
    assert_ne!(first.uuid(), third.uuid());
}

#[tokio::test]
async fn room_manager_concurrent_create_attempts_publish_one_live_room() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = Arc::new(RoomManager::new(
        super::super::RoomManagerConfig::new(
            1,
            super::super::RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(2),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        super::super::RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    ));
    let config = RoomConfig::default();

    let (first, second) = tokio::join!(
        manager.serve_room("issuer-a", TEST_ROOM_KEY, &config, None),
        manager.serve_room("issuer-a", TEST_ROOM_KEY, &config, None),
    );

    assert_eq!(first.uuid(), second.uuid());
    assert_eq!(metrics.snapshot().active_rooms(), 1);
}

#[tokio::test]
async fn room_manager_places_first_join_on_lowest_load_worker() {
    let manager = RoomManager::for_test_with_media_workers(2);
    let media_transport = MediaTransport::fake_for_testing();
    let first = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let second = manager
        .serve_room("issuer-b", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let third = manager
        .serve_room("issuer-c", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    assert_eq!(first.test_api().inspect().media_worker_id(), 0);
    assert_eq!(second.test_api().inspect().media_worker_id(), 0);
    assert_eq!(third.test_api().inspect().media_worker_id(), 0);

    let first_connection = manager_join_user(&manager, &first, 10, &media_transport).await;
    let second_connection = manager_join_user(&manager, &second, 20, &media_transport).await;
    let third_connection = manager_join_user(&manager, &third, 30, &media_transport).await;

    assert_eq!(
        first
            .transport_user_key(&UserId::Integer(10), first_connection)
            .media_worker_id(),
        0
    );
    assert_eq!(
        second
            .transport_user_key(&UserId::Integer(20), second_connection)
            .media_worker_id(),
        1
    );
    assert_eq!(
        third
            .transport_user_key(&UserId::Integer(30), third_connection)
            .media_worker_id(),
        0
    );
}

#[tokio::test]
async fn room_spillover_policy_places_user_transport_on_local_workers() {
    let manager = spillover_room_manager(2);
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    let first_connection = manager_join_user(&manager, &room, 10, &media_transport).await;
    let second_connection = manager_join_user(&manager, &room, 20, &media_transport).await;
    let first_key = room.transport_user_key(&UserId::Integer(10), first_connection);
    let second_key = room.transport_user_key(&UserId::Integer(20), second_connection);

    assert_eq!(first_key.media_worker_id(), 0);
    assert_eq!(second_key.media_worker_id(), 1);
}

#[tokio::test]
async fn strict_room_placement_keeps_topology_and_transport_on_primary_worker() {
    let manager = RoomManager::for_test_with_media_workers(3);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    join_users_for_placement(&room, &[10, 20, 30]).await;

    for raw_user_id in [10_i64, 20, 30] {
        assert_user_placement(&room, &UserId::Integer(raw_user_id), RouterId(0), 0).await;
    }
}

#[tokio::test]
async fn bounded_room_placement_keeps_topology_and_transport_aligned() {
    let manager = spillover_room_manager(3);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    join_users_for_placement(&room, &[10, 20, 30]).await;

    for (raw_user_id, router_id, media_worker_id) in [
        (10_i64, RouterId(0), 0),
        (20, RouterId(1), 1),
        (30, RouterId(2), 2),
    ] {
        assert_user_placement(
            &room,
            &UserId::Integer(raw_user_id),
            router_id,
            media_worker_id,
        )
        .await;
    }
}

#[tokio::test]
async fn load_triggered_room_placement_keeps_topology_and_transport_aligned()
-> Result<(), LocalSpilloverPolicyError> {
    let manager = load_spillover_room_manager(
        3,
        LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 2,
            activation_window: 1,
            ..LocalSpilloverPolicyParts::conservative()
        })?,
    );
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    join_users_for_placement(&room, &[10, 20, 30]).await;

    for (raw_user_id, router_id, media_worker_id) in [
        (10_i64, RouterId(0), 0),
        (20, RouterId(1), 1),
        (30, RouterId(2), 2),
    ] {
        assert_user_placement(
            &room,
            &UserId::Integer(raw_user_id),
            router_id,
            media_worker_id,
        )
        .await;
    }
    Ok(())
}

#[tokio::test]
async fn load_triggered_room_keeps_small_room_transport_on_primary_worker()
-> Result<(), LocalSpilloverPolicyError> {
    let manager = load_spillover_room_manager(
        2,
        LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 3,
            activation_window: 1,
            ..LocalSpilloverPolicyParts::conservative()
        })?,
    );
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    for raw_user_id in [10_i64, 20] {
        let (tx, _rx) = test_sender();
        join_user_with_sender(&room, UserId::Integer(raw_user_id), tx).await;
    }

    for raw_user_id in [10_i64, 20] {
        let user_id = UserId::Integer(raw_user_id);
        let Some(connection_id) = room.test_api().inspect().user_connection_id(&user_id).await
        else {
            panic!("user should be joined");
        };
        assert_eq!(
            room.transport_user_key(&user_id, connection_id)
                .media_worker_id(),
            0
        );
        assert_eq!(
            room.test_api()
                .inspect()
                .topology_home_router_id(&user_id)
                .await,
            Some(RouterId(0))
        );
    }
    Ok(())
}

#[tokio::test]
async fn load_triggered_room_uses_transport_pressure_for_new_placement()
-> Result<(), LocalSpilloverPolicyError> {
    let manager = load_spillover_room_manager(
        2,
        LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 99,
            max_active_consumers_per_router: 99,
            max_fanout_per_source: 99,
            egress_bitrate_threshold: Bitrate::from_bps(128),
            activation_window: 1,
            ..LocalSpilloverPolicyParts::conservative()
        })?,
    );
    let (media_transport, fake) = fake_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    manager_join_user(&manager, &room, 10, &media_transport).await;
    assert_user_placement(&room, &UserId::Integer(10), RouterId(0), 0).await;

    fake.set_worker_pressure_snapshot(
        0,
        TransportPlacementPressureSnapshot {
            egress_bitrate: Bitrate::from_bps(256),
            ..Default::default()
        },
    );

    manager_join_user(&manager, &room, 20, &media_transport).await;
    assert_user_placement(&room, &UserId::Integer(20), RouterId(1), 1).await;
    Ok(())
}

#[tokio::test]
async fn load_triggered_room_places_new_receivers_on_spillover_worker_after_pressure()
-> Result<(), LocalSpilloverPolicyError> {
    let manager = load_spillover_room_manager(
        2,
        LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 2,
            activation_window: 1,
            ..LocalSpilloverPolicyParts::conservative()
        })?,
    );
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    for raw_user_id in [10_i64, 20] {
        let (tx, _rx) = test_sender();
        join_user_with_sender(&room, UserId::Integer(raw_user_id), tx).await;
    }
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);
    let Some(first_connection) = room
        .test_api()
        .inspect()
        .user_connection_id(&first_user_id)
        .await
    else {
        panic!("first user should be joined");
    };
    let Some(second_connection) = room
        .test_api()
        .inspect()
        .user_connection_id(&second_user_id)
        .await
    else {
        panic!("second user should be joined");
    };

    assert_eq!(
        room.transport_user_key(&first_user_id, first_connection)
            .media_worker_id(),
        0
    );
    assert_eq!(
        room.transport_user_key(&second_user_id, second_connection)
            .media_worker_id(),
        1
    );
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(&second_user_id)
            .await,
        Some(RouterId(1))
    );
    Ok(())
}

#[tokio::test]
async fn cleanup_retry_keeps_resolved_spillover_worker_after_unregister()
-> Result<(), LocalSpilloverPolicyError> {
    let manager = load_spillover_room_manager(
        2,
        LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 99,
            max_active_consumers_per_router: 99,
            max_fanout_per_source: 99,
            egress_bitrate_threshold: Bitrate::from_bps(128),
            activation_window: 1,
            ..LocalSpilloverPolicyParts::conservative()
        })?,
    );
    let (media_transport, fake) = fake_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    let first_user_id = UserId::Integer(10);
    let first_connection = manager_join_user(&manager, &room, 10, &media_transport).await;
    assert_eq!(
        room.transport_user_key(&first_user_id, first_connection)
            .media_worker_id(),
        0
    );

    fake.set_worker_pressure_snapshot(
        0,
        TransportPlacementPressureSnapshot {
            egress_bitrate: Bitrate::from_bps(256),
            ..Default::default()
        },
    );

    let second_user_id = UserId::Integer(20);
    let second_connection = manager_join_user(&manager, &room, 20, &media_transport).await;
    let second_session_key = room.transport_user_key(&second_user_id, second_connection);
    assert_eq!(second_session_key.media_worker_id(), 1);

    fake.fail_close_session_until_allowed(second_session_key.clone());
    assert!(
        manager
            .close_session(
                room.uuid(),
                &second_user_id,
                second_connection,
                &media_transport,
            )
            .await
    );
    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 1);

    fake.allow_close_session(&second_session_key);
    room.test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&media_transport)
        .await;

    let events = fake.snapshot_events();
    assert!(events.iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::SessionClosed {
            user_id,
            media_worker_id: 1,
        } if *user_id == second_user_id
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::SessionClosed {
            user_id,
            media_worker_id: 0,
        } if *user_id == second_user_id
    )));
    Ok(())
}

#[tokio::test]
async fn room_replacement_join_rehomes_topology_and_transport_together() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let adapter = MediaTransport::fake_for_testing();
    let user_id = UserId::Integer(10);
    let (first_tx, _first_rx) = test_sender();
    let first_join = room
        .test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            user_id.clone(),
            None,
            UserPermissions::default(),
            first_tx,
            &adapter,
        )
        .await;
    assert!(first_join.is_ok());
    let Some(first_connection) = first_join.ok() else {
        return;
    };
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(&user_id)
            .await,
        Some(RouterId(0))
    );
    assert_eq!(
        room.transport_user_key(&user_id, first_connection)
            .media_worker_id(),
        0
    );

    let (replacement_tx, _replacement_rx) = test_sender();
    let replacement_join = room
        .test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            user_id.clone(),
            None,
            UserPermissions::default(),
            replacement_tx,
            &adapter,
        )
        .await;
    assert!(replacement_join.is_ok());
    let Some(replacement_connection) = replacement_join.ok() else {
        return;
    };

    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(&user_id)
            .await,
        Some(RouterId(1))
    );
    assert_eq!(
        room.transport_user_key(&user_id, replacement_connection)
            .media_worker_id(),
        1
    );
}

#[tokio::test]
async fn room_spillover_diagnostics_reports_each_users_transport_worker() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);
    for user_id in [first_user_id.clone(), second_user_id.clone()] {
        let (tx, _rx) = test_sender();
        join_user_with_sender(&room, user_id, tx).await;
    }
    let adapter = MediaTransport::fake_for_testing();

    let diagnostics = room.diagnostics_user_views(&adapter).await;
    let first_transport = diagnostics
        .iter()
        .find(|view| view.user_id == first_user_id)
        .map(|view| view.transport.clone());
    let second_transport = diagnostics
        .iter()
        .find(|view| view.user_id == second_user_id)
        .map(|view| view.transport.clone());

    assert_eq!(
        first_transport.map(|transport| transport.media_worker_id),
        Some(0)
    );
    assert_eq!(
        second_transport.map(|transport| transport.media_worker_id),
        Some(1)
    );
}

#[tokio::test]
async fn room_spillover_publish_subscribe_and_leave_cleanup_stay_aligned() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    let publisher_id = UserId::Integer(10);
    let subscriber_id = UserId::Integer(20);
    let (publisher_tx, _publisher_rx) = test_sender();
    let (subscriber_tx, _subscriber_rx) = test_sender();

    join_user_with_sender(&room, publisher_id.clone(), publisher_tx).await;
    let subscriber_connection =
        join_user_with_sender(&room, subscriber_id.clone(), subscriber_tx).await;
    make_session_ready(&room, &publisher_id).await;
    make_session_ready(&room, &subscriber_id).await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &publisher_id,
                TestSourceKind::ScalableVideo,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id,
                source_user_id,
                media_kind: MediaKind::Video,
            } if consumer_user_id == &subscriber_id && source_user_id == &publisher_id
        )
    })
    .await;

    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(&publisher_id)
            .await,
        Some(RouterId(0))
    );
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(&subscriber_id)
            .await,
        Some(RouterId(1))
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert_eq!(room.test_api().inspect().topology_router_count().await, 2);

    assert!(
        room.remove_user_with_cleanup(
            &subscriber_id,
            subscriber_connection,
            UserCleanup::state_only(Some(&adapter)),
        )
        .await
    );

    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_router_id(&subscriber_id)
            .await,
        None
    );
    assert_eq!(room.test_api().inspect().topology_router_count().await, 1);
}

#[tokio::test]
async fn room_owned_relay_route_shares_remote_worker_lifecycle() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (media_transport, fake) = fake_adapter();
    let users = join_ready_users(&room, &[10, 20, 30, 40]).await;
    let publisher_id = UserId::Integer(10);
    let first_remote_id = UserId::Integer(20);
    let second_remote_id = UserId::Integer(40);
    let first_remote_connection = joined_connection(&users, &first_remote_id);
    let second_remote_connection = joined_connection(&users, &second_remote_id);
    let target_worker_id = room
        .transport_user_key(&first_remote_id, first_remote_connection)
        .media_worker_id();
    assert_eq!(
        room.transport_user_key(&second_remote_id, second_remote_connection)
            .media_worker_id(),
        target_worker_id
    );

    publish_track(
        &room,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &media_transport,
    )
    .await;
    wait_video(&fake, &first_remote_id, &publisher_id).await;
    wait_video(&fake, &second_remote_id, &publisher_id).await;

    let source_media_id = camera_media_id(&room, &publisher_id).await;
    let route = (&publisher_id, source_media_id, target_worker_id);
    assert_relay_count(&fake, route, Install, 1);
    assert_relay_count(&fake, route, SetActivity(RelayRouteActivity::Active), 1);

    set_camera_active(
        &room,
        &first_remote_id,
        first_remote_connection,
        &publisher_id,
        false,
        &media_transport,
    )
    .await;
    assert_relay_count(&fake, route, SetActivity(RelayRouteActivity::Inactive), 0);

    set_camera_active(
        &room,
        &second_remote_id,
        second_remote_connection,
        &publisher_id,
        false,
        &media_transport,
    )
    .await;
    wait_relay(&fake, route, SetActivity(RelayRouteActivity::Inactive)).await;

    set_camera_active(
        &room,
        &first_remote_id,
        first_remote_connection,
        &publisher_id,
        true,
        &media_transport,
    )
    .await;
    wait_relay(&fake, route, SetActivity(RelayRouteActivity::Active)).await;

    leave(
        &room,
        &second_remote_id,
        second_remote_connection,
        &media_transport,
    )
    .await;
    assert_relay_count(&fake, route, Release, 0);

    fake.fail_next_relay_release(publisher_id.clone(), source_media_id, target_worker_id);
    leave(
        &room,
        &first_remote_id,
        first_remote_connection,
        &media_transport,
    )
    .await;
    wait_relay(&fake, route, Release).await;
    assert_relay_count(&fake, route, Release, 1);
}

#[tokio::test]
async fn room_owned_relay_route_is_released_when_bootstrap_turns_stale() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (media_transport, fake) = fake_adapter();
    let users = join_ready_users(&room, &[10, 20]).await;
    let publisher_id = UserId::Integer(10);
    let subscriber_id = UserId::Integer(20);
    let subscriber_connection = joined_connection(&users, &subscriber_id);
    let target_worker_id = room
        .transport_user_key(&subscriber_id, subscriber_connection)
        .media_worker_id();

    fake.set_consume_media_delay(Some(Duration::from_millis(200)));
    let publish_room = Arc::clone(&room);
    let publish_transport = media_transport.clone();
    let publish_user_id = publisher_id.clone();
    let publish_task = tokio::spawn(async move {
        publish_track(
            &publish_room,
            &publish_user_id,
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &publish_transport,
        )
        .await
    });
    wait_video(&fake, &subscriber_id, &publisher_id).await;

    let source_media_id = camera_media_id(&room, &publisher_id).await;
    let route = (&publisher_id, source_media_id, target_worker_id);
    assert_relay_count(&fake, route, Install, 1);

    fake.fail_next_relay_release(publisher_id.clone(), source_media_id, target_worker_id);
    leave(
        &room,
        &subscriber_id,
        subscriber_connection,
        &media_transport,
    )
    .await;
    wait_relay(&fake, route, Release).await;
    assert_relay_count(&fake, route, Release, 1);

    fake.set_consume_media_delay(None);
    let _ = publish_task.await.expect("publish task should finish");
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
}

#[tokio::test]
async fn room_manager_lookup_by_uuid() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let fetched = manager.get_by_uuid(room.uuid()).await;
    assert!(fetched.is_some());
    assert_eq!(
        fetched.map(|room| room.uuid().to_owned()),
        Some(room.uuid().to_owned())
    );
    assert!(manager.get_by_uuid("nonexistent").await.is_none());
}

#[tokio::test]
async fn room_manager_join_user_reports_missing_room() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = MediaTransport::fake_for_testing();
    let (tx, _rx) = test_sender();
    let result = manager
        .join_user(
            "missing-room",
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            &media_transport,
        )
        .await;
    assert!(matches!(result, Err(RoomManagerJoinError::MissingRoom)));
}

#[tokio::test]
async fn manager_leave_user_removes_empty_room() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = MediaTransport::fake_for_testing();
    let first_room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = first_room.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            &media_transport,
        )
        .await;
    assert!(joined.is_ok());
    let Some((_channel, connection_id)) = joined.ok() else {
        return;
    };

    manager
        .close_session(
            &room_id,
            &UserId::Integer(1),
            connection_id,
            &media_transport,
        )
        .await;

    assert!(manager.get_by_uuid(&room_id).await.is_none());
    let replacement = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), room_id);
}

#[tokio::test]
async fn manager_disconnect_users_removes_empty_room() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = MediaTransport::fake_for_testing();
    let first_room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = first_room.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            &media_transport,
        )
        .await;
    assert!(joined.is_ok());

    manager
        .disconnect_users(&room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    assert!(manager.get_by_uuid(&room_id).await.is_none());
    let replacement = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), room_id);
}

#[tokio::test]
async fn manager_retry_drain_removes_empty_room_after_cleanup_succeeds() {
    let manager = RoomManager::for_test();
    let (media_transport, fake) = fake_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    let user_id = UserId::Integer(1);
    let connection_id = manager_join_user(&manager, &room, 1, &media_transport).await;
    let session_key = room.transport_user_key(&user_id, connection_id);
    fake.fail_close_session_until_allowed(session_key.clone());

    assert!(
        manager
            .close_session(&room_id, &user_id, connection_id, &media_transport)
            .await
    );
    assert!(manager.get_by_uuid(&room_id).await.is_some());

    fake.allow_close_session(&session_key);
    for _ in 0..2 {
        manager.drain_cleanup_retries(&media_transport).await;
    }

    assert!(manager.get_by_uuid(&room_id).await.is_none());
    assert!(fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::SessionClosed { user_id: closed_user_id, .. }
            if *closed_user_id == user_id
    )));
}

#[tokio::test]
async fn manager_retry_drain_removes_empty_room_after_cleanup_exhaustion() {
    let manager = RoomManager::for_test();
    let (media_transport, fake) = fake_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    let user_id = UserId::Integer(1);
    let connection_id = manager_join_user(&manager, &room, 1, &media_transport).await;
    let session_key = room.transport_user_key(&user_id, connection_id);
    fake.fail_close_session_until_allowed(session_key);

    assert!(
        manager
            .close_session(&room_id, &user_id, connection_id, &media_transport)
            .await
    );
    assert!(manager.get_by_uuid(&room_id).await.is_some());

    for _ in 0..5 {
        manager.drain_cleanup_retries(&media_transport).await;
    }

    assert!(manager.get_by_uuid(&room_id).await.is_none());
}

#[tokio::test]
async fn manager_concurrent_empty_room_cleanup_decrements_metrics_once() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = Arc::new(RoomManager::new(
        super::super::RoomManagerConfig::new(
            1,
            super::super::RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(1),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        super::super::RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    ));
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    let (tx, _rx) = test_sender();
    let joined = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            &media_transport,
        )
        .await;
    assert!(joined.is_ok());
    assert_eq!(metrics.snapshot().active_rooms(), 1);
    assert_eq!(metrics.snapshot().active_users(), 1);

    let first_user_ids = [UserId::Integer(1)];
    let second_user_ids = [UserId::Integer(1)];
    let manager_ref = Arc::clone(&manager);
    let transport_ref = media_transport.clone();
    let first_cleanup = async {
        manager_ref
            .disconnect_users(&room_id, &first_user_ids, &transport_ref)
            .await;
    };
    let second_cleanup = async {
        manager
            .disconnect_users(&room_id, &second_user_ids, &media_transport)
            .await;
    };

    tokio::join!(first_cleanup, second_cleanup);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms(), 0);
    assert_eq!(snapshot.active_users(), 0);
    assert!(manager.get_by_uuid(&room_id).await.is_none());
}

#[tokio::test]
async fn manager_lifecycle_future_does_not_block_empty_cleanup() {
    let manager = Arc::new(RoomManager::for_test_with_admission_policy(
        RoomAdmissionPolicy::new(2),
    ));
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    let first_user = UserId::Integer(1);
    let second_user = UserId::Integer(2);
    let (first_tx, _first_rx) = test_sender();
    let joined = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: first_user.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: first_tx,
            },
            &media_transport,
        )
        .await;
    let (_room, first_connection_id) = joined.expect("initial user should join");

    let lifecycle_entered = Arc::new(Notify::new());
    let release_lifecycle = Arc::new(Notify::new());
    let manager_for_hold = Arc::clone(&manager);
    let room_id_for_hold = room_id.clone();
    let lifecycle_entered_for_hold = Arc::clone(&lifecycle_entered);
    let release_lifecycle_for_hold = Arc::clone(&release_lifecycle);
    let hold_lifecycle = tokio::spawn(async move {
        manager_for_hold
            .with_current_room(&room_id_for_hold, |room| async move {
                lifecycle_entered_for_hold.notify_one();
                release_lifecycle_for_hold.notified().await;
                room
            })
            .await
    });
    lifecycle_entered.notified().await;

    let manager_for_close = Arc::clone(&manager);
    let room_id_for_close = room_id.clone();
    let first_user_for_close = first_user.clone();
    let transport_for_close = media_transport.clone();
    let close_task = tokio::spawn(async move {
        manager_for_close
            .close_session(
                &room_id_for_close,
                &first_user_for_close,
                first_connection_id,
                &transport_for_close,
            )
            .await
    });
    yield_now().await;

    let did_close = timeout(Duration::from_secs(1), close_task)
        .await
        .expect("close should finish while another lifecycle future is parked")
        .expect("close task should not panic");
    assert!(did_close);
    assert!(manager.get_by_uuid(&room_id).await.is_some());

    let (second_tx, _second_rx) = test_sender();
    let join_result = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: second_user.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: second_tx,
            },
            &media_transport,
        )
        .await;
    assert!(matches!(
        join_result,
        Err(RoomManagerJoinError::MissingRoom)
    ));

    release_lifecycle.notify_one();

    let held_room = timeout(Duration::from_secs(1), hold_lifecycle)
        .await
        .expect("lifecycle holder should finish")
        .expect("lifecycle holder task should not panic");
    assert!(held_room.is_some());
    assert!(manager.get_by_uuid(&room_id).await.is_none());
    assert!(!room.test_api().inspect().has_session(&second_user).await);
}

#[tokio::test]
async fn manager_metrics_track_live_rooms_and_users_without_replacement_drift() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = RoomManager::new(
        super::super::RoomManagerConfig::new(
            1,
            super::super::RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(2),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        super::super::RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    assert_eq!(metrics.snapshot().active_rooms(), 1);

    let (first_tx, _first_rx) = test_sender();
    let first_join = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender: first_tx,
            },
            &media_transport,
        )
        .await;
    assert!(first_join.is_ok());
    assert_eq!(metrics.snapshot().active_users(), 1);

    let (replacement_tx, _replacement_rx) = test_sender();
    let replacement_join = manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: Some(String::from("replacement")),
                permissions: UserPermissions::default(),
                sender: replacement_tx,
            },
            &media_transport,
        )
        .await;
    assert!(replacement_join.is_ok());
    assert_eq!(metrics.snapshot().active_users(), 1);

    manager
        .disconnect_users(&room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms(), 0);
    assert_eq!(snapshot.active_users(), 0);
}

#[tokio::test]
async fn manager_metrics_track_live_media_totals_across_publish_and_disconnect() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = RoomManager::new(
        super::super::RoomManagerConfig::new(
            1,
            super::super::RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(2),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        super::super::RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();

    for raw_user_id in [1_i64, 2_i64] {
        let (sender, _receiver) = test_sender();
        let joined = manager
            .join_user(
                &room_id,
                JoinUserRequest {
                    user_id: UserId::Integer(raw_user_id),
                    label: None,
                    permissions: UserPermissions::default(),
                    sender,
                },
                &media_transport,
            )
            .await;
        assert!(joined.is_ok(), "user {raw_user_id} should join");
        make_session_ready(&room, &UserId::Integer(raw_user_id)).await;
    }

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                TestSourceKind::ScalableVideo,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &media_transport,
            )
            .await
            .is_some()
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms(), 1);
    assert_eq!(snapshot.active_users(), 2);
    assert_eq!(snapshot.active_publications(), 1);
    assert_eq!(snapshot.active_subscriptions(), 1);

    manager
        .disconnect_users(&room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms(), 1);
    assert_eq!(snapshot.active_users(), 1);
    assert_eq!(snapshot.active_publications(), 0);
    assert_eq!(snapshot.active_subscriptions(), 0);

    manager
        .disconnect_users(&room_id, &[UserId::Integer(2)], &media_transport)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms(), 0);
    assert_eq!(snapshot.active_users(), 0);
    assert_eq!(snapshot.active_publications(), 0);
    assert_eq!(snapshot.active_subscriptions(), 0);
}

#[tokio::test]
async fn manager_metrics_track_receiver_source_selection_updates() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = RoomManager::new(
        super::super::RoomManagerConfig::new(
            1,
            super::super::RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(3),
                RuntimeFeatureFlags::default(),
                super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        super::super::RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room(
            "issuer-source-selection",
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            None,
        )
        .await;

    for raw_user_id in [1_i64, 2, 3] {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        assert!(
            room.test_api()
                .lifecycle()
                .join_user(user_id.clone(), None, UserPermissions::default(), sender)
                .await
                .is_ok()
        );
        make_session_ready(&room, &user_id).await;
    }

    publish_simulcast_camera(&room, &UserId::Integer(1), &media_transport).await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.source_selection_updates_encoding(), 2);
}

#[tokio::test]
async fn manager_syncs_active_speaker_camera_policy_without_room_mutations() {
    let manager = RoomManager::for_test();
    let media_transport = MediaTransport::fake_for_testing();
    let fake = media_transport
        .as_fake_transport()
        .expect("test expects the fake media transport");
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;

    let mut receivers = Vec::new();
    for raw_user_id in [1_i64, 2_i64, 3_i64] {
        let (sender, receiver) = test_sender();
        receivers.push(receiver);
        room.test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(raw_user_id),
                None,
                UserPermissions::default(),
                sender,
            )
            .await
            .expect("user join should succeed");
        make_session_ready(&room, &UserId::Integer(raw_user_id)).await;
    }
    for raw_user_id in [1_i64, 2_i64] {
        publish_audio_and_camera(&room, &UserId::Integer(raw_user_id), &media_transport).await;
    }
    for receiver in &mut receivers {
        let _ = drain_outbound(receiver);
    }

    let (_first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;
    let (second_audio_media_id, _second_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(2)).await;

    let baseline_event_count = fake.snapshot_events().len();
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        second_audio_media_id,
        Instant::now(),
    )]);

    manager
        .sync_source_packet_selection_policies_for_runtime_ids(
            &BTreeSet::from([room.instance_id()]),
            &media_transport,
        )
        .await;

    let events = fake.snapshot_events();
    let policy_events = &events[baseline_event_count..];
    let featured_messages = drain_outbound(&mut receivers[0]);
    assert_consumer_packet_selection_update(
        policy_events,
        &UserId::Integer(1),
        &UserId::Integer(2),
        "hi",
    );
    assert_consumer_packet_selection_update(
        policy_events,
        &UserId::Integer(3),
        &UserId::Integer(2),
        "hi",
    );
    assert_featured_snapshot_update(&featured_messages, &UserId::Integer(1), false);
    assert_featured_snapshot_update(&featured_messages, &UserId::Integer(2), true);
}
