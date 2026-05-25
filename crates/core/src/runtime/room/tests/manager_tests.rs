use std::{sync::Arc, time::Duration};

use o_sfu_telemetry::schema::event as telemetry_event;
use tokio::{sync::Notify, task::yield_now, time::timeout};

use super::{
    super::{
        effects::SubscriptionEffectPlan,
        placement::RoomPlacementDecisionReason,
        state::{ConsumerRouteTransportRef, ConsumerRouteUpdate},
        user_negotiation::UserTransportReady,
    },
    api::NegotiatedPublish,
    fixtures::*,
};
use crate::{
    LocalSpilloverPolicy, MediaCodecFlags, RoomWorkerPolicy, RuntimeFeatureFlags,
    prelude::LocalSpilloverPolicyParts,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry, source_model::UserStreamId,
    },
};

type TestRoom = super::super::Room;

async fn manager_join_user(
    manager: &RoomManager,
    room: &Arc<TestRoom>,
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

fn manager_with_room_worker_policy(room_worker_policy: RoomWorkerPolicy) -> RoomManager {
    manager_with_room_worker_policy_and_worker_count(room_worker_policy, 2)
}

fn manager_with_room_worker_policy_and_worker_count(
    room_worker_policy: RoomWorkerPolicy,
    media_worker_count: usize,
) -> RoomManager {
    RoomManager::for_test_with_config(super::super::RoomManagerConfig::new(
        media_worker_count,
        super::super::RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(100),
            RuntimeFeatureFlags::default(),
            super::super::rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
        )
        .with_room_worker_policy(room_worker_policy),
    ))
}

async fn serve_test_room(manager: &RoomManager, issuer: &str) -> Arc<TestRoom> {
    manager
        .serve_room(issuer, TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
}

fn load_triggered_policy(
    min_receiver_count: usize,
    activation_window: usize,
    cooldown_window: usize,
    max_fanout_per_source: usize,
) -> RoomWorkerPolicy {
    load_triggered_policy_with_cap(
        2,
        min_receiver_count,
        LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER,
        activation_window,
        cooldown_window,
        max_fanout_per_source,
    )
}

fn load_triggered_policy_with_cap(
    max_local_routers: usize,
    min_receiver_count: usize,
    max_active_consumers_per_router: usize,
    activation_window: usize,
    cooldown_window: usize,
    max_fanout_per_source: usize,
) -> RoomWorkerPolicy {
    let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
        min_receiver_count,
        max_active_consumers_per_router,
        max_fanout_per_source,
        activation_window,
        cooldown_window,
        ..LocalSpilloverPolicyParts::conservative()
    })
    .expect("test spillover policy should be valid");
    RoomWorkerPolicy::load_triggered_local_spillover(max_local_routers, policy)
}

async fn close_test_user(
    manager: &RoomManager,
    room: &Arc<TestRoom>,
    raw_user_id: i64,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) {
    assert!(
        manager
            .close_session(
                room.uuid(),
                &UserId::Integer(raw_user_id),
                connection_id,
                media_transport,
            )
            .await
    );
}

async fn assert_home_worker(room: &Arc<TestRoom>, raw_user_id: i64, media_worker: usize) {
    assert_eq!(
        room.test_api()
            .inspect()
            .topology_home_media_worker_id(&UserId::Integer(raw_user_id))
            .await,
        Some(media_worker)
    );
}

async fn assert_router_count(room: &Arc<TestRoom>, expected: usize) {
    assert_eq!(
        room.test_api().inspect().topology_router_count().await,
        expected
    );
}

fn assert_last_decision_reason(room: &Arc<TestRoom>, reason: RoomPlacementDecisionReason) {
    assert_eq!(
        room.test_api()
            .inspect()
            .load_triggered_last_decision_reason(),
        Some(reason)
    );
}

fn assert_event_worker(
    room: &TestRoom,
    user_id: &UserId,
    connection_id: ConnectionId,
    event_name: &str,
    transport_media_id: TransportMediaId,
    media_worker_id: usize,
) {
    let events = room.diagnostics.user_recent_events(room.uuid(), user_id);
    let event = events
        .iter()
        .find(|event| {
            event.event == event_name
                && event.connection_id == Some(connection_id.as_u64())
                && event.transport_media_id == Some(transport_media_id.as_u64())
        })
        .unwrap_or_else(|| panic!("expected recent diagnostics event {event_name}"));
    assert_eq!(event.media_worker_id, Some(media_worker_id));
}

async fn mark_publish_ready(room: &TestRoom, user_id: &UserId, connection_id: ConnectionId) {
    let mut state = room.state.write().await;
    assert!(
        state
            .set_transport_ready_for_test(user_id, connection_id, UserTransportReady::Publish)
            .session_present
    );
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                user_id,
                connection_id,
                &test_client_rtp_capabilities(),
            )
            .session_present
    );
    drop(state);
}

async fn seed_source_fanout_pressure(
    manager: &RoomManager,
    room: &Arc<TestRoom>,
    media_transport: &MediaTransport,
) -> UserStreamId {
    manager_join_user(manager, room, 1, media_transport).await;
    manager_join_user(manager, room, 2, media_transport).await;
    make_session_ready_with_transport(room, &UserId::Integer(1), media_transport).await;
    make_session_ready_with_transport(room, &UserId::Integer(2), media_transport).await;
    publish_track(
        room,
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        media_transport,
    )
    .await
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
async fn room_manager_join_user_reports_missing_room() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = real_adapter();
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
    let media_transport = real_adapter();
    let first_room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = first_room.uuid().to_owned();
    let connection_id = manager_join_user(&manager, &first_room, 1, &media_transport).await;

    manager
        .close_session(
            &room_id,
            &UserId::Integer(1),
            connection_id,
            &media_transport,
        )
        .await;

    assert!(manager.get_by_uuid(&room_id).await.is_none());
}

#[tokio::test]
async fn manager_disconnect_users_removes_empty_room() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = real_adapter();
    let first_room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = first_room.uuid().to_owned();
    manager_join_user(&manager, &first_room, 1, &media_transport).await;

    manager
        .disconnect_users(&room_id, &[UserId::Integer(1)], &media_transport)
        .await;

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
    let media_transport = real_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    manager_join_user(&manager, &room, 1, &media_transport).await;

    let first_user_ids = [UserId::Integer(1)];
    let second_user_ids = [UserId::Integer(1)];
    let first_manager = Arc::clone(&manager);
    let first_room_id = room_id.clone();
    let first_transport = media_transport.clone();
    let first_cleanup = async {
        first_manager
            .disconnect_users(&first_room_id, &first_user_ids, &first_transport)
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
}

#[tokio::test]
async fn manager_lifecycle_future_does_not_block_empty_cleanup() {
    let manager = Arc::new(RoomManager::for_test_with_admission_policy(
        RoomAdmissionPolicy::new(2),
    ));
    let media_transport = real_adapter();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    let first_user = UserId::Integer(1);
    let (first_tx, _first_rx) = test_sender();
    let (_room, first_connection_id) = manager
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
        .await
        .expect("initial user should join");

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

    release_lifecycle.notify_one();
    let held_room = timeout(Duration::from_secs(1), hold_lifecycle)
        .await
        .expect("lifecycle holder should finish")
        .expect("lifecycle holder task should not panic");
    assert!(held_room.is_some());
    assert!(manager.get_by_uuid(&room_id).await.is_none());
}

#[tokio::test]
async fn load_triggered_join_keeps_small_rooms_on_primary_worker() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(4, 1, 1, 48));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-small").await;

    manager_join_user(&manager, &room, 1, &media_transport).await;
    manager_join_user(&manager, &room, 2, &media_transport).await;

    assert_home_worker(&room, 1, 0).await;
    assert_home_worker(&room, 2, 0).await;
}

#[tokio::test]
async fn load_triggered_join_requires_sustained_receiver_pressure() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(2, 2, 1, 48));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-activation").await;

    manager_join_user(&manager, &room, 1, &media_transport).await;
    manager_join_user(&manager, &room, 2, &media_transport).await;
    assert_last_decision_reason(&room, RoomPlacementDecisionReason::ActivationWindowNotMet);
    manager_join_user(&manager, &room, 3, &media_transport).await;

    assert_home_worker(&room, 2, 0).await;
    assert_home_worker(&room, 3, 1).await;
    assert_last_decision_reason(&room, RoomPlacementDecisionReason::ReceiverCountPressure);
}

#[tokio::test]
async fn load_triggered_large_room_reaches_but_does_not_exceed_local_router_cap() {
    const LOCAL_ROUTER_CAP: usize = 4;
    const MIN_RECEIVER_COUNT: usize = 3;
    const MAX_ACTIVE_CONSUMERS_PER_ROUTER: usize = 2;
    const ACTIVATION_WINDOW: usize = 1;
    const COOLDOWN_WINDOW: usize = 1;
    const MAX_FANOUT_PER_SOURCE: usize = 2;

    let manager = manager_with_room_worker_policy_and_worker_count(
        load_triggered_policy_with_cap(
            LOCAL_ROUTER_CAP,
            MIN_RECEIVER_COUNT,
            MAX_ACTIVE_CONSUMERS_PER_ROUTER,
            ACTIVATION_WINDOW,
            COOLDOWN_WINDOW,
            MAX_FANOUT_PER_SOURCE,
        ),
        LOCAL_ROUTER_CAP,
    );
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-large-room-cap").await;

    for user_id in 1..=12 {
        manager_join_user(&manager, &room, user_id, &media_transport).await;
        assert!(
            room.test_api().inspect().topology_router_count().await <= LOCAL_ROUTER_CAP,
            "load-triggered placement should not exceed the configured local router cap"
        );
    }

    assert_router_count(&room, LOCAL_ROUTER_CAP).await;
    for (user_id, media_worker) in [
        (1, 0),
        (2, 0),
        (3, 1),
        (4, 1),
        (5, 2),
        (6, 2),
        (7, 3),
        (8, 3),
    ] {
        assert_home_worker(&room, user_id, media_worker).await;
    }
    assert_last_decision_reason(&room, RoomPlacementDecisionReason::LocalRouterCapReached);
}

#[tokio::test]
async fn spillover_media_diagnostics_use_connection_worker() {
    let manager = manager_with_room_worker_policy(RoomWorkerPolicy::bounded_local_spillover(2));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-spillover-diagnostics").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    let publisher_connection_id = manager_join_user(&manager, &room, 2, &media_transport).await;
    let publisher_id = UserId::Integer(2);
    let media_worker_id = 1;
    assert_home_worker(&room, 2, media_worker_id).await;
    mark_publish_ready(&room, &publisher_id, publisher_connection_id).await;

    let transport_media_id = TransportMediaId::new(99);
    let stream_id = room
        .test_api()
        .media()
        .publish_negotiated_track(
            &publisher_id,
            NegotiatedPublish {
                connection_id: publisher_connection_id,
                stream_type: TestSourceKind::AudioDetector,
                media_kind: MediaKind::Audio,
                transport_media_id,
                consumable_rtp_parameters: test_audio_rtp_parameters(),
            },
            &media_transport,
        )
        .await
        .expect("synthetic negotiated publish should commit");
    assert_event_worker(
        &room,
        &publisher_id,
        publisher_connection_id,
        telemetry_event::PUBLISH_COMMITTED,
        transport_media_id,
        media_worker_id,
    );

    assert_eq!(
        room.user_operation(&publisher_id, publisher_connection_id, &media_transport)
            .set_publication_activity(&stream_id, PublicationActivity::Inactive)
            .await,
        PublicationActivityOutcome::Applied {
            transport_update: crate::TransportEffectOutcome::Failed
        }
    );
    assert_event_worker(
        &room,
        &publisher_id,
        publisher_connection_id,
        telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
        transport_media_id,
        media_worker_id,
    );

    let consumer_media_id = TransportMediaId::new(199);
    let route = ConsumerRouteTransportRef::from_parts(
        publisher_id.clone(),
        publisher_connection_id,
        consumer_media_id,
        UserId::Integer(1),
        user_connection_id(&room, &UserId::Integer(1)).await,
        TransportMediaId::new(11),
    );
    let subscription_update =
        ConsumerRouteUpdate::new(route, stream_id.clone(), MediaKind::Audio, false);
    SubscriptionEffectPlan::from_route_updates(
        &room,
        &publisher_id,
        publisher_connection_id,
        vec![subscription_update],
    )
    .execute(&room, &media_transport)
    .await;
    assert_event_worker(
        &room,
        &publisher_id,
        publisher_connection_id,
        telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
        consumer_media_id,
        media_worker_id,
    );
}

#[tokio::test]
async fn bounded_spillover_still_detaches_idle_router_immediately() {
    let manager = manager_with_room_worker_policy(RoomWorkerPolicy::bounded_local_spillover(2));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-bounded-detach").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    let second_connection = manager_join_user(&manager, &room, 2, &media_transport).await;
    assert_router_count(&room, 2).await;

    close_test_user(&manager, &room, 2, second_connection, &media_transport).await;

    assert_router_count(&room, 1).await;
}

#[tokio::test]
async fn load_triggered_cooldown_delays_idle_spillover_detach() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(2, 1, 3, 48));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-cooldown").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    let second_connection = manager_join_user(&manager, &room, 2, &media_transport).await;
    assert_router_count(&room, 2).await;

    close_test_user(&manager, &room, 2, second_connection, &media_transport).await;
    assert_router_count(&room, 2).await;

    manager.drain_cleanup_retries(&media_transport).await;
    assert_router_count(&room, 2).await;
    manager.drain_cleanup_retries(&media_transport).await;
    assert_router_count(&room, 1).await;
}

#[tokio::test]
async fn load_triggered_activity_resets_spillover_cooldown() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(2, 1, 3, 48));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-cooldown-reset").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    let second_connection = manager_join_user(&manager, &room, 2, &media_transport).await;

    close_test_user(&manager, &room, 2, second_connection, &media_transport).await;
    assert_router_count(&room, 2).await;
    let third_connection = manager_join_user(&manager, &room, 3, &media_transport).await;
    assert_home_worker(&room, 3, 1).await;

    close_test_user(&manager, &room, 3, third_connection, &media_transport).await;
    manager.drain_cleanup_retries(&media_transport).await;

    assert_router_count(&room, 2).await;
}

#[tokio::test]
async fn source_fanout_pressure_places_next_join_on_spillover_worker() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(99, 1, 1, 1));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-fanout").await;
    seed_source_fanout_pressure(&manager, &room, &media_transport).await;

    manager_join_user(&manager, &room, 3, &media_transport).await;

    assert_home_worker(&room, 3, 1).await;
    assert_last_decision_reason(&room, RoomPlacementDecisionReason::SourceFanoutPressure);
}

#[tokio::test]
async fn source_fanout_pressure_clears_after_unpublish() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(99, 1, 1, 1));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-fanout-clear").await;
    let stream_id = seed_source_fanout_pressure(&manager, &room, &media_transport).await;
    let publisher_connection = user_connection_id(&room, &UserId::Integer(1)).await;

    assert_eq!(
        room.user_operation(&UserId::Integer(1), publisher_connection, &media_transport)
            .unpublish(&stream_id)
            .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
    manager_join_user(&manager, &room, 3, &media_transport).await;

    assert_home_worker(&room, 3, 0).await;
}

#[tokio::test]
async fn source_fanout_pressure_clears_after_receiver_leave() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(99, 1, 1, 1));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-fanout-leave").await;
    seed_source_fanout_pressure(&manager, &room, &media_transport).await;
    let receiver_connection = user_connection_id(&room, &UserId::Integer(2)).await;

    close_test_user(&manager, &room, 2, receiver_connection, &media_transport).await;
    manager_join_user(&manager, &room, 3, &media_transport).await;

    assert_home_worker(&room, 3, 0).await;
}

#[tokio::test]
async fn source_fanout_pressure_clears_after_receiver_replacement() {
    let manager = manager_with_room_worker_policy(load_triggered_policy(99, 2, 1, 1));
    let media_transport = real_adapter();
    let room = serve_test_room(&manager, "issuer-load-fanout-replace").await;
    seed_source_fanout_pressure(&manager, &room, &media_transport).await;

    manager_join_user(&manager, &room, 2, &media_transport).await;
    manager_join_user(&manager, &room, 3, &media_transport).await;

    assert_home_worker(&room, 3, 0).await;
}
