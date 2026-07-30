use std::{
    num::{NonZeroU64, NonZeroUsize},
    slice,
    sync::Arc,
    time::Duration,
};

use o_sfu_telemetry::schema::event as telemetry_event;
use serde_json::Value;
use tokio::{sync::Notify, task::yield_now, time::timeout};

use super::{
    super::manager::JoinPlacementTestGate,
    api::NegotiatedPublish,
    fixtures::*,
    tracing::{assert_exact, assert_user_exact, capture},
};
use crate::{RoomWorkerPolicy, RuntimeFeatureFlags, engine::metrics::RuntimeMetrics};

type TestRoom = super::super::Room;

async fn manager_join_user(
    manager: &RoomManager,
    room: &Arc<TestRoom>,
    raw_user_id: i64,
    media_transport: &MediaTransport,
) -> ConnectionId {
    let (tx, _rx) = test_sender();
    let admission = manager
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
    admission.connection_id
}

fn manager_with_room_worker_policy(room_worker_policy: RoomWorkerPolicy) -> RoomManager {
    RoomManager::for_test_with_runtime_policy(
        super::super::RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(100),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        )
        .with_room_worker_policy(room_worker_policy),
    )
}

async fn serve_test_room(manager: &RoomManager, issuer: &str) -> Arc<TestRoom> {
    manager
        .serve_room(issuer, TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
}

fn spillover_policy(max_local_routers: usize) -> RoomWorkerPolicy {
    RoomWorkerPolicy::new(
        NonZeroUsize::new(max_local_routers).expect("test router cap should be positive"),
        NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
            .expect("default delay threshold should be positive"),
    )
}

async fn assert_home_worker(room: &Arc<TestRoom>, raw_user_id: i64, media_worker: usize) {
    assert_eq!(
        room.test_api()
            .inspect()
            .home_media_worker_id(&UserId::Integer(raw_user_id))
            .await,
        Some(media_worker)
    );
}

async fn assert_router_count(room: &Arc<TestRoom>, expected: usize) {
    assert_eq!(room.test_api().inspect().router_count().await, expected);
}

#[tokio::test(flavor = "current_thread")]
async fn room_and_user_lifecycle_events_preserve_contract_fields() {
    let _guard = capture().await;
    let manager = RoomManager::for_test();
    let media_transport = real_adapter();
    let room = manager
        .serve_room(
            "issuer-lifecycle-events",
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            Some("203.0.113.1"),
        )
        .await;
    let closed_user = UserId::Integer(1);
    let disconnected_user = UserId::Integer(2);
    let closed_connection = manager_join_user(&manager, &room, 1, &media_transport).await;
    let disconnected_connection = manager_join_user(&manager, &room, 2, &media_transport).await;

    assert!(
        manager
            .close_session(
                room.uuid(),
                &closed_user,
                closed_connection,
                &media_transport,
            )
            .await
    );
    manager
        .disconnect_users(
            room.uuid(),
            slice::from_ref(&disconnected_user),
            &media_transport,
        )
        .await;

    assert_exact(
        telemetry_event::ROOM_CREATED,
        &[
            ("room_id", Value::from(room.uuid())),
            ("remote_address", Value::from("203.0.113.1")),
            ("web_rtc_enabled", Value::from(true)),
        ],
    );
    for (name, user, connection) in [
        (
            telemetry_event::USER_JOINED,
            &closed_user,
            closed_connection,
        ),
        (
            telemetry_event::USER_JOINED,
            &disconnected_user,
            disconnected_connection,
        ),
        (
            telemetry_event::USER_CLOSED,
            &closed_user,
            closed_connection,
        ),
        (
            telemetry_event::USER_DISCONNECTED,
            &disconnected_user,
            disconnected_connection,
        ),
    ] {
        assert_user_exact(
            name,
            room.uuid(),
            user.path_segment().as_ref(),
            connection.as_u64(),
            0,
            &[],
        );
    }
}

#[tokio::test]
async fn room_manager_concurrent_create_attempts_publish_one_live_room() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = Arc::new(RoomManager::new(
        super::super::RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(2),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        ),
        Arc::clone(&metrics),
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
async fn manager_concurrent_empty_room_cleanup_decrements_metrics_once() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = Arc::new(RoomManager::new(
        super::super::RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(1),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        ),
        Arc::clone(&metrics),
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
    let first_session = manager
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
    let first_connection_id = first_session.connection_id;

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
async fn manager_concurrent_overload_joins_revalidate_local_router_cap_at_commit() {
    const LOCAL_ROUTER_CAP: usize = 2;
    const WORKER_COUNT: usize = 4;
    const CONCURRENT_JOINS: usize = 12;

    let manager = Arc::new(manager_with_room_worker_policy(spillover_policy(
        LOCAL_ROUTER_CAP,
    )));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); WORKER_COUNT]);
    let room = serve_test_room(&manager, "issuer-concurrent-large-room-cap").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(20), Some(0), Some(0), Some(0)]);
    let placement_gate = Arc::new(JoinPlacementTestGate::new(CONCURRENT_JOINS));
    manager.set_join_placement_gate_for_test(Arc::clone(&placement_gate));

    let mut join_tasks = Vec::with_capacity(CONCURRENT_JOINS);
    for user_offset in 0..CONCURRENT_JOINS {
        let manager = Arc::clone(&manager);
        let room_id = room.uuid().to_owned();
        let media_transport = media_transport.clone();
        join_tasks.push(tokio::spawn(async move {
            let (sender, _receiver) = test_sender();
            manager
                .join_user(
                    &room_id,
                    JoinUserRequest {
                        user_id: UserId::Integer(i64::try_from(user_offset + 2).unwrap()),
                        label: None,
                        permissions: UserPermissions::default(),
                        sender,
                    },
                    &media_transport,
                )
                .await
                .expect("concurrent user should join through manager");
        }));
    }

    timeout(Duration::from_secs(5), placement_gate.hold_all_ready())
        .await
        .expect("all concurrent joins should reach placement commit");
    assert_router_count(&room, 1).await;
    placement_gate.release_all().await;

    for join_task in join_tasks {
        join_task.await.expect("join task should not panic");
        assert!(
            room.test_api().inspect().router_count().await <= LOCAL_ROUTER_CAP,
            "concurrent placement should not exceed the configured local router cap"
        );
    }

    assert_router_count(&room, LOCAL_ROUTER_CAP).await;
}

#[tokio::test(flavor = "current_thread")]
async fn placement_reads_worker_health_after_the_commit_gate() {
    const WORKER_COUNT: usize = 4;

    let manager = Arc::new(manager_with_room_worker_policy(spillover_policy(2)));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); WORKER_COUNT]);
    let room = serve_test_room(&manager, "issuer-fresh-placement-health").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    let primary_worker = room
        .test_api()
        .inspect()
        .home_media_worker_id(&UserId::Integer(1))
        .await
        .expect("first user should have a worker");
    let mut overloaded = vec![Some(0); WORKER_COUNT];
    *overloaded
        .get_mut(primary_worker)
        .expect("primary worker should exist") = Some(20);
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(overloaded);
    let gate = Arc::new(JoinPlacementTestGate::new(1));
    manager.set_join_placement_gate_for_test(Arc::clone(&gate));
    let pending_join = {
        let manager = Arc::clone(&manager);
        let room = Arc::clone(&room);
        let media_transport = media_transport.clone();
        tokio::spawn(async move {
            manager_join_user(&manager, &room, 2, &media_transport).await;
        })
    };

    timeout(Duration::from_secs(5), gate.hold_all_ready())
        .await
        .expect("join should reach the placement gate");
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); WORKER_COUNT]);
    gate.release_all().await;
    pending_join.await.expect("join task should not panic");

    assert_home_worker(&room, 2, primary_worker).await;
    assert_router_count(&room, 1).await;
}

#[tokio::test(flavor = "current_thread")]
async fn spillover_media_diagnostics_use_connection_worker() {
    let _guard = capture().await;
    let manager = manager_with_room_worker_policy(spillover_policy(2));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0), Some(0), None, None]);
    let room = serve_test_room(&manager, "issuer-spillover-diagnostics").await;
    manager_join_user(&manager, &room, 1, &media_transport).await;
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(20), Some(0), None, None]);
    let publisher_connection_id = manager_join_user(&manager, &room, 2, &media_transport).await;
    let publisher_id = UserId::Integer(2);
    let media_worker_id = 1;
    assert_home_worker(&room, 2, media_worker_id).await;
    let mut state = room.state.write().await;
    let session_negotiated = state
        .set_user_negotiated(
            &publisher_id,
            publisher_connection_id,
            test_client_rtp_capabilities(),
        )
        .is_some();
    drop(state);
    assert!(session_negotiated);

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
    assert_user_exact(
        telemetry_event::PUBLISH_COMMITTED,
        room.uuid(),
        publisher_id.path_segment().as_ref(),
        publisher_connection_id.as_u64(),
        media_worker_id,
        &[(
            "transport_media_id",
            Value::from(transport_media_id.as_u64()),
        )],
    );

    assert!(
        room.test_api()
            .media()
            .deactivate_publication(&publisher_id, &stream_id, &media_transport)
            .await
    );
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
    assert_user_exact(
        telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
        room.uuid(),
        publisher_id.path_segment().as_ref(),
        publisher_connection_id.as_u64(),
        media_worker_id,
        &[
            (
                "transport_media_id",
                Value::from(transport_media_id.as_u64()),
            ),
            ("active", Value::from(false)),
            ("stream_id", Value::from(stream_id.to_string())),
        ],
    );
}
