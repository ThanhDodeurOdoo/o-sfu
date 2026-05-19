use std::{sync::Arc, time::Duration};

use tokio::{sync::Notify, task::yield_now, time::timeout};

use super::fixtures::*;
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

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
