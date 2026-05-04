use std::{collections::BTreeSet, time::Instant};

use super::fixtures::*;
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        diagnostics::DiagnosticsStore,
        media_transport::{SourcePacketGate, TransportMediaId},
        metrics::RuntimeMetrics,
        recording::MediaTap,
    },
};

async fn publish_audio_and_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    assert!(
        room.test_api()
            .media()
            .publish_track(
                user_id,
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                media_transport,
            )
            .await
            .is_some()
    );
    assert!(
        room.test_api()
            .media()
            .publish_track(
                user_id,
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                media_transport,
            )
            .await
            .is_some()
    );
}

async fn source_media_ids(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
) -> (TransportMediaId, TransportMediaId) {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist");
    };
    let Some(audio_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, StreamType::Audio)
        .await
    else {
        panic!("audio producer should expose a transport media id");
    };
    let Some(camera_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, StreamType::Camera)
        .await
    else {
        panic!("camera producer should expose a transport media id");
    };
    (audio_media_id, camera_media_id)
}

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
        .with_room_sharding_policy(crate::RoomShardingPolicy::bounded_local_spillover(
            local_router_count,
        )),
    ))
}

fn assert_consumer_packet_selection_update(
    events: &[FakeMediaTransportEvent],
    consumer_user_id: &UserId,
    source_user_id: &UserId,
    expected_rid: &str,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerPacketGateUpdated {
                consumer_user_id: updated_consumer_user_id,
                source_user_id: updated_source_user_id,
                packet_gate: SourcePacketGate::Rid(rid),
            } if updated_consumer_user_id == consumer_user_id
                && updated_source_user_id == source_user_id
                && rid == expected_rid
        )
    }));
}

fn assert_featured_snapshot_update(messages: &[UserOutbound], user_id: &UserId, is_featured: bool) {
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot))
                if snapshot.get(user_id).is_some_and(|info| info.is_featured == Some(is_featured))
        )
    }));
}

#[tokio::test]
async fn room_manager_is_idempotent_by_issuer() {
    let manager = RoomManager::for_test();
    let config = RoomConfig::default();
    let first = manager.serve_room("issuer-a", None, &config, None).await;
    let second = manager
        .serve_room("issuer-a", Some("ignored"), &config, None)
        .await;
    let third = manager.serve_room("issuer-b", None, &config, None).await;
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
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    ));
    let config = RoomConfig::default();

    let (first, second) = tokio::join!(
        manager.serve_room("issuer-a", None, &config, None),
        manager.serve_room("issuer-a", None, &config, None),
    );

    assert_eq!(first.uuid(), second.uuid());
    assert_eq!(metrics.snapshot().active_rooms, 1);
}

#[tokio::test]
async fn room_manager_assigns_media_workers_explicitly() {
    let manager = RoomManager::for_test_with_media_workers(2);
    let first = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let second = manager
        .serve_room("issuer-b", None, &RoomConfig::default(), None)
        .await;
    let third = manager
        .serve_room("issuer-c", None, &RoomConfig::default(), None)
        .await;

    assert_eq!(first.test_api().inspect().media_worker_id(), 0);
    assert_eq!(second.test_api().inspect().media_worker_id(), 1);
    assert_eq!(third.test_api().inspect().media_worker_id(), 0);
}

#[tokio::test]
async fn room_spillover_policy_places_user_transport_on_local_workers() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;

    let first_key = room.transport_user_key(&UserId::Integer(10), ConnectionId::from_raw(0));
    let second_key = room.transport_user_key(&UserId::Integer(20), ConnectionId::from_raw(1));

    assert_eq!(first_key.media_worker_id(), 0);
    assert_eq!(second_key.media_worker_id(), 1);
}

#[tokio::test]
async fn room_replacement_join_rehomes_topology_and_transport_together() {
    let manager = spillover_room_manager(2);
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);
    for user_id in [first_user_id.clone(), second_user_id.clone()] {
        let (tx, _rx) = test_sender();
        let joined = room
            .test_api()
            .lifecycle()
            .join_user(user_id, None, UserPermissions::default(), tx)
            .await;
        assert!(joined.is_ok());
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
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    let publisher_id = UserId::Integer(10);
    let subscriber_id = UserId::Integer(20);
    let (publisher_tx, _publisher_rx) = test_sender();
    let (subscriber_tx, _subscriber_rx) = test_sender();

    let publisher_join = room
        .test_api()
        .lifecycle()
        .join_user(
            publisher_id.clone(),
            None,
            UserPermissions::default(),
            publisher_tx,
        )
        .await;
    assert!(publisher_join.is_ok());
    let subscriber_join = room
        .test_api()
        .lifecycle()
        .join_user(
            subscriber_id.clone(),
            None,
            UserPermissions::default(),
            subscriber_tx,
        )
        .await;
    assert!(subscriber_join.is_ok());
    let Some(subscriber_connection) = subscriber_join.ok() else {
        return;
    };
    make_session_ready(&room, &publisher_id).await;
    make_session_ready(&room, &subscriber_id).await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &publisher_id,
                StreamType::Camera,
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
        room.test_api()
            .lifecycle()
            .leave_session_without_transport_cleanup(
                &subscriber_id,
                subscriber_connection,
                &adapter
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
async fn room_manager_lookup_by_uuid() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), room_id);
}

#[tokio::test]
async fn manager_disconnect_users_removes_empty_room() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = MediaTransport::fake_for_testing();
    let first_room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    assert_ne!(replacement.uuid(), room_id);
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
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    ));
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
    assert_eq!(metrics.snapshot().active_rooms, 1);
    assert_eq!(metrics.snapshot().active_users, 1);

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
    assert_eq!(snapshot.active_rooms, 0);
    assert_eq!(snapshot.active_users, 0);
    assert!(manager.get_by_uuid(&room_id).await.is_none());
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
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let room_id = room.uuid().to_owned();
    assert_eq!(metrics.snapshot().active_rooms, 1);

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
    assert_eq!(metrics.snapshot().active_users, 1);

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
    assert_eq!(metrics.snapshot().active_users, 1);

    manager
        .disconnect_users(&room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms, 0);
    assert_eq!(snapshot.active_users, 0);
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
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &media_transport,
            )
            .await
            .is_some()
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms, 1);
    assert_eq!(snapshot.active_users, 2);
    assert_eq!(snapshot.active_publications, 1);
    assert_eq!(snapshot.active_subscriptions, 1);

    manager
        .disconnect_users(&room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms, 1);
    assert_eq!(snapshot.active_users, 1);
    assert_eq!(snapshot.active_publications, 0);
    assert_eq!(snapshot.active_subscriptions, 0);

    manager
        .disconnect_users(&room_id, &[UserId::Integer(2)], &media_transport)
        .await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms, 0);
    assert_eq!(snapshot.active_users, 0);
    assert_eq!(snapshot.active_publications, 0);
    assert_eq!(snapshot.active_subscriptions, 0);
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
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let media_transport = MediaTransport::fake_for_testing();
    let room = manager
        .serve_room(
            "issuer-source-selection",
            None,
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

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &media_transport,
            )
            .await
            .is_some()
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.source_selection_updates_encoding, 2);
}

#[tokio::test]
async fn manager_syncs_active_speaker_camera_policy_without_room_mutations() {
    let manager = RoomManager::for_test();
    let media_transport = MediaTransport::fake_for_testing();
    let fake = media_transport
        .as_fake_transport()
        .expect("test expects the fake media transport");
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
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
