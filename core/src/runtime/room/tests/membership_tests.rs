use super::fixtures::*;
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        room::{RoomManagerConfig, RoomManagerDeps, RoomRuntimePolicy, rtp_capabilities},
    },
};

#[tokio::test]
async fn join_user_enforces_capacity() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, _rx1) = test_sender();
    let result = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    assert!(result.is_ok());

    let (tx2, _rx2) = test_sender();
    let result = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;
    assert_eq!(result, Err(RoomJoinError::RoomFull));
}

#[tokio::test]
async fn reconnection_bypasses_capacity_and_replaces_existing_connection() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let first_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    assert!(first_connection.is_ok());
    assert_eq!(room.test_api().inspect().router_user_count().await, 1);

    let (tx2, _rx2) = test_sender();
    let second_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx2)
        .await;
    assert!(second_connection.is_ok());
    assert_eq!(room.test_api().inspect().router_user_count().await, 1);
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::Replaced))
    ));

    let Some(first_connection) = first_connection.ok() else {
        return;
    };
    let Some(second_connection) = second_connection.ok() else {
        return;
    };

    room.test_api()
        .lifecycle()
        .leave_user(&UserId::Integer(1), first_connection)
        .await;
    assert_eq!(room.user_count().await, 1);
    assert_eq!(room.test_api().inspect().router_user_count().await, 1);

    assert_eq!(
        room.test_api()
            .inspect()
            .user_connection_id(&UserId::Integer(1))
            .await,
        Some(second_connection),
        "stale leave must not remove the replacement connection"
    );

    room.test_api()
        .lifecycle()
        .leave_user(&UserId::Integer(1), second_connection)
        .await;
    assert_eq!(room.user_count().await, 0);
    assert_eq!(room.test_api().inspect().router_user_count().await, 0);
}

#[tokio::test]
async fn leave_user_sends_departure_to_remaining_peers() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let alice_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let bob_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;
    assert!(alice_connection.is_ok());
    assert!(bob_connection.is_ok());
    let Some(bob_connection) = bob_connection.ok() else {
        return;
    };

    room.test_api()
        .lifecycle()
        .leave_user(&UserId::Integer(2), bob_connection)
        .await;

    let msg = rx1.try_recv();
    assert!(msg.is_ok());
    if let Ok(UserOutbound::Message(RoomEventMessage::UserDeparted { user_id })) = msg {
        assert_eq!(user_id, UserId::Integer(2));
    } else {
        panic!("expected UserDeparted, got {msg:?}");
    }
    assert_eq!(room.user_count().await, 1);
}

#[tokio::test]
async fn join_user_notifies_existing_peers_with_user_joined() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let media_transport = MediaTransport::fake_for_testing();
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let first_join = room
        .add_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            tx1,
            &media_transport,
        )
        .await;
    let second_join = room
        .add_user(
            UserId::Integer(2),
            None,
            UserPermissions::default(),
            tx2,
            &media_transport,
        )
        .await;
    assert!(first_join.is_ok());
    assert!(second_join.is_ok());

    let msg = rx1.try_recv();
    assert!(msg.is_ok());
    if let Ok(UserOutbound::Message(RoomEventMessage::UserJoined { user_id, info })) = msg {
        assert_eq!(user_id, UserId::Integer(2));
        assert_eq!(info, UserInfo::snapshot_defaults());
    } else {
        panic!("expected UserJoined, got {msg:?}");
    }
}

#[tokio::test]
async fn replacing_a_user_notifies_remaining_peers() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, mut alice_rx) = test_sender();
    let (tx2, mut bob_old_rx) = test_sender();
    let (tx3, _bob_new_rx) = test_sender();
    let _alice_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let _bob_old_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;

    let _bob_new_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx3)
        .await;
    assert!(matches!(
        bob_old_rx.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::Replaced))
    ));
    let msg = alice_rx.try_recv();
    assert!(msg.is_ok());
    if let Ok(UserOutbound::Message(RoomEventMessage::UserDeparted { user_id })) = msg {
        assert_eq!(user_id, UserId::Integer(2));
    } else {
        panic!("expected UserDeparted, got {msg:?}");
    }
    assert_eq!(room.user_count().await, 2);
}

#[tokio::test]
async fn replacing_a_user_runtime_emits_departure_then_join_for_existing_peers() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let media_transport = MediaTransport::fake_for_testing();
    let (tx1, mut alice_rx) = test_sender();
    let (tx2, mut bob_old_rx) = test_sender();
    let (tx3, _bob_new_rx) = test_sender();
    assert!(
        room.add_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            tx1,
            &media_transport,
        )
        .await
        .is_ok()
    );
    assert!(
        room.add_user(
            UserId::Integer(2),
            None,
            UserPermissions::default(),
            tx2,
            &media_transport,
        )
        .await
        .is_ok()
    );
    assert!(matches!(
        alice_rx.try_recv().ok(),
        Some(UserOutbound::Message(RoomEventMessage::UserJoined { user_id, info }))
            if user_id == UserId::Integer(2) && info == UserInfo::snapshot_defaults()
    ));

    assert!(
        room.add_user(
            UserId::Integer(2),
            None,
            UserPermissions::default(),
            tx3,
            &media_transport,
        )
        .await
        .is_ok()
    );
    assert!(matches!(
        bob_old_rx.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::Replaced))
    ));
    assert!(matches!(
        alice_rx.try_recv().ok(),
        Some(UserOutbound::Message(RoomEventMessage::UserDeparted { user_id }))
            if user_id == UserId::Integer(2)
    ));
    assert!(matches!(
        alice_rx.try_recv().ok(),
        Some(UserOutbound::Message(RoomEventMessage::UserJoined { user_id, info }))
            if user_id == UserId::Integer(2) && info == UserInfo::snapshot_defaults()
    ));
    assert_eq!(room.user_count().await, 2);
}

async fn join_same_user_twice(room: &Arc<super::super::Room>) -> (ConnectionId, ConnectionId) {
    let (tx1, _rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let first_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await
        .unwrap_or(test_connection_id(u64::MAX));
    let second_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx2)
        .await
        .unwrap_or(test_connection_id(u64::MAX));
    (first_connection, second_connection)
}

async fn publish_camera(
    room: &Arc<super::super::Room>,
    media_transport: &MediaTransport,
) -> Option<UserStreamId> {
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            media_transport,
        )
        .await
}

#[tokio::test]
async fn leave_user_runtime_removes_surviving_consumer_media() {
    let (room, media_transport, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;

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
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id,
                source_user_id,
                ..
            } if *consumer_user_id == UserId::Integer(2)
                && *source_user_id == UserId::Integer(1)
        )
    })
    .await;

    let Some(connection_id) = room
        .test_api()
        .inspect()
        .user_connection_id(&UserId::Integer(1))
        .await
    else {
        panic!("publisher connection should exist");
    };
    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_runtime(&UserId::Integer(1), connection_id, &media_transport,)
            .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved { user_id, .. }
                if *user_id == UserId::Integer(2)
        )
    })
    .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved { user_id, .. }
                if *user_id == UserId::Integer(1)
        )
    })
    .await;
}

#[tokio::test]
async fn leave_user_runtime_removes_departing_consumer_media() {
    let (room, media_transport, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(2),
                TestSourceKind::ScalableVideo,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &media_transport,
            )
            .await
            .is_some()
    );
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(1),
                source_user_id: UserId::Integer(2),
                ..
            }
        )
    })
    .await;
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);

    let Some(connection_id) = room
        .test_api()
        .inspect()
        .user_connection_id(&UserId::Integer(1))
        .await
    else {
        panic!("consumer connection should exist");
    };
    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_runtime(&UserId::Integer(1), connection_id, &media_transport,)
            .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved {
                user_id: UserId::Integer(1),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn join_user_runtime_replacement_removes_surviving_consumer_media() {
    let (room, media_transport, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;

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
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id,
                source_user_id,
                ..
            } if *consumer_user_id == UserId::Integer(2)
                && *source_user_id == UserId::Integer(1)
        )
    })
    .await;

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        room.add_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            replacement_tx,
            &media_transport,
        )
        .await
        .is_ok()
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved { user_id, .. }
                if *user_id == UserId::Integer(2)
        )
    })
    .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved { user_id, .. }
                if *user_id == UserId::Integer(1)
        )
    })
    .await;
}

#[tokio::test]
async fn media_cleanup_failure_retries_until_success() {
    let (room, media_transport, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;

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
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&UserId::Integer(1))
        .await
        .expect("publisher should have a live connection");
    let transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            connection_id,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose a transport media id");
    fake.fail_next_remove_media(transport_media_id);

    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_runtime(&UserId::Integer(1), connection_id, &media_transport)
            .await
    );

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 0);
    assert!(fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            user_id: UserId::Integer(1),
            transport_media_id: removed_media_id,
        } if *removed_media_id == transport_media_id
    )));
}

#[tokio::test]
async fn user_close_failure_retries_until_success() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (media_transport, fake) = fake_adapter();
    let (tx, _rx) = test_sender();
    let user_id = UserId::Integer(1);
    let connection_id = room
        .add_user(
            user_id.clone(),
            None,
            UserPermissions::default(),
            tx,
            &media_transport,
        )
        .await
        .expect("user should join");
    let session_key = room.transport_user_key(&user_id, connection_id);
    fake.fail_next_close_session(session_key);

    assert!(
        room.remove_user(&user_id, connection_id, &media_transport)
            .await
    );

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 0);
    assert!(fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::SessionClosed {
            user_id: closed_user_id,
        } if *closed_user_id == user_id
    )));
}

#[tokio::test]
async fn cleanup_retry_exhaustion_drops_pending_retry() {
    let (room, media_transport, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;

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
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&UserId::Integer(1))
        .await
        .expect("publisher should have a live connection");
    let transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            connection_id,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose a transport media id");
    fake.fail_remove_media_until_allowed(transport_media_id);

    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_runtime(&UserId::Integer(1), connection_id, &media_transport)
            .await
    );
    room.test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&media_transport)
        .await;
    room.test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&media_transport)
        .await;

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 0);
    assert!(!fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            transport_media_id: removed_media_id,
            ..
        } if *removed_media_id == transport_media_id
    )));
}

#[tokio::test]
async fn manager_shutdown_abandons_pending_cleanup_retry_for_removed_room() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = RoomManager::new(
        RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(100),
                RuntimeFeatureFlags::default(),
                rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (media_transport, fake) = fake_adapter();
    let (tx, _rx) = test_sender();
    let user_id = UserId::Integer(1);
    let connection_id = room
        .add_user(
            user_id.clone(),
            None,
            UserPermissions::default(),
            tx,
            &media_transport,
        )
        .await
        .expect("user should join");
    let session_key = room.transport_user_key(&user_id, connection_id);
    fake.fail_close_session_until_allowed(session_key);

    assert!(
        manager
            .close_session(room.uuid(), &user_id, connection_id, &media_transport)
            .await
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_rooms, 0);
    assert_eq!(snapshot.transport_cleanup_failures_shutdown, 1);
}

#[tokio::test]
async fn state_only_cleanup_does_not_enqueue_transport_retry() {
    let (room, media_transport, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;

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
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&UserId::Integer(1))
        .await
        .expect("publisher should have a live connection");
    let transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            connection_id,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose a transport media id");
    fake.fail_next_remove_media(transport_media_id);

    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_without_transport_cleanup(
                &UserId::Integer(1),
                connection_id,
                &media_transport,
            )
            .await
    );

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 0);
    assert!(!fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            transport_media_id: removed_media_id,
            ..
        } if *removed_media_id == transport_media_id
    )));
}

#[tokio::test]
async fn stale_negotiation_callbacks_do_not_ready_a_replaced_user() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (media_transport, _fake) = fake_adapter();
    let (first_connection, second_connection) = join_same_user_twice(&room).await;

    assert_ne!(first_connection, second_connection);
    assert_eq!(
        room.test_api()
            .inspect()
            .user_connection_id(&UserId::Integer(1))
            .await,
        Some(second_connection)
    );

    assert!(
        !apply_publish_transport_ready(
            &room,
            &UserId::Integer(1),
            first_connection,
            &media_transport,
        )
        .await
    );
    assert!(
        !apply_client_rtp_capabilities(
            &room,
            &UserId::Integer(1),
            first_connection,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await
    );
    assert_eq!(
        room.apply_session_negotiated(
            &UserId::Integer(1),
            first_connection,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::StaleConnection
    );
    assert!(
        !room
            .test_api()
            .inspect()
            .session_has_parsed_client_rtp_capabilities(&UserId::Integer(1))
            .await
    );
    assert!(
        publish_camera(&room, &media_transport).await.is_none(),
        "stale negotiation callbacks must not make the replacement user publish-ready"
    );

    assert_eq!(
        room.apply_session_negotiated(
            &UserId::Integer(1),
            second_connection,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
    assert!(
        room.test_api()
            .inspect()
            .session_has_parsed_client_rtp_capabilities(&UserId::Integer(1))
            .await
    );
    assert!(
        publish_camera(&room, &media_transport).await.is_some(),
        "the current connection should become publish-ready after its own negotiation answer"
    );
}

#[tokio::test]
async fn stale_refresh_callbacks_do_not_target_a_replaced_user() {
    let mut scenario = setup_stale_refresh_scenario().await;

    assert_eq!(
        scenario
            .room
            .apply_session_negotiated(
                &UserId::Integer(2),
                scenario.second_subscriber_connection,
                test_client_rtp_capabilities(),
                &scenario.media_transport,
            )
            .await,
        SessionNegotiationOutcome::Applied
    );
    assert!(
        drain_outbound(&mut scenario.second_subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_))),
        "the current connection should receive the consumer bootstrap once it becomes ready"
    );
    wait_for_fake_event(&scenario.fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerKeyframeRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
            }
        )
    })
    .await;

    assert_eq!(
        scenario
            .room
            .apply_session_refreshed(
                &UserId::Integer(2),
                scenario.first_subscriber_connection,
                &scenario.media_transport,
            )
            .await,
        SessionNegotiationOutcome::StaleConnection,
        "stale refresh callbacks must not target the replacement connection"
    );
    assert!(
        drain_outbound(&mut scenario.second_subscriber_rx).is_empty(),
        "stale refresh callbacks must not emit duplicate bootstrap on the replacement connection"
    );

    assert_eq!(
        scenario
            .room
            .apply_session_refreshed(
                &UserId::Integer(2),
                scenario.second_subscriber_connection,
                &scenario.media_transport,
            )
            .await,
        SessionNegotiationOutcome::Applied,
        "the current connection should still accept refresh follow-up callbacks"
    );
    assert!(
        drain_outbound(&mut scenario.second_subscriber_rx).is_empty(),
        "refreshing the current connection must not duplicate already-committed consumers"
    );
}

struct StaleRefreshScenario {
    room: Arc<super::super::Room>,
    media_transport: MediaTransport,
    fake: Arc<FakeMediaTransport>,
    first_subscriber_connection: ConnectionId,
    second_subscriber_connection: ConnectionId,
    second_subscriber_rx: mpsc::UnboundedReceiver<UserOutbound>,
}

async fn setup_stale_refresh_scenario() -> StaleRefreshScenario {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (media_transport, fake) = fake_adapter();
    let (publisher_tx, mut publisher_rx) = test_sender();
    let (first_subscriber_tx, _first_subscriber_rx) = test_sender();
    let publisher_connection = room
        .test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            publisher_tx,
        )
        .await
        .unwrap();
    let first_subscriber_connection = room
        .test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(2),
            None,
            UserPermissions::default(),
            first_subscriber_tx,
        )
        .await
        .unwrap();

    assert_eq!(
        room.apply_session_negotiated(
            &UserId::Integer(1),
            publisher_connection,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
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
    assert!(drain_outbound(&mut publisher_rx).is_empty());

    let (second_subscriber_tx, second_subscriber_rx) = test_sender();
    let second_subscriber_connection = room
        .test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(2),
            None,
            UserPermissions::default(),
            second_subscriber_tx,
        )
        .await
        .unwrap();
    assert_ne!(first_subscriber_connection, second_subscriber_connection);

    StaleRefreshScenario {
        room,
        media_transport,
        fake,
        first_subscriber_connection,
        second_subscriber_connection,
        second_subscriber_rx,
    }
}

#[tokio::test]
async fn broadcast_reaches_all_except_sender() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let (tx3, mut rx3) = test_sender();
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(3), None, UserPermissions::default(), tx3)
        .await;

    room.test_api()
        .lifecycle()
        .broadcast(&UserId::Integer(2), serde_json::json!({"text": "hi"}))
        .await;

    assert!(rx1.try_recv().is_ok(), "user 1 should receive broadcast");
    assert!(
        rx2.try_recv().is_err(),
        "sender (user 2) should NOT receive own broadcast"
    );
    assert!(rx3.try_recv().is_ok(), "user 3 should receive broadcast");
}

#[tokio::test]
async fn update_user_info_broadcasts_to_all() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let media_transport = MediaTransport::fake_for_testing();
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;

    let info = UserInfo {
        is_talking: Some(true),
        ..UserInfo::default()
    };
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(1), info, false, &media_transport)
        .await;

    let msg1 = rx1.try_recv();
    let msg2 = rx2.try_recv();
    assert!(msg1.is_ok());
    assert!(msg2.is_ok());
    if let Ok(UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot))) = msg1 {
        assert!(snapshot.contains_key(&UserId::Integer(1)));
        assert_eq!(
            snapshot
                .get(&UserId::Integer(1))
                .and_then(|info| info.is_talking),
            Some(true)
        );
    } else {
        panic!("expected UserInfoChanged");
    }
}

#[tokio::test]
async fn update_user_info_with_refresh_sends_full_snapshot() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let media_transport = MediaTransport::fake_for_testing();
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;

    let info = UserInfo {
        is_self_muted: Some(true),
        ..UserInfo::default()
    };
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(1), info, true, &media_transport)
        .await;

    let msg = rx1.try_recv();
    assert!(msg.is_ok());
    if let Ok(UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot))) = msg {
        assert_eq!(snapshot.len(), 2, "full refresh should include all users");
        assert!(snapshot.contains_key(&UserId::Integer(1)));
        assert!(snapshot.contains_key(&UserId::Integer(2)));
        assert_eq!(
            snapshot
                .get(&UserId::Integer(1))
                .and_then(|info| info.is_self_muted),
            Some(true)
        );
    } else {
        panic!("expected UserInfoChanged with full snapshot");
    }
}

#[tokio::test]
async fn disconnect_users_kicks_targets_and_notifies_remaining() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let (tx3, mut rx3) = test_sender();
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;
    let _ = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(3), None, UserPermissions::default(), tx3)
        .await;

    room.test_api()
        .lifecycle()
        .disconnect_users(&[UserId::Integer(1), UserId::Integer(2)])
        .await;

    let msg1 = rx1.try_recv();
    assert!(msg1.is_ok());
    assert!(matches!(
        msg1.ok(),
        Some(UserOutbound::Close(UserCloseReason::RemovedByRuntime))
    ));
    let msg2 = rx2.try_recv();
    assert!(msg2.is_ok());
    assert!(matches!(
        msg2.ok(),
        Some(UserOutbound::Close(UserCloseReason::RemovedByRuntime))
    ));

    let departure1 = rx3.try_recv();
    let departure2 = rx3.try_recv();
    assert!(departure1.is_ok());
    assert!(departure2.is_ok());

    assert_eq!(room.user_count().await, 1);
}

#[tokio::test]
async fn disconnect_users_target_only_the_active_replaced_user() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let first_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    let second_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx2)
        .await;
    assert!(first_connection.is_ok());
    assert!(second_connection.is_ok());
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::Replaced))
    ));

    room.test_api()
        .lifecycle()
        .disconnect_users(&[UserId::Integer(1)])
        .await;

    assert!(matches!(
        rx2.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::RemovedByRuntime))
    ));
    assert!(rx1.try_recv().is_err());
    assert_eq!(room.user_count().await, 0);
    assert_eq!(room.test_api().inspect().router_user_count().await, 0);
}

#[tokio::test]
async fn room_maps_string_user_ids_into_router_users() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (tx, _rx) = test_sender();
    let joined = room
        .test_api()
        .lifecycle()
        .join_user(
            UserId::String("guest-1".to_owned()),
            None,
            UserPermissions::default(),
            tx,
        )
        .await;
    assert!(joined.is_ok());
    assert_eq!(room.user_count().await, 1);
    assert_eq!(room.test_api().inspect().router_user_count().await, 1);

    let Some(connection_id) = joined.ok() else {
        return;
    };

    room.test_api()
        .lifecycle()
        .leave_user(&UserId::String("guest-1".to_owned()), connection_id)
        .await;
    assert_eq!(room.user_count().await, 0);
    assert_eq!(room.test_api().inspect().router_user_count().await, 0);
}

#[tokio::test]
async fn room_keeps_user_permissions_above_router_state() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let permissions = UserPermissions {
        transcription: Some(true),
        audio_recording: Some(false),
        video_recording: Some(true),
    };
    let (first_tx, _first_rx) = test_sender();
    let joined = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, permissions.clone(), first_tx)
        .await;
    assert!(joined.is_ok());
    let stored_permissions = room
        .test_api()
        .inspect()
        .room_user_permissions(&UserId::Integer(1))
        .await;
    assert!(stored_permissions.is_some_and(|permissions| {
        permissions.transcription()
            && !permissions.audio_recording()
            && permissions.video_recording()
    }));

    let replacement_permissions = UserPermissions {
        transcription: Some(false),
        audio_recording: Some(true),
        video_recording: Some(false),
    };
    let (second_tx, _second_rx) = test_sender();
    let replaced = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, replacement_permissions, second_tx)
        .await;
    assert!(replaced.is_ok());
    let stored_permissions = room
        .test_api()
        .inspect()
        .room_user_permissions(&UserId::Integer(1))
        .await;
    assert!(stored_permissions.is_some_and(|permissions| {
        !permissions.transcription()
            && permissions.audio_recording()
            && !permissions.video_recording()
    }));
}
