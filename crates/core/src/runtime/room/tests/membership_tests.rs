use super::fixtures::*;

#[tokio::test]
async fn join_user_enforces_capacity() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
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
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let first_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await
        .expect("first join should succeed");

    let (tx2, _rx2) = test_sender();
    let second_connection = room
        .test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx2)
        .await
        .expect("replacement join should succeed");

    assert_ne!(first_connection, second_connection);
    assert_eq!(room.test_api().inspect().router_user_count().await, 1);
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::Replaced))
    ));
    assert!(
        !room
            .remove_user_with_cleanup(
                &UserId::Integer(1),
                first_connection,
                UserCleanup::state_only(None),
            )
            .await
    );
    assert_eq!(
        room.test_api()
            .inspect()
            .user_connection_id(&UserId::Integer(1))
            .await,
        Some(second_connection)
    );
}

#[tokio::test]
async fn leave_user_sends_departure_to_remaining_peers() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    join_user_with_sender(&room, UserId::Integer(1), tx1).await;
    let bob_connection = join_user_with_sender(&room, UserId::Integer(2), tx2).await;

    room.remove_user_with_cleanup(
        &UserId::Integer(2),
        bob_connection,
        UserCleanup::state_only(None),
    )
    .await;

    let msg = rx1.try_recv();
    assert!(matches!(
        msg,
        Ok(UserOutbound::Message(RoomEventMessage::UserDeparted {
            user_id: UserId::Integer(2)
        }))
    ));
    assert_eq!(room.user_count().await, 1);
}

#[tokio::test]
async fn negotiated_session_rejects_stale_connection() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let media_transport = real_adapter();
    let (tx, _rx) = test_sender();
    let connection_id = join_user_with_sender(&room, UserId::Integer(1), tx).await;
    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(1),
            connection_id,
            UserCleanup::state_only(Some(&media_transport)),
        )
        .await
    );

    assert_eq!(
        room.apply_session_negotiated(
            &UserId::Integer(1),
            connection_id,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::StaleConnection
    );
}

#[tokio::test]
async fn removing_publisher_clears_media_state_and_transport_routes() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users().await;
    publish_simulcast_camera(&room, &UserId::Integer(1), &media_transport).await;
    drain_outbound(&mut publisher_rx);
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );
    let connection_id = user_connection_id(&room, &UserId::Integer(1)).await;
    let transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            connection_id,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose transport media");

    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(1),
            connection_id,
            UserCleanup::runtime(&media_transport),
        )
        .await
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert!(
        room.test_api()
            .inspect()
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
    assert!(
        media_transport
            .debug_route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}
