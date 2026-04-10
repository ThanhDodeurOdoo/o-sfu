use super::fixtures::*;

#[tokio::test]
async fn join_session_enforces_capacity() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, _rx1) = test_sender();
    let result = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            1,
        )
        .await;
    assert!(result.is_ok());

    let (tx2, _rx2) = test_sender();
    let result = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            1,
        )
        .await;
    assert_eq!(result, Err(ChannelJoinError::ChannelFull));
}

#[tokio::test]
async fn reconnection_bypasses_capacity_and_replaces_existing_connection() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let first_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            1,
        )
        .await;
    assert!(first_connection.is_ok());
    assert_eq!(channel.router_session_count().await, 1);

    let (tx2, mut rx2) = test_sender();
    let second_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx2,
            1,
        )
        .await;
    assert!(second_connection.is_ok());
    assert_eq!(channel.router_session_count().await, 1);
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
    ));

    let Some(first_connection) = first_connection.ok() else {
        return;
    };
    let Some(second_connection) = second_connection.ok() else {
        return;
    };

    channel
        .leave_session(&SessionId::Integer(1), first_connection)
        .await;
    assert_eq!(channel.session_count().await, 1);
    assert_eq!(channel.router_session_count().await, 1);

    channel
        .broadcast(&SessionId::Integer(99), serde_json::json!("hello"))
        .await;
    let msg = rx2.try_recv();
    assert!(msg.is_ok(), "new sender should receive broadcast");

    channel
        .leave_session(&SessionId::Integer(1), second_connection)
        .await;
    assert_eq!(channel.session_count().await, 0);
    assert_eq!(channel.router_session_count().await, 0);
}

#[tokio::test]
async fn leave_session_sends_departure_to_remaining_peers() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let alice_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let bob_connection = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;
    assert!(alice_connection.is_ok());
    assert!(bob_connection.is_ok());
    let Some(bob_connection) = bob_connection.ok() else {
        return;
    };

    channel
        .leave_session(&SessionId::Integer(2), bob_connection)
        .await;

    let msg = rx1.try_recv();
    assert!(msg.is_ok());
    if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionDeparted(payload))) = msg {
        assert_eq!(payload.session_id, SessionId::Integer(2));
    } else {
        panic!("expected SessionDeparted, got {msg:?}");
    }
    assert_eq!(channel.session_count().await, 1);
}

#[tokio::test]
async fn replacing_a_session_notifies_remaining_peers() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut alice_rx) = test_sender();
    let (tx2, mut bob_old_rx) = test_sender();
    let (tx3, _bob_new_rx) = test_sender();
    let _alice_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let _bob_old_connection = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;

    let _bob_new_connection = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx3,
            10,
        )
        .await;
    assert!(matches!(
        bob_old_rx.try_recv().ok(),
        Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
    ));
    let msg = alice_rx.try_recv();
    assert!(msg.is_ok());
    if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionDeparted(payload))) = msg {
        assert_eq!(payload.session_id, SessionId::Integer(2));
    } else {
        panic!("expected SessionDeparted, got {msg:?}");
    }
    assert_eq!(channel.session_count().await, 2);
}

#[tokio::test]
async fn broadcast_reaches_all_except_sender() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let (tx3, mut rx3) = test_sender();
    let _ = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(3),
            None,
            SessionPermissions::default(),
            tx3,
            10,
        )
        .await;

    channel
        .broadcast(&SessionId::Integer(2), serde_json::json!({"text": "hi"}))
        .await;

    assert!(rx1.try_recv().is_ok(), "session 1 should receive broadcast");
    assert!(
        rx2.try_recv().is_err(),
        "sender (session 2) should NOT receive own broadcast"
    );
    assert!(rx3.try_recv().is_ok(), "session 3 should receive broadcast");
}

#[tokio::test]
async fn update_session_info_broadcasts_to_all() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let _ = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;

    let info = SessionInfo {
        is_talking: Some(true),
        ..SessionInfo::default()
    };
    channel
        .update_session_info(&SessionId::Integer(1), info, false)
        .await;

    let msg1 = rx1.try_recv();
    let msg2 = rx2.try_recv();
    assert!(msg1.is_ok());
    assert!(msg2.is_ok());
    if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot))) = msg1 {
        assert!(snapshot.contains_key("1"));
        assert_eq!(
            snapshot.get("1").and_then(|info| info.is_talking),
            Some(true)
        );
    } else {
        panic!("expected SessionInfoChanged");
    }
}

#[tokio::test]
async fn update_session_info_with_refresh_sends_full_snapshot() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let _ = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;

    let info = SessionInfo {
        is_camera_on: Some(true),
        ..SessionInfo::default()
    };
    channel
        .update_session_info(&SessionId::Integer(1), info, true)
        .await;

    let msg = rx1.try_recv();
    assert!(msg.is_ok());
    if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot))) = msg {
        assert_eq!(
            snapshot.len(),
            2,
            "full refresh should include all sessions"
        );
        assert!(snapshot.contains_key("1"));
        assert!(snapshot.contains_key("2"));
    } else {
        panic!("expected SessionInfoChanged with full snapshot");
    }
}

#[tokio::test]
async fn disconnect_sessions_kicks_targets_and_notifies_remaining() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let (tx3, mut rx3) = test_sender();
    let _ = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(3),
            None,
            SessionPermissions::default(),
            tx3,
            10,
        )
        .await;

    channel
        .disconnect_sessions(&[SessionId::Integer(1), SessionId::Integer(2)])
        .await;

    let msg1 = rx1.try_recv();
    assert!(msg1.is_ok());
    assert!(matches!(
        msg1.ok(),
        Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
    ));
    let msg2 = rx2.try_recv();
    assert!(msg2.is_ok());
    assert!(matches!(
        msg2.ok(),
        Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
    ));

    let departure1 = rx3.try_recv();
    let departure2 = rx3.try_recv();
    assert!(departure1.is_ok());
    assert!(departure2.is_ok());

    assert_eq!(channel.session_count().await, 1);
}

#[tokio::test]
async fn disconnect_sessions_target_only_the_active_replaced_session() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx1, mut rx1) = test_sender();
    let (tx2, mut rx2) = test_sender();
    let first_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
            10,
        )
        .await;
    let second_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx2,
            10,
        )
        .await;
    assert!(first_connection.is_ok());
    assert!(second_connection.is_ok());
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
    ));

    channel.disconnect_sessions(&[SessionId::Integer(1)]).await;

    assert!(matches!(
        rx2.try_recv().ok(),
        Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
    ));
    assert!(rx1.try_recv().is_err());
    assert_eq!(channel.session_count().await, 0);
    assert_eq!(channel.router_session_count().await, 0);
}

#[tokio::test]
async fn channel_maps_string_session_ids_into_router_sessions() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (tx, _rx) = test_sender();
    let joined = channel
        .join_session(
            SessionId::String("guest-1".to_owned()),
            None,
            SessionPermissions::default(),
            tx,
            10,
        )
        .await;
    assert!(joined.is_ok());
    assert_eq!(channel.session_count().await, 1);
    assert_eq!(channel.router_session_count().await, 1);

    let Some(connection_id) = joined.ok() else {
        return;
    };

    channel
        .leave_session(&SessionId::String("guest-1".to_owned()), connection_id)
        .await;
    assert_eq!(channel.session_count().await, 0);
    assert_eq!(channel.router_session_count().await, 0);
}

#[tokio::test]
async fn channel_keeps_router_session_permissions_in_sync() {
    let manager = ChannelManager::new();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let permissions = SessionPermissions {
        transcription: Some(true),
        audio_recording: Some(false),
        video_recording: Some(true),
    };
    let (first_tx, _first_rx) = test_sender();
    let joined = channel
        .join_session(
            SessionId::Integer(1),
            None,
            permissions.clone(),
            first_tx,
            10,
        )
        .await;
    assert!(joined.is_ok());
    assert_eq!(
        channel
            .router_session_permissions(&SessionId::Integer(1))
            .await,
        Some(RouterSessionPermissions::new(true, false, true))
    );

    let replacement_permissions = SessionPermissions {
        transcription: Some(false),
        audio_recording: Some(true),
        video_recording: Some(false),
    };
    let (second_tx, _second_rx) = test_sender();
    let replaced = channel
        .join_session(
            SessionId::Integer(1),
            None,
            replacement_permissions,
            second_tx,
            10,
        )
        .await;
    assert!(replaced.is_ok());
    assert_eq!(
        channel
            .router_session_permissions(&SessionId::Integer(1))
            .await,
        Some(RouterSessionPermissions::new(false, true, false))
    );
}
