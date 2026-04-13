use super::fixtures::*;

#[tokio::test]
async fn join_session_enforces_capacity() {
    let manager = ChannelManager::for_test_with_admission_policy(ChannelAdmissionPolicy::new(1));
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
        )
        .await;
    assert_eq!(result, Err(ChannelJoinError::ChannelFull));
}

#[tokio::test]
async fn reconnection_bypasses_capacity_and_replaces_existing_connection() {
    let manager = ChannelManager::for_test_with_admission_policy(ChannelAdmissionPolicy::new(1));
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
        )
        .await;
    assert!(second_connection.is_ok());
    assert_eq!(channel.router_session_count().await, 1);
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(SessionOutbound::Close(WebSocketCloseCode::Kicked))
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
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let bob_connection = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
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
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let _bob_old_connection = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await;

    let _bob_new_connection = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx3,
        )
        .await;
    assert!(matches!(
        bob_old_rx.try_recv().ok(),
        Some(SessionOutbound::Close(WebSocketCloseCode::Kicked))
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

async fn join_same_session_twice(channel: &Arc<super::super::Channel>) -> (u64, u64) {
    let (tx1, _rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    let first_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx1,
        )
        .await
        .unwrap_or(u64::MAX);
    let second_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await
        .unwrap_or(u64::MAX);
    (first_connection, second_connection)
}

async fn publish_camera(
    channel: &Arc<super::super::Channel>,
    transport_adapter: &RuntimeTransportAdapter,
) -> Option<String> {
    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            transport_adapter,
        )
        .await
}

#[tokio::test]
async fn leave_session_runtime_removes_surviving_consumer_media() {
    let (channel, transport_adapter, stub, _publisher_rx, _subscriber_rx) =
        setup_two_ready_sessions_with_stub().await;

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &transport_adapter,
            )
            .await
            .is_some()
    );
    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id,
                source_session_id,
                ..
            } if *consumer_session_id == SessionId::Integer(2)
                && *source_session_id == SessionId::Integer(1)
        )
    })
    .await;

    let Some(connection_id) = channel.session_connection_id(&SessionId::Integer(1)).await else {
        panic!("publisher connection should exist");
    };
    assert!(
        channel
            .leave_session_runtime(&SessionId::Integer(1), connection_id, &transport_adapter)
            .await
    );

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::MediaRemoved { session_id, .. }
                if *session_id == SessionId::Integer(2)
        )
    })
    .await;
}

#[tokio::test]
async fn join_session_runtime_replacement_removes_surviving_consumer_media() {
    let (channel, transport_adapter, stub, _publisher_rx, _subscriber_rx) =
        setup_two_ready_sessions_with_stub().await;

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &transport_adapter,
            )
            .await
            .is_some()
    );
    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id,
                source_session_id,
                ..
            } if *consumer_session_id == SessionId::Integer(2)
                && *source_session_id == SessionId::Integer(1)
        )
    })
    .await;

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        channel
            .join_session_runtime(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                replacement_tx,
                &transport_adapter,
            )
            .await
            .is_ok()
    );

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::MediaRemoved { session_id, .. }
                if *session_id == SessionId::Integer(2)
        )
    })
    .await;
}

#[tokio::test]
async fn stale_negotiation_callbacks_do_not_ready_a_replaced_session() {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (transport_adapter, _stub) = stub_adapter();
    let (first_connection, second_connection) = join_same_session_twice(&channel).await;

    assert_ne!(first_connection, second_connection);
    assert_eq!(
        channel.session_connection_id(&SessionId::Integer(1)).await,
        Some(second_connection)
    );

    assert!(
        !channel
            .apply_transport_connected(
                &SessionId::Integer(1),
                first_connection,
                TransportConnectDirection::Upload,
                &transport_adapter,
            )
            .await
    );
    assert!(
        !channel
            .apply_client_rtp_capabilities(
                &SessionId::Integer(1),
                first_connection,
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );
    assert!(
        !channel
            .apply_session_negotiated(
                &SessionId::Integer(1),
                first_connection,
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );
    assert!(
        !channel
            .session_has_parsed_client_rtp_capabilities(&SessionId::Integer(1))
            .await
    );
    assert!(
        publish_camera(&channel, &transport_adapter).await.is_none(),
        "stale negotiation callbacks must not make the replacement session publish-ready"
    );

    assert!(
        channel
            .apply_session_negotiated(
                &SessionId::Integer(1),
                second_connection,
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );
    assert!(
        channel
            .session_has_parsed_client_rtp_capabilities(&SessionId::Integer(1))
            .await
    );
    assert!(
        publish_camera(&channel, &transport_adapter).await.is_some(),
        "the current connection should become publish-ready after its own negotiation answer"
    );
}

#[tokio::test]
async fn broadcast_reaches_all_except_sender() {
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(3),
            None,
            SessionPermissions::default(),
            tx3,
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
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
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
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await;

    let info = SessionInfo {
        is_self_muted: Some(true),
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
        assert_eq!(
            snapshot.get("1").and_then(|info| info.is_self_muted),
            Some(true)
        );
    } else {
        panic!("expected SessionInfoChanged with full snapshot");
    }
}

#[tokio::test]
async fn disconnect_sessions_kicks_targets_and_notifies_remaining() {
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await;
    let _ = channel
        .join_session(
            SessionId::Integer(3),
            None,
            SessionPermissions::default(),
            tx3,
        )
        .await;

    channel
        .disconnect_sessions(&[SessionId::Integer(1), SessionId::Integer(2)])
        .await;

    let msg1 = rx1.try_recv();
    assert!(msg1.is_ok());
    assert!(matches!(
        msg1.ok(),
        Some(SessionOutbound::Close(WebSocketCloseCode::Kicked))
    ));
    let msg2 = rx2.try_recv();
    assert!(msg2.is_ok());
    assert!(matches!(
        msg2.ok(),
        Some(SessionOutbound::Close(WebSocketCloseCode::Kicked))
    ));

    let departure1 = rx3.try_recv();
    let departure2 = rx3.try_recv();
    assert!(departure1.is_ok());
    assert!(departure2.is_ok());

    assert_eq!(channel.session_count().await, 1);
}

#[tokio::test]
async fn disconnect_sessions_target_only_the_active_replaced_session() {
    let manager = ChannelManager::for_test();
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
        )
        .await;
    let second_connection = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            tx2,
        )
        .await;
    assert!(first_connection.is_ok());
    assert!(second_connection.is_ok());
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(SessionOutbound::Close(WebSocketCloseCode::Kicked))
    ));

    channel.disconnect_sessions(&[SessionId::Integer(1)]).await;

    assert!(matches!(
        rx2.try_recv().ok(),
        Some(SessionOutbound::Close(WebSocketCloseCode::Kicked))
    ));
    assert!(rx1.try_recv().is_err());
    assert_eq!(channel.session_count().await, 0);
    assert_eq!(channel.router_session_count().await, 0);
}

#[tokio::test]
async fn channel_maps_string_session_ids_into_router_sessions() {
    let manager = ChannelManager::for_test();
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
    let manager = ChannelManager::for_test();
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
        .join_session(SessionId::Integer(1), None, permissions.clone(), first_tx)
        .await;
    assert!(joined.is_ok());
    assert_eq!(
        channel
            .router_session_permissions(&SessionId::Integer(1))
            .await,
        Some(RouterSessionPermissions::from_flags(
            o_sfu_router::SessionPermissionFlags {
                transcription: true,
                audio_recording: false,
                video_recording: true,
            },
        ))
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
        )
        .await;
    assert!(replaced.is_ok());
    assert_eq!(
        channel
            .router_session_permissions(&SessionId::Integer(1))
            .await,
        Some(RouterSessionPermissions::from_flags(
            o_sfu_router::SessionPermissionFlags {
                transcription: false,
                audio_recording: true,
                video_recording: false,
            },
        ))
    );
}
