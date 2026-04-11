use super::fixtures::*;

#[tokio::test]
async fn websocket_recreates_channel_after_last_disconnect_cleanup() {
    let server = spawn_test_server(1_000, 1).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

    let first_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(1));
    let second_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(2));
    assert!(first_token.is_some());
    assert!(second_token.is_some());
    let (Some(first_token), Some(second_token)) = (first_token, second_token) else {
        return;
    };

    let first_websocket = authenticate_with_jwt(&server, &first_token).await;
    assert!(first_websocket.is_some());
    let Some(mut first_websocket) = first_websocket else {
        return;
    };
    let startup = read_message(&mut first_websocket).await;
    assert!(startup.is_some(), "first startup payload should exist");
    let Some(startup) = startup else {
        return;
    };
    assert!(
        startup.is_ok(),
        "first startup payload should arrive: {startup:?}"
    );

    let second_websocket = authenticate_with_jwt(&server, &second_token).await;
    assert!(second_websocket.is_some());
    let Some(mut second_websocket) = second_websocket else {
        return;
    };
    assert_eq!(
        read_close_code(&mut second_websocket).await,
        Some(CloseCode::Library(4004)),
    );

    let close_result = first_websocket.close(None).await;
    assert!(
        close_result.is_ok(),
        "first websocket should close cleanly: {close_result:?}"
    );
    drop(first_websocket);
    sleep(Duration::from_millis(20)).await;

    assert!(server.channels.get_by_uuid(channel.uuid()).await.is_none());

    let replacement_channel =
        create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    assert_ne!(replacement_channel.uuid(), channel.uuid());
    let third_token = signed_connect_claims(
        TEST_AUTH_KEY,
        replacement_channel.uuid(),
        SessionId::Integer(3),
    );
    assert!(third_token.is_some());
    let Some(third_token) = third_token else {
        return;
    };

    let third_websocket = authenticate_with_jwt(&server, &third_token).await;
    assert!(third_websocket.is_some());
    let Some(mut third_websocket) = third_websocket else {
        return;
    };
    let startup = read_message(&mut third_websocket).await;
    assert!(startup.is_some(), "third startup payload should exist");
    let Some(startup) = startup else {
        return;
    };
    assert!(
        startup.is_ok(),
        "third startup payload should arrive after cleanup: {startup:?}"
    );
    assert!(matches!(startup.ok(), Some(tungstenite::Message::Text(_))));
}

#[tokio::test]
async fn broadcast_reaches_other_sessions_in_same_channel() {
    let server = spawn_test_server(1_000, 10).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

    let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
    let mut bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some(ref mut alice) = alice else {
        return;
    };
    let Some(ref mut bob) = bob else {
        return;
    };

    let sent = send_bus_message(
        alice,
        CurrentClientMessage::Broadcast(serde_json::json!({"text": "hello"})),
    )
    .await;
    assert!(sent.is_some());

    let msg = read_server_message(bob).await;
    assert!(msg.is_some(), "bob should receive broadcast");
    if let Some(CurrentServerMessage::Broadcast(payload)) = msg {
        assert_eq!(payload.sender_id, SessionId::Integer(1));
        assert_eq!(payload.message, serde_json::json!({"text": "hello"}));
    } else {
        panic!("expected Broadcast, got {msg:?}");
    }
}

#[tokio::test]
async fn session_leave_notifies_remaining_peers() {
    let server = spawn_test_server(1_000, 10).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

    let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
    let bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some(ref mut alice) = alice else {
        return;
    };
    let Some(mut bob) = bob else {
        return;
    };

    let close_result = bob.close(None).await;
    assert!(close_result.is_ok());
    drop(bob);
    sleep(Duration::from_millis(50)).await;

    let msg = read_server_message(alice).await;
    assert!(msg.is_some(), "alice should receive session departure");
    if let Some(CurrentServerMessage::SessionDeparted(payload)) = msg {
        assert_eq!(payload.session_id, SessionId::Integer(2));
    } else {
        panic!("expected SessionDeparted, got {msg:?}");
    }
}

#[tokio::test]
async fn info_change_broadcasts_to_all_sessions() {
    let server = spawn_test_server(1_000, 10).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

    let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
    let mut bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some(ref mut alice) = alice else {
        return;
    };
    let Some(ref mut bob) = bob else {
        return;
    };

    let sent = send_bus_message(
        alice,
        CurrentClientMessage::UpdateSessionInfo(CurrentSessionInfoUpdatePayload {
            info: SessionInfo {
                is_talking: Some(true),
                ..SessionInfo::default()
            },
            need_refresh: None,
        }),
    )
    .await;
    assert!(sent.is_some());

    let alice_msg = read_server_message(alice).await;
    let bob_msg = read_server_message(bob).await;
    assert!(alice_msg.is_some(), "alice should receive info change");
    assert!(bob_msg.is_some(), "bob should receive info change");
    if let Some(CurrentServerMessage::SessionInfoChanged(snapshot)) = bob_msg {
        assert!(snapshot.contains_key("1"));
        assert_eq!(
            snapshot.get("1").and_then(|info| info.is_talking),
            Some(true)
        );
    } else {
        panic!("expected SessionInfoChanged, got {bob_msg:?}");
    }
}
