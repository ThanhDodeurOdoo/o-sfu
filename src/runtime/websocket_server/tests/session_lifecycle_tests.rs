use std::slice;

use super::fixtures::*;
use crate::runtime::{ConnectionId, transport_adapter::TransportSessionHealth};

#[tokio::test]
async fn websocket_sends_ping_frames_and_accepts_pongs() {
    let server =
        spawn_test_server_with_timeouts(1_000, 200, 20, 100, MediaTransport::fake_for_testing())
            .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-ping", None, CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(410));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_welcome(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _welcome)) = authenticated else {
        return;
    };
    assert!(
        complete_initial_negotiation(&mut websocket, "v=0\r\ns=ping-answer\r\n")
            .await
            .is_some()
    );

    let ping_payload = timeout(Duration::from_secs(1), read_websocket_ping(&mut websocket)).await;
    assert!(
        ping_payload.is_ok(),
        "server should send a websocket ping promptly: {ping_payload:?}"
    );
    let Some(ping_payload) = ping_payload.ok().flatten() else {
        panic!("expected websocket ping frame");
    };
    assert!(
        send_websocket_pong(&mut websocket, ping_payload)
            .await
            .is_some()
    );

    let no_close = timeout(Duration::from_millis(80), read_close_code(&mut websocket)).await;
    assert!(
        no_close.is_err(),
        "user should remain open after answering ping"
    );
}

#[tokio::test]
async fn websocket_closes_when_pong_times_out() {
    let server =
        spawn_test_server_with_timeouts(1_000, 30, 15, 100, MediaTransport::fake_for_testing())
            .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-ping-timeout",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(411);
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), user_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_welcome(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _welcome)) = authenticated else {
        return;
    };
    assert!(
        complete_initial_negotiation(&mut websocket, "v=0\r\ns=ping-timeout-answer\r\n")
            .await
            .is_some()
    );

    sleep(Duration::from_millis(80)).await;

    let close_code = timeout(
        Duration::from_secs(1),
        read_close_code_without_answering_ping(&mut websocket),
    )
    .await;
    assert!(
        close_code.is_ok(),
        "server should close after websocket pong timeout: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));

    sleep(Duration::from_millis(20)).await;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_user_loop_exits_ping_timeout, 1);
    assert!(
        !server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &user_id.clone())
            .await
    );
}

#[tokio::test]
async fn websocket_closes_when_rtc_transport_disconnects() {
    let server =
        spawn_test_server_with_timeouts(1_000, 200, 20, 100, build_real_rtc_transport_adapter())
            .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-rtc-disconnect",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(412);
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), user_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_jwt(&server, &token).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };
    assert!(read_welcome(&mut websocket).await.is_some());
    let Some(offer_batch) = read_protocol_server_batch(&mut websocket).await else {
        panic!("protocol user should receive an initial offer");
    };
    let Some((request_id, request)) = first_protocol_server_request(&offer_batch) else {
        panic!("initial protocol frame should be an offer request");
    };
    assert!(
        respond_to_protocol_negotiation_request(
            &mut websocket,
            request_id,
            request,
            "v=0\r\ns=rtc-disconnect-answer\r\n",
        )
        .await
        .is_some()
    );

    let core_user_id = user_id.clone();
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&core_user_id)
        .await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };
    server.media_transport.debug_set_session_transport_health(
        &room.transport_user_key(&core_user_id, connection_id),
        TransportSessionHealth::Disconnected,
    );

    let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
    assert!(
        close_code.is_ok(),
        "server should close once RTC transport health becomes disconnected: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));
}

#[tokio::test]
async fn websocket_closes_when_rtc_transport_disconnects_during_initial_negotiation() {
    let server =
        spawn_test_server_with_timeouts(1_000, 200, 20, 100, build_real_rtc_transport_adapter())
            .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-rtc-disconnect-negotiating",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(413);
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), user_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_jwt(&server, &token).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };
    assert!(read_welcome(&mut websocket).await.is_some());
    assert!(
        read_protocol_server_batch(&mut websocket).await.is_some(),
        "user should receive the initial offer before the transport disconnect is injected"
    );

    let core_user_id = user_id.clone();
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&core_user_id)
        .await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };
    server.media_transport.debug_set_session_transport_health(
        &room.transport_user_key(&core_user_id, connection_id),
        TransportSessionHealth::Disconnected,
    );

    let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
    assert!(
        close_code.is_ok(),
        "server should close even when the RTC transport disconnects before the initial answer: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));
}

#[tokio::test]
async fn websocket_finish_rolls_back_staged_publish_before_room_cleanup() {
    let server =
        spawn_test_server_with_adapter(1_000, 100, MediaTransport::fake_for_testing()).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-staged-publish-finish",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(414);
    let websocket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let core_user_id = user_id.clone();
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&core_user_id)
        .await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };
    let publish = ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
        stream_type: StreamType::Camera,
    }))
    .into_envelope()
    .ok();
    assert!(publish.is_some());
    let Some(publish) = publish else {
        return;
    };
    let payload = serde_json::to_string(&vec![publish]).ok();
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert!(
        websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .is_ok()
    );
    assert!(
        wait_for_protocol_server_request(&mut websocket)
            .await
            .is_some(),
        "publish intent should stage media and request renegotiation"
    );
    assert!(
        room.has_staged_publish(&core_user_id, connection_id, StreamType::Camera)
            .await,
        "publish should be staged before the user finishes"
    );

    assert!(websocket.close(None).await.is_ok());
    let cleanup = timeout(Duration::from_secs(1), async {
        loop {
            if !room
                .has_staged_publish(&core_user_id, connection_id, StreamType::Camera)
                .await
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        cleanup.is_ok(),
        "user finish should explicitly roll back staged publishes"
    );
}

#[tokio::test]
async fn protocol_error_rolls_back_staged_publish_before_room_cleanup() {
    let server =
        spawn_test_server_with_adapter(1_000, 100, MediaTransport::fake_for_testing()).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-staged-publish-protocol-error",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(415);
    let websocket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let connection_id = stage_camera_publish(&mut websocket, &room, &user_id).await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };

    assert!(
        websocket
            .send(tungstenite::Message::Text("{".into()))
            .await
            .is_ok()
    );
    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol)
    );
    assert!(
        wait_for_staged_publish_cleanup(&room, &user_id, connection_id)
            .await
            .is_some(),
        "protocol-error close should roll back staged publishes"
    );
}

#[tokio::test]
async fn replacement_close_rolls_back_staged_publish_before_room_cleanup() {
    let server =
        spawn_test_server_with_adapter(1_000, 100, MediaTransport::fake_for_testing()).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-staged-publish-replacement",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(416);
    let first_socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(first_socket.is_some());
    let Some(mut first_socket) = first_socket else {
        return;
    };
    let connection_id = stage_camera_publish(&mut first_socket, &room, &user_id).await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };

    let replacement_socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(replacement_socket.is_some());
    let Some(mut replacement_socket) = replacement_socket else {
        return;
    };
    assert_eq!(
        read_close_code(&mut first_socket).await,
        Some(CloseCode::Library(4108))
    );
    assert!(
        wait_for_staged_publish_cleanup(&room, &user_id, connection_id)
            .await
            .is_some(),
        "replacement close should roll back staged publishes from the stale socket"
    );
    assert!(replacement_socket.close(None).await.is_ok());
}

#[tokio::test]
async fn runtime_disconnect_rolls_back_staged_publish_before_room_cleanup() {
    let server =
        spawn_test_server_with_adapter(1_000, 100, MediaTransport::fake_for_testing()).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-staged-publish-runtime-disconnect",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(417);
    let websocket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let connection_id = stage_camera_publish(&mut websocket, &room, &user_id).await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };

    server
        .room_manager
        .disconnect_users(
            room.uuid(),
            slice::from_ref(&user_id),
            &server.media_transport,
        )
        .await;

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4108))
    );
    assert!(
        wait_for_staged_publish_cleanup(&room, &user_id, connection_id)
            .await
            .is_some(),
        "runtime disconnect should roll back staged publishes"
    );
}

#[tokio::test]
async fn websocket_closure_emits_fake_webrtc_user_closed_event() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        MediaTransport::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(213);
    let websocket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let close_result = websocket.close(None).await;
    assert!(close_result.is_ok());

    let events = wait_for_fake_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            user_id: user_id.clone(),
        })
    );
}

#[tokio::test]
async fn stale_replaced_socket_close_cleans_only_the_stale_transport_user() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        MediaTransport::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(260);
    let first_socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(first_socket.is_some());
    let Some(mut first_socket) = first_socket else {
        return;
    };
    let second_socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(second_socket.is_some());
    let Some(mut second_socket) = second_socket else {
        return;
    };

    assert_eq!(
        read_close_code(&mut first_socket).await,
        Some(CloseCode::Library(4108))
    );

    let events = wait_for_fake_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            user_id: user_id.clone(),
        })
    );

    let close_result = second_socket.close(None).await;
    assert!(close_result.is_ok());
    let events = wait_for_fake_webrtc_events(&adapter, 2).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            user_id: user_id.clone(),
        })
    );
}

#[tokio::test]
async fn disconnect_cleanup_still_closes_transport_adapter_user_state() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        MediaTransport::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 10, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;

    let mut alice = setup_negotiated_session(&server, &room, UserId::Integer(1)).await;
    let mut bob = setup_negotiated_session(&server, &room, UserId::Integer(2)).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some(ref mut alice) = alice else {
        return;
    };
    let Some(ref mut bob) = bob else {
        return;
    };

    server
        .room_manager
        .disconnect_users(room.uuid(), &[UserId::Integer(1)], &server.media_transport)
        .await;

    assert_eq!(read_close_code(alice).await, Some(CloseCode::Library(4108)));
    let peer_message = read_protocol_server_batch(bob).await.and_then(|batch| {
        protocol_server_messages(&batch).and_then(|mut messages| messages.drain(..).next())
    });
    assert!(
        matches!(peer_message, Some(ServerMessage::PeerLeft(_))),
        "remaining peer should receive user departure after disconnect: {peer_message:?}"
    );

    let events = wait_for_fake_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            user_id: UserId::Integer(1)
        })
    );
}

#[tokio::test]
async fn disconnect_cleanup_closes_transport_user_before_empty_room_removal() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        MediaTransport::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 10, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(1);

    let socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(socket.is_some());
    let Some(mut socket) = socket else {
        return;
    };

    let core_user_id = user_id.clone();
    server
        .room_manager
        .disconnect_users(
            room.uuid(),
            slice::from_ref(&core_user_id),
            &server.media_transport,
        )
        .await;

    assert!(server.room_manager.get_by_uuid(room.uuid()).await.is_none());
    assert_eq!(
        read_close_code(&mut socket).await,
        Some(CloseCode::Library(4108))
    );

    let events = wait_for_fake_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            user_id: core_user_id
        })
    );
}

async fn stage_camera_publish(
    websocket: &mut TestWebSocket,
    room: &Room,
    user_id: &UserId,
) -> Option<ConnectionId> {
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(user_id)
        .await?;
    let publish = ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
        stream_type: StreamType::Camera,
    }))
    .into_envelope()
    .ok()?;
    let payload = serde_json::to_string(&vec![publish]).ok()?;
    assert!(
        websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .is_ok()
    );
    assert!(
        wait_for_protocol_server_request(websocket).await.is_some(),
        "publish intent should stage media and request renegotiation"
    );
    assert!(
        room.has_staged_publish(user_id, connection_id, StreamType::Camera)
            .await,
        "publish should be staged before the close path starts"
    );
    Some(connection_id)
}

async fn wait_for_staged_publish_cleanup(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
) -> Option<()> {
    timeout(Duration::from_secs(1), async {
        loop {
            if !room
                .has_staged_publish(user_id, connection_id, StreamType::Camera)
                .await
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}
