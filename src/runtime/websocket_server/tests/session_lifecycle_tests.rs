use super::fixtures::*;
use crate::runtime::channel::SessionCleanupPolicy;
use crate::runtime::rtc_adapter::TransportSessionHealth;

#[tokio::test]
async fn websocket_sends_ping_requests_and_accepts_responses() {
    let server = spawn_test_server_with_timeouts(
        1_000,
        200,
        20,
        100,
        RuntimeTransportAdapter::fake_for_testing(),
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-ping", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(410));
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

    let server_request = timeout(
        Duration::from_secs(1),
        wait_for_protocol_server_request(&mut websocket),
    )
    .await;
    assert!(
        server_request.is_ok(),
        "server should send ping request promptly: {server_request:?}"
    );
    let Some((request_id, ServerRequest::Ping)) = server_request.ok().flatten() else {
        panic!("expected PING server request");
    };
    assert!(
        respond_to_protocol_ping(&mut websocket, request_id)
            .await
            .is_some()
    );

    let no_close = timeout(Duration::from_millis(80), read_close_code(&mut websocket)).await;
    assert!(
        no_close.is_err(),
        "session should remain open after answering ping"
    );
}

#[tokio::test]
async fn websocket_closes_when_ping_response_times_out() {
    let server = spawn_test_server_with_timeouts(
        1_000,
        30,
        15,
        100,
        RuntimeTransportAdapter::fake_for_testing(),
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-ping-timeout",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let session_id = SessionId::Integer(411);
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
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

    let server_request = timeout(
        Duration::from_secs(1),
        wait_for_protocol_server_request(&mut websocket),
    )
    .await;
    assert!(
        server_request.is_ok(),
        "server should send ping request promptly: {server_request:?}"
    );
    let Some((_request_id, ServerRequest::Ping)) = server_request.ok().flatten() else {
        panic!("expected PING server request");
    };

    let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
    assert!(
        close_code.is_ok(),
        "server should close after ping timeout: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));

    sleep(Duration::from_millis(20)).await;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_session_loop_exits_ping_timeout, 1);
    assert!(
        !server
            .channels
            .has_session(channel.uuid(), &session_id)
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
    let channel = create_channel(
        &server,
        "issuer-rtc-disconnect",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let session_id = SessionId::Integer(412);
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
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
        panic!("protocol session should receive an initial offer");
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

    let connection_id = channel.session_connection_id(&session_id).await;
    assert!(connection_id.is_some());
    let Some(connection_id) = connection_id else {
        return;
    };
    server
        .state
        .transport_adapter
        .debug_set_session_transport_health(
            &channel.transport_session_key(&session_id, connection_id),
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
async fn websocket_closure_emits_fake_webrtc_session_closed_event() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let session_id = SessionId::Integer(213);
    let websocket = setup_negotiated_session(&server, &channel, session_id.clone()).await;
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
        Some(&FakeWebRtcEvent::SessionClosed { session_id })
    );
}

#[tokio::test]
async fn stale_replaced_socket_close_cleans_only_the_stale_transport_session() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let session_id = SessionId::Integer(260);
    let first_socket = setup_negotiated_session(&server, &channel, session_id.clone()).await;
    assert!(first_socket.is_some());
    let Some(mut first_socket) = first_socket else {
        return;
    };
    let second_socket = setup_negotiated_session(&server, &channel, session_id.clone()).await;
    assert!(second_socket.is_some());
    let Some(mut second_socket) = second_socket else {
        return;
    };

    assert_eq!(
        read_close_code(&mut first_socket).await,
        Some(CloseCode::Library(4003))
    );

    let events = wait_for_fake_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            session_id: session_id.clone(),
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
        Some(&FakeWebRtcEvent::SessionClosed { session_id })
    );
}

#[tokio::test]
async fn disconnect_cleanup_still_closes_transport_adapter_session_state() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_fake_adapter(Arc::<FakeWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 10, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

    let mut alice = setup_negotiated_session(&server, &channel, SessionId::Integer(1)).await;
    let mut bob = setup_negotiated_session(&server, &channel, SessionId::Integer(2)).await;
    assert!(alice.is_some());
    assert!(bob.is_some());
    let Some(ref mut alice) = alice else {
        return;
    };
    let Some(ref mut bob) = bob else {
        return;
    };

    server
        .channels
        .disconnect_sessions(
            channel.uuid(),
            &[SessionId::Integer(1)],
            &server.state.transport_adapter,
            SessionCleanupPolicy::StateAndTransportMedia,
        )
        .await;

    assert_eq!(read_close_code(alice).await, Some(CloseCode::Library(4003)));
    let peer_message = read_protocol_server_batch(bob).await.and_then(|batch| {
        protocol_server_messages(&batch).and_then(|mut messages| messages.drain(..).next())
    });
    assert!(
        matches!(peer_message, Some(ServerMessage::PeerLeft(_))),
        "remaining peer should receive session departure after disconnect: {peer_message:?}"
    );

    let events = wait_for_fake_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&FakeWebRtcEvent::SessionClosed {
            session_id: SessionId::Integer(1)
        })
    );
}
