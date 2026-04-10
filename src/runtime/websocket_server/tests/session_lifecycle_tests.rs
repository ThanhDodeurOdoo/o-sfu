use super::fixtures::*;

#[tokio::test]
async fn websocket_sends_ping_requests_and_accepts_responses() {
    let server =
        spawn_test_server_with_timeouts(1_000, 200, 20, 100, RuntimeTransportAdapter::stub()).await;
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
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    assert!(
        acknowledge_transport_bootstrap(&mut websocket)
            .await
            .is_some()
    );

    let server_request = timeout(Duration::from_secs(1), read_server_request(&mut websocket)).await;
    assert!(
        server_request.is_ok(),
        "server should send ping request promptly: {server_request:?}"
    );
    let Some((envelope, CurrentServerRequest::Ping)) = server_request.ok().flatten() else {
        panic!("expected PING server request");
    };
    let request_id = envelope.need_response.clone();
    assert!(request_id.is_some(), "PING should expect a response");
    let Some(request_id) = request_id else {
        return;
    };
    assert!(
        respond_to_server_request(&mut websocket, request_id, serde_json::json!({}))
            .await
            .is_some(),
        "client should be able to answer server ping"
    );

    let no_close = timeout(Duration::from_millis(80), read_close_code(&mut websocket)).await;
    assert!(
        no_close.is_err(),
        "session should remain open after answering ping"
    );
}

#[tokio::test]
async fn websocket_closes_when_ping_response_times_out() {
    let server =
        spawn_test_server_with_timeouts(1_000, 30, 15, 100, RuntimeTransportAdapter::stub()).await;
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
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    assert!(
        acknowledge_transport_bootstrap(&mut websocket)
            .await
            .is_some()
    );

    let server_request = timeout(Duration::from_secs(1), read_server_request(&mut websocket)).await;
    assert!(
        server_request.is_ok(),
        "server should send ping request promptly: {server_request:?}"
    );
    let Some((_envelope, CurrentServerRequest::Ping)) = server_request.ok().flatten() else {
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
async fn websocket_emits_stub_webrtc_bootstrap_event() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(210));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };

    let batch = read_bus_batch(&mut websocket).await;
    assert!(batch.is_some());

    let events = wait_for_stub_webrtc_events(&adapter, 1).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(events, vec![StubWebRtcEvent::BootstrapRequested]);
}

#[tokio::test]
async fn websocket_closure_emits_stub_webrtc_session_closed_event() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let session_id = SessionId::Integer(213);
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    let batch = read_bus_batch(&mut websocket).await;
    assert!(batch.is_some(), "transport bootstrap should be sent");
    let close_result = websocket.close(None).await;
    assert!(close_result.is_ok());

    let events = wait_for_stub_webrtc_events(&adapter, 2).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    let expected = vec![
        StubWebRtcEvent::BootstrapRequested,
        StubWebRtcEvent::SessionClosed { session_id },
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn stale_replaced_socket_close_cleans_only_the_stale_transport_session() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let session_id = SessionId::Integer(260);
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let first_authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(first_authenticated.is_some());
    let Some((mut first_socket, _first_startup)) = first_authenticated else {
        return;
    };
    assert!(
        read_bus_batch(&mut first_socket).await.is_some(),
        "first session should receive transport bootstrap"
    );

    let second_authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(second_authenticated.is_some());
    let Some((mut second_socket, _second_startup)) = second_authenticated else {
        return;
    };
    assert!(
        read_bus_batch(&mut second_socket).await.is_some(),
        "replacement session should receive transport bootstrap"
    );

    assert_eq!(
        read_close_code(&mut first_socket).await,
        Some(CloseCode::Library(4108))
    );

    let events = wait_for_stub_webrtc_events(&adapter, 3).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events,
        vec![
            StubWebRtcEvent::BootstrapRequested,
            StubWebRtcEvent::BootstrapRequested,
            StubWebRtcEvent::SessionClosed {
                session_id: session_id.clone(),
            }
        ]
    );

    let close_result = second_socket.close(None).await;
    assert!(close_result.is_ok());
    let events = wait_for_stub_webrtc_events(&adapter, 4).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&StubWebRtcEvent::SessionClosed { session_id })
    );
}

#[tokio::test]
async fn disconnect_cleanup_still_closes_transport_adapter_session_state() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 10, transport_adapter).await;
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

    server
        .channels
        .disconnect_sessions(channel.uuid(), &[SessionId::Integer(1)])
        .await;

    assert_eq!(read_close_code(alice).await, Some(CloseCode::Library(4108)));
    let peer_message = read_server_message(bob).await;
    assert!(
        matches!(peer_message, Some(CurrentServerMessage::SessionDeparted(_))),
        "remaining peer should receive session departure after disconnect: {peer_message:?}"
    );

    let events = wait_for_stub_webrtc_events(&adapter, 3).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    assert_eq!(
        events.last(),
        Some(&StubWebRtcEvent::SessionClosed {
            session_id: SessionId::Integer(1)
        })
    );
}
