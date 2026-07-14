use super::fixtures::*;
use crate::runtime::media_transport::TransportSessionHealth;

#[tokio::test]
async fn websocket_sends_ping_frames_and_accepts_pongs() {
    let server = TestServerBuilder::new()
        .user_timeout_ms(200)
        .ping_interval_ms(20)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-ping", CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(410);
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id.clone());
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
    assert!(
        close_socket_and_wait_for_session_cleanup(&mut websocket, &room, &user_id)
            .await
            .is_some()
    );
}

#[tokio::test]
async fn websocket_closes_when_pong_times_out() {
    let server = TestServerBuilder::new()
        .user_timeout_ms(30)
        .ping_interval_ms(15)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-ping-timeout", CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(411);
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_silent_with_jwt(&server, &token).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };
    assert!(read_silent_welcome(&mut websocket).await.is_some());
    assert!(
        wait_for_silent_protocol_server_request(&mut websocket)
            .await
            .is_some()
    );

    sleep(Duration::from_millis(80)).await;

    let close_code = timeout(
        Duration::from_secs(1),
        read_silent_close_code(&mut websocket),
    )
    .await;
    assert!(
        close_code.is_ok(),
        "server should close after websocket pong timeout: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));

    sleep(Duration::from_millis(20)).await;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_user_loop_exits_ping_timeout(), 1);
    assert!(
        !server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &user_id)
            .await
    );
}

#[tokio::test]
async fn websocket_closes_when_rtc_transport_disconnects() {
    let server = TestServerBuilder::new()
        .user_timeout_ms(200)
        .ping_interval_ms(20)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-rtc-disconnect", CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(412);
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id.clone());
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
        respond_to_protocol_negotiation_request_with_test_rtc(
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
    let session_key = room.transport_user_key(&core_user_id, connection_id).await;
    server
        .media_transport
        .test_api()
        .set_session_transport_health(&session_key, TransportSessionHealth::Disconnected);

    let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
    assert!(
        close_code.is_ok(),
        "server should close once RTC transport health becomes disconnected: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));
    assert!(
        wait_for_session_cleanup(&room, &user_id).await.is_some(),
        "transport disconnect should complete explicit user cleanup"
    );
    assert!(
        wait_for_active_transport_users(&server, 0).await.is_some(),
        "transport disconnect should close the transport user"
    );
}

#[tokio::test]
async fn websocket_closes_when_rtc_transport_disconnects_during_initial_negotiation() {
    let server = TestServerBuilder::new()
        .user_timeout_ms(200)
        .ping_interval_ms(20)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-rtc-disconnect-negotiating",
        CreateRoomQuery::default(),
    )
    .await;
    let user_id = UserId::Integer(413);
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id.clone());
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
    let session_key = room.transport_user_key(&core_user_id, connection_id).await;
    server
        .media_transport
        .test_api()
        .set_session_transport_health(&session_key, TransportSessionHealth::Disconnected);

    let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
    assert!(
        close_code.is_ok(),
        "server should close even when the RTC transport disconnects before the initial answer: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));
    assert!(
        wait_for_session_cleanup(&room, &user_id).await.is_some(),
        "initial negotiation transport disconnect should complete explicit user cleanup"
    );
    assert!(
        wait_for_active_transport_users(&server, 0).await.is_some(),
        "initial negotiation transport disconnect should close the transport user"
    );
}

#[tokio::test]
async fn stale_replaced_socket_close_cleans_only_the_stale_transport_user() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let user_id = UserId::Integer(260);
    let first_socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(first_socket.is_some());
    let Some(mut first_socket) = first_socket else {
        return;
    };
    assert!(
        wait_for_active_transport_users(&server, 1).await.is_some(),
        "first negotiated socket should create one transport user"
    );
    let second_socket = setup_negotiated_session(&server, &room, user_id.clone()).await;
    assert!(second_socket.is_some());
    let Some(mut second_socket) = second_socket else {
        return;
    };

    assert_eq!(
        read_close_code(&mut first_socket).await,
        Some(CloseCode::Library(4108))
    );
    assert!(
        wait_for_active_transport_users(&server, 1).await.is_some(),
        "stale socket close should clean only the replaced transport user"
    );
    assert!(
        room.test_api().inspect().has_session(&user_id).await,
        "replacement session should stay live after stale socket cleanup"
    );

    let close_result = second_socket.close(None).await;
    assert!(close_result.is_ok());
    assert!(
        wait_for_session_cleanup(&room, &user_id).await.is_some(),
        "closing the replacement socket should remove the final session"
    );
    assert!(
        wait_for_active_transport_users(&server, 0).await.is_some(),
        "closing the replacement socket should clean the final transport user"
    );
}

async fn wait_for_active_transport_users(server: &TestServer, expected: i64) -> Option<()> {
    timeout(Duration::from_secs(1), async {
        loop {
            if server.state.metrics.snapshot().active_transport_users() == expected {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}
