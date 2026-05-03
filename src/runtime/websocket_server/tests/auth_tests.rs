use super::fixtures::*;

#[tokio::test]
async fn websocket_times_out_when_client_never_authenticates() {
    let server = spawn_test_server(25, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };

    let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
    assert!(
        close_code.is_ok(),
        "timeout close should arrive promptly: {close_code:?}"
    );
    assert_eq!(close_code.ok().flatten(), Some(CloseCode::Library(4107)));

    sleep(Duration::from_millis(20)).await;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_connections_accepted, 1);
    assert_eq!(metrics.ws_handshake_rejected_timeout, 1);
}

#[tokio::test]
async fn websocket_authenticates_with_room_key_and_sends_welcome_payload() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-a",
        Some(TEST_ROOM_KEY),
        CreateRoomQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(7));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };
    let welcome = read_welcome(&mut websocket).await;
    assert!(welcome.is_some(), "welcome payload should exist");
    let Some(welcome) = welcome else {
        return;
    };
    assert_eq!(
        welcome,
        WelcomePayload {
            features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            },
            recording: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            peers: vec![],
        }
    );

    let close_result = websocket.close(None).await;
    assert!(close_result.is_ok());
    sleep(Duration::from_millis(20)).await;

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_connections_accepted, 1);
    assert_eq!(metrics.ws_handshake_credentials_received, 1);
    assert_eq!(metrics.ws_users_joined, 1);
    assert_eq!(metrics.ws_user_loops_started, 1);
}

#[tokio::test]
async fn websocket_authenticates_legacy_room_scoped_token_with_explicit_room_id() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-a",
        Some(TEST_ROOM_KEY),
        CreateRoomQuery::default(),
    )
    .await;
    let token =
        signed_legacy_channel_scoped_connect_claims(TEST_ROOM_KEY, UserId::Integer(17), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };

    let welcome = read_welcome(&mut websocket).await;
    assert!(welcome.is_some(), "welcome payload should exist");
}

#[tokio::test]
async fn websocket_rejects_explicit_room_id_that_disagrees_with_claims() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first_room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;
    let second_room = create_room(&server, "issuer-b", None, CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, first_room.uuid(), UserId::Integer(8));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(second_room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4106)),
    );
}

#[tokio::test]
async fn websocket_accepts_global_key_without_explicit_room_id() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(9));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_jwt(&server, &token).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };
    let welcome = read_welcome(&mut websocket).await;
    assert!(welcome.is_some(), "welcome payload should exist");
}

#[tokio::test]
async fn websocket_rejects_non_auth_handshake_frame_with_protocol_metric() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(
            serde_json::to_string(&vec![serde_json::json!({
                "t": "info",
                "p": {},
            })])
            .unwrap_or_default()
            .into(),
        ))
        .await;
    assert!(send_result.is_ok());

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    sleep(Duration::from_millis(20)).await;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_handshake_credentials_received, 0);
    assert_eq!(metrics.ws_handshake_rejected_protocol_error, 1);
    assert_eq!(metrics.ws_handshake_rejected_error, 0);
}
