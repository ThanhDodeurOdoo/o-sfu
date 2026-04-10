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
async fn websocket_authenticates_with_channel_key_and_sends_startup_payload() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-a",
        Some(TEST_CHANNEL_KEY),
        CreateChannelQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_CHANNEL_KEY, channel.uuid(), SessionId::Integer(7));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let payload = serde_json::to_string(&CurrentWebSocketCredentials {
        channel_uuid: Some(channel.uuid().to_owned()),
        jwt: token,
    })
    .ok();
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "auth payload should send: {send_result:?}"
    );

    let message = read_message(&mut websocket).await;
    assert!(message.is_some(), "startup payload should exist");
    let Some(message) = message else {
        return;
    };
    assert!(
        message.is_ok(),
        "startup payload should be readable: {message:?}"
    );
    let Some(message) = message.ok() else {
        return;
    };
    let tungstenite::Message::Text(startup_json) = message else {
        return;
    };
    let startup = serde_json::from_str::<CurrentStartupPayload>(&startup_json);
    assert!(
        startup.is_ok(),
        "startup payload should deserialize: {startup:?}"
    );
    let Some(startup) = startup.ok() else {
        return;
    };
    assert_eq!(
        startup,
        CurrentStartupPayload {
            available_features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            },
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
        }
    );

    let close_result = websocket.close(None).await;
    assert!(close_result.is_ok());
    sleep(Duration::from_millis(20)).await;

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_connections_accepted, 1);
    assert_eq!(metrics.ws_handshake_credentials_received, 1);
    assert_eq!(metrics.ws_sessions_joined, 1);
    assert_eq!(metrics.ws_session_loops_started, 1);
}

#[tokio::test]
async fn websocket_rejects_explicit_channel_uuid_that_disagrees_with_claims() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first_channel =
        create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let second_channel =
        create_channel(&server, "issuer-b", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, first_channel.uuid(), SessionId::Integer(8));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let payload = serde_json::to_string(&CurrentWebSocketCredentials {
        channel_uuid: Some(second_channel.uuid().to_owned()),
        jwt: token,
    })
    .ok();
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "mismatched auth payload should still send: {send_result:?}"
    );

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4106)),
    );
}

#[tokio::test]
async fn websocket_accepts_global_key_without_explicit_channel_uuid() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(9));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };
    let payload = serde_json::to_string(&serde_json::json!({ "jwt": token })).ok();
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "jwt-only payload should send: {send_result:?}"
    );

    let message = read_message(&mut websocket).await;
    assert!(message.is_some(), "startup payload should exist");
    let Some(message) = message else {
        return;
    };
    assert!(
        message.is_ok(),
        "startup payload should be readable: {message:?}"
    );
    assert!(matches!(message.ok(), Some(tungstenite::Message::Text(_))));
}
