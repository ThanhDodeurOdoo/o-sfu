use tungstenite::http::StatusCode;

use super::fixtures::*;
use crate::runtime::auth::MAX_JWT_TOKEN_BYTES;

#[tokio::test]
async fn websocket_rejects_pre_auth_connections_over_configured_capacity() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    let rejected = timeout(Duration::from_millis(200), connect_async(server.url())).await;
    assert!(
        rejected.is_ok(),
        "pre-auth cap rejection should complete promptly: {rejected:?}"
    );
    let Some(result) = rejected.ok() else {
        return;
    };
    match result {
        Err(tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        other => panic!("expected websocket HTTP rejection, got {other:?}"),
    }

    assert!(first.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_rejects_pre_auth_connections_over_origin_capacity() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(2, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    let rejected = timeout(Duration::from_millis(200), connect_async(server.url())).await;
    assert!(
        rejected.is_ok(),
        "per-origin pre-auth cap rejection should complete promptly: {rejected:?}"
    );
    let Some(result) = rejected.ok() else {
        return;
    };
    match result {
        Err(tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        other => panic!("expected websocket HTTP rejection, got {other:?}"),
    }

    assert!(first.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_pre_auth_origin_cap_allows_distinct_trusted_origins() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(2, 1)
        .trust_proxy_headers(true)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket_with_forwarded_for(&server, "198.51.100.24").await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };
    let second = connect_websocket_with_forwarded_for(&server, "203.0.113.8").await;
    assert!(
        second.is_some(),
        "distinct trusted origins should each receive one pre-auth slot"
    );
    let Some(mut second) = second else {
        return;
    };

    assert!(first.close(None).await.is_ok());
    assert!(second.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_times_out_when_client_never_authenticates() {
    let server = TestServerBuilder::new()
        .authentication_timeout_ms(25)
        .spawn()
        .await;
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
    assert_eq!(metrics.ws_connections_accepted(), 1);
    assert_eq!(metrics.ws_handshake_rejected_timeout(), 1);
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_auth_timeout() {
    let server = TestServerBuilder::new()
        .authentication_timeout_ms(25)
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    assert_eq!(
        read_close_code(&mut first).await,
        Some(CloseCode::Library(4107)),
    );
    sleep(Duration::from_millis(20)).await;

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after auth timeout"
    );
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_auth_failure() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    let send_result = first
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
    assert_eq!(read_close_code(&mut first).await, Some(CloseCode::Protocol));
    sleep(Duration::from_millis(20)).await;

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after auth failure"
    );
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_early_client_close() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    assert!(first.close(None).await.is_ok());
    sleep(Duration::from_millis(20)).await;

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after early client close"
    );
}

#[tokio::test]
async fn websocket_authenticates_with_room_key_and_sends_welcome_payload() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
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
    assert_eq!(metrics.ws_connections_accepted(), 1);
    assert_eq!(metrics.ws_handshake_credentials_received(), 1);
    assert_eq!(metrics.ws_users_joined(), 1);
    assert_eq!(metrics.ws_user_loops_started(), 1);
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_auth_success() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(77));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_welcome(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut first, _welcome)) = authenticated else {
        return;
    };

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after auth succeeds"
    );
    assert!(first.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_authenticates_legacy_room_scoped_token_with_explicit_room_id() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
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
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first_room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let second_room = create_room(&server, "issuer-b", CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_ROOM_KEY, first_room.uuid(), UserId::Integer(8));
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
async fn websocket_rejects_explicit_room_token_signed_with_another_key() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let token = signed_connect_claims("b3RoZXItcm9vbS1rZXk=", room.uuid(), UserId::Integer(19));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
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
async fn websocket_rejects_oversized_auth_token_with_auth_failure() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let token = "a".repeat(MAX_JWT_TOKEN_BYTES + 1);
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
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
async fn websocket_authenticates_room_key_token_without_explicit_room_id() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(9));
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
    let server = TestServerBuilder::new().spawn().await;
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
    assert_eq!(metrics.ws_handshake_credentials_received(), 0);
    assert_eq!(metrics.ws_handshake_rejected_protocol_error(), 1);
    assert_eq!(metrics.ws_handshake_rejected_error(), 0);
}
