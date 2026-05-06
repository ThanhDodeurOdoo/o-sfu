use super::fixtures::*;
use crate::runtime::websocket_server::io::{MAX_CLIENT_BATCH_ENVELOPES, MAX_CLIENT_FRAME_BYTES};

#[tokio::test]
async fn websocket_rejects_unknown_protocol_envelope_tag() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", None, CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(12));
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

    let invalid_request = serde_json::to_string(&vec![serde_json::json!({
        "t": "not-a-real-message",
        "p": {},
    })]);
    assert!(invalid_request.is_ok());
    let Some(invalid_request) = invalid_request.ok() else {
        return;
    };
    let send_result = websocket
        .send(tungstenite::Message::Text(invalid_request.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "invalid protocol envelope should still send: {send_result:?}"
    );

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_bus_parse_failures(), 1);
    assert_eq!(metrics.ws_bus_invalid_input_failures(), 0);
    assert_eq!(metrics.ws_bus_unsupported_feature_failures(), 1);
}

#[tokio::test]
async fn websocket_rejects_invalid_json_payload() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-invalid-json",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(14));
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

    let send_result = websocket
        .send(tungstenite::Message::Text("not-json".into()))
        .await;
    assert!(
        send_result.is_ok(),
        "invalid payload should still send: {send_result:?}"
    );

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_bus_parse_failures(), 1);
    assert_eq!(metrics.ws_bus_invalid_input_failures(), 1);
    assert_eq!(metrics.ws_bus_unsupported_feature_failures(), 0);
}

#[tokio::test]
async fn websocket_rejects_oversized_auth_frame() {
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
    let oversized_jwt = "x".repeat(MAX_CLIENT_FRAME_BYTES);
    let oversized_auth = serde_json::to_string(&vec![serde_json::json!({
        "t": "auth",
        "p": {
            "jwt": oversized_jwt,
        },
    })]);
    assert!(oversized_auth.is_ok());
    let Some(oversized_auth) = oversized_auth.ok() else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(oversized_auth.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "oversized auth payload should still send: {send_result:?}"
    );

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );
}

#[tokio::test]
async fn websocket_rejects_batches_over_protocol_envelope_limit() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-batch-limit",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(18));
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

    let oversized_batch = serde_json::to_string(
        &(0..=MAX_CLIENT_BATCH_ENVELOPES)
            .map(|_| serde_json::json!({ "t": "info", "p": {} }))
            .collect::<Vec<_>>(),
    );
    assert!(oversized_batch.is_ok());
    let Some(oversized_batch) = oversized_batch.ok() else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(oversized_batch.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "oversized envelope batch should still send: {send_result:?}"
    );

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_bus_parse_failures(), 1);
    assert_eq!(metrics.ws_bus_invalid_input_failures(), 1);
    assert_eq!(metrics.ws_bus_unsupported_feature_failures(), 0);
}

#[tokio::test]
async fn invalid_protocol_initial_answer_closes_before_user_negotiates() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-invalid-publish-answer",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(91));
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

    let publish_frame = serde_json::to_string(&vec![serde_json::json!({
        "t": "publish",
        "p": {
            "type": "camera",
        },
    })]);
    assert!(publish_frame.is_ok());
    let Some(publish_frame) = publish_frame.ok() else {
        return;
    };
    let send_result = websocket
        .send(tungstenite::Message::Text(publish_frame.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "protocol publish intent should still send: {send_result:?}"
    );

    let initial_offer = wait_for_protocol_server_request(&mut websocket).await;
    assert!(initial_offer.is_some());
    let Some((request_id, request)) = initial_offer else {
        return;
    };
    assert!(matches!(request, ServerRequest::Offer(_)));
    assert!(
        respond_to_protocol_negotiation_request(
            &mut websocket,
            request_id,
            request,
            "invalid-answer",
        )
        .await
        .is_some(),
        "invalid answer should still round-trip through the websocket"
    );

    assert_eq!(
        timeout(Duration::from_secs(1), read_close_code(&mut websocket))
            .await
            .ok()
            .flatten(),
        Some(CloseCode::Protocol),
    );
    assert!(
        !room
            .is_stream_published(
                &UserId::Integer(91),
                &stream_id_for_stream_type(StreamType::Camera),
            )
            .await,
        "invalid initial answer must not let queued publish state commit through a fallback-ready user"
    );
}
