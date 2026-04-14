use super::fixtures::*;

#[tokio::test]
async fn websocket_rejects_unknown_native_envelope_tag() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(12));
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
        "invalid native envelope should still send: {send_result:?}"
    );

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );
}

#[tokio::test]
async fn websocket_rejects_invalid_json_payload() {
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
}
