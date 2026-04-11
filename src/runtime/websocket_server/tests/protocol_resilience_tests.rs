use super::fixtures::*;

#[tokio::test]
async fn websocket_returns_empty_object_for_malformed_bus_requests() {
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
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    let acknowledged = acknowledge_transport_bootstrap(&mut websocket).await;
    assert!(
        acknowledged.is_some(),
        "transport bootstrap should round-trip"
    );

    let malformed_request = serde_json::to_string(&vec![CurrentBusEnvelope {
        message: serde_json::json!({
            "name": "NOT_A_REAL_REQUEST"
        }),
        need_response: Some(CurrentBusRequestId::new(CurrentBusOrigin::Client, 4, 2)),
        response_to: None,
    }]);
    assert!(malformed_request.is_ok());
    let Some(malformed_request) = malformed_request.ok() else {
        return;
    };
    let send_result = websocket
        .send(tungstenite::Message::Text(malformed_request.into()))
        .await;
    assert!(
        send_result.is_ok(),
        "malformed request should still send: {send_result:?}"
    );
    let response = read_bus_batch(&mut websocket).await;
    assert!(response.is_some());
    let Some(response) = response else {
        return;
    };
    let Some(envelope) = response.first() else {
        return;
    };
    assert_eq!(envelope.message, serde_json::json!({}));
    assert_eq!(
        envelope
            .response_to
            .as_ref()
            .map(CurrentBusRequestId::as_str),
        Some("c_4_2")
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
