use super::fixtures::*;

#[tokio::test]
async fn disconnect_rejects_invalid_utf8_body() {
    let request = build_request(
        Request::post(DISCONNECT_PATH),
        Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "disconnect request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn disconnect_requires_valid_jwt() {
    let request = build_request(Request::post(DISCONNECT_PATH), Body::from("invalid-token"));
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "disconnect request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn disconnect_rejects_oversized_body_before_auth_decode() {
    let oversized_body = "x".repeat((16 * 1024) + 1);
    let request = build_request(Request::post(DISCONNECT_PATH), Body::from(oversized_body));
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "oversized disconnect request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn disconnect_accepts_valid_jwt() {
    let token = signed_disconnect_claims(BTreeMap::new());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let request = build_request(Request::post(DISCONNECT_PATH), Body::from(token));
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "disconnect request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn channel_route_updates_metrics_for_unauthorized_and_success_paths() {
    let state = test_state();
    let unauthorized = build_request(
        Request::get(CHANNEL_PATH).header(header::HOST, "sfu.example.com"),
        Body::empty(),
    );
    assert!(unauthorized.is_some());
    let Some(unauthorized) = unauthorized else {
        return;
    };
    let unauthorized_response = app(state.clone()).oneshot(unauthorized).await;
    assert!(unauthorized_response.is_ok());
    let Some(unauthorized_response) = unauthorized_response.ok() else {
        return;
    };
    assert_eq!(unauthorized_response.status(), StatusCode::UNAUTHORIZED);

    let token = signed_channel_claims(Some("issuer-a"), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authorized = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
    );
    assert!(authorized.is_some());
    let Some(authorized) = authorized else {
        return;
    };
    let authorized_response = app(state.clone()).oneshot(authorized).await;
    assert!(authorized_response.is_ok());
    let Some(authorized_response) = authorized_response.ok() else {
        return;
    };
    assert_eq!(authorized_response.status(), StatusCode::OK);

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_channel_requests, 2);
    assert_eq!(metrics.http_channel_unauthorized, 1);
    assert_eq!(metrics.http_channel_success, 1);
}

#[tokio::test]
async fn disconnect_route_updates_metrics_for_all_outcomes() {
    let state = test_state();

    let invalid_utf8 = build_request(
        Request::post(DISCONNECT_PATH),
        Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
    );
    assert!(invalid_utf8.is_some());
    let Some(invalid_utf8) = invalid_utf8 else {
        return;
    };
    let invalid_utf8_response = app(state.clone()).oneshot(invalid_utf8).await;
    assert!(invalid_utf8_response.is_ok());
    let Some(invalid_utf8_response) = invalid_utf8_response.ok() else {
        return;
    };
    assert_eq!(invalid_utf8_response.status(), StatusCode::BAD_REQUEST);

    let invalid_claims = build_request(Request::post(DISCONNECT_PATH), Body::from("invalid-token"));
    assert!(invalid_claims.is_some());
    let Some(invalid_claims) = invalid_claims else {
        return;
    };
    let invalid_claims_response = app(state.clone()).oneshot(invalid_claims).await;
    assert!(invalid_claims_response.is_ok());
    let Some(invalid_claims_response) = invalid_claims_response.ok() else {
        return;
    };
    assert_eq!(
        invalid_claims_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let token = signed_disconnect_claims(BTreeMap::new());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let success = build_request(Request::post(DISCONNECT_PATH), Body::from(token));
    assert!(success.is_some());
    let Some(success) = success else {
        return;
    };
    let success_response = app(state.clone()).oneshot(success).await;
    assert!(success_response.is_ok());
    let Some(success_response) = success_response.ok() else {
        return;
    };
    assert_eq!(success_response.status(), StatusCode::OK);

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_disconnect_requests, 3);
    assert_eq!(metrics.http_disconnect_bad_request, 1);
    assert_eq!(metrics.http_disconnect_unprocessable_entity, 1);
    assert_eq!(metrics.http_disconnect_success, 1);
}
