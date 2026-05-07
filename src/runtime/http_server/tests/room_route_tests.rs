use super::fixtures::*;

#[tokio::test]
async fn room_requires_authorization_header() {
    let request = build_request(
        Request::get(CHANNEL_PATH).header(header::HOST, "sfu.example.com"),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "room request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn room_rejects_non_bearer_authorization_scheme() {
    let token = signed_room_claims(Some("issuer-a"), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let request = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Basic {token}")),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "room request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn room_rejects_oversized_authorization_token() {
    let token = "a".repeat(auth::MAX_JWT_TOKEN_BYTES + 1);
    let request = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "room request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn room_requires_issuer_claim() {
    let token = signed_room_claims(None, None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let request = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "room request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn room_rejects_recording_without_key() {
    let token = signed_room_claims(Some("issuer-a"), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let request = build_request(
        Request::get(format!(
            "{CHANNEL_PATH}?recordingAddress=https://record.example.com"
        ))
        .header(header::HOST, "sfu.example.com")
        .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "room request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn room_returns_uuid_and_request_base_url() {
    let token = signed_room_claims(Some("issuer-a"), Some("Y2hhbm5lbC1rZXk="));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let request = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}")),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "room request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<RoomResponse> = parse_json(response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert!(!payload.uuid.is_empty());
    assert_eq!(payload.url, "http://sfu.example.com");
}

#[tokio::test]
async fn room_ignores_forwarded_headers_when_proxy_trust_is_disabled() {
    let token = signed_room_claims(Some("issuer-a"), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let create_request = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("x-forwarded-for", "198.51.100.24, 10.0.0.1"),
        Body::empty(),
    );
    assert!(create_request.is_some());
    let Some(create_request) = create_request else {
        return;
    };
    let state = test_state();
    let create_response = app(state.clone()).oneshot(create_request).await;
    assert!(
        create_response.is_ok(),
        "room request should complete: {create_response:?}"
    );
    let Some(create_response) = create_response.ok() else {
        return;
    };
    assert_eq!(create_response.status(), StatusCode::OK);
    let payload: Option<RoomResponse> = parse_json(create_response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.url, "http://sfu.example.com");

    let stats_request = build_request(Request::get(STATS_PATH), Body::empty());
    assert!(stats_request.is_some());
    let Some(stats_request) = stats_request else {
        return;
    };
    let stats_response = app(state).oneshot(stats_request).await;
    assert!(
        stats_response.is_ok(),
        "stats request should succeed: {stats_response:?}"
    );
    let Some(stats_response) = stats_response.ok() else {
        return;
    };
    let payload: Option<StatsResponse> = parse_json(stats_response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.len(), 1);
    let first = payload.first();
    assert!(first.is_some());
    let Some(first) = first else {
        return;
    };
    assert_eq!(first.remote_address, "unknown");
}

#[tokio::test]
async fn room_uses_forwarded_headers_when_proxy_trust_is_enabled() {
    let token = signed_room_claims(Some("issuer-a"), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let create_request = build_request(
        Request::get(CHANNEL_PATH)
            .header(header::HOST, "sfu.example.com")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("x-forwarded-host", "proxy.example.com")
            .header("x-forwarded-proto", "https")
            .header("x-forwarded-for", "198.51.100.24, 10.0.0.1"),
        Body::empty(),
    );
    assert!(create_request.is_some());
    let Some(create_request) = create_request else {
        return;
    };
    let mut state = test_state();
    state.config.http.trust_proxy_headers = true;
    let create_response = app(state.clone()).oneshot(create_request).await;
    assert!(
        create_response.is_ok(),
        "room request should complete: {create_response:?}"
    );
    let Some(create_response) = create_response.ok() else {
        return;
    };
    assert_eq!(create_response.status(), StatusCode::OK);
    let payload: Option<RoomResponse> = parse_json(create_response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.url, "https://proxy.example.com");

    let stats_request = build_request(Request::get(STATS_PATH), Body::empty());
    assert!(stats_request.is_some());
    let Some(stats_request) = stats_request else {
        return;
    };
    let stats_response = app(state).oneshot(stats_request).await;
    assert!(
        stats_response.is_ok(),
        "stats request should succeed: {stats_response:?}"
    );
    let Some(stats_response) = stats_response.ok() else {
        return;
    };
    let payload: Option<StatsResponse> = parse_json(stats_response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.len(), 1);
    let first = payload.first();
    assert!(first.is_some());
    let Some(first) = first else {
        return;
    };
    assert_eq!(first.remote_address, "198.51.100.24");
}

#[tokio::test]
async fn room_route_updates_metrics_for_unauthorized_and_success_paths() {
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

    let token = signed_room_claims(Some("issuer-a"), None);
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
    assert_eq!(metrics.http_room_requests(), 2);
    assert_eq!(metrics.http_room_unauthorized(), 1);
    assert_eq!(metrics.http_room_success(), 1);
}
