#![allow(
    clippy::panic,
    reason = "disconnect route tests use panic helpers for mandatory fixture setup failures"
)]

use std::fmt::Debug;

use super::fixtures::*;

fn require_some<T>(value: Option<T>, context: &str) -> T {
    value.unwrap_or_else(|| panic!("{context}"))
}

fn require_ok<T, E: Debug>(value: Result<T, E>, context: &str) -> T {
    match value {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error:?}"),
    }
}

#[tokio::test]
async fn disconnect_rejects_invalid_utf8_body() {
    let request = require_some(
        build_request(
            Request::post(DISCONNECT_PATH),
            Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
        ),
        "invalid UTF-8 disconnect request should build",
    );
    let response = require_ok(
        app(test_state()).oneshot(request).await,
        "disconnect request should complete",
    );
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn disconnect_requires_valid_jwt() {
    let request = require_some(
        build_request(Request::post(DISCONNECT_PATH), Body::from("invalid-token")),
        "invalid-token disconnect request should build",
    );
    let response = require_ok(
        app(test_state()).oneshot(request).await,
        "disconnect request should complete",
    );
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn disconnect_rejects_oversized_body_before_auth_decode() {
    let oversized_body = "x".repeat(auth::MAX_JWT_TOKEN_BYTES + 1);
    let request = require_some(
        build_request(Request::post(DISCONNECT_PATH), Body::from(oversized_body)),
        "oversized disconnect request should build",
    );
    let response = require_ok(
        app(test_state()).oneshot(request).await,
        "oversized disconnect request should complete",
    );
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn disconnect_accepts_valid_jwt() {
    let token = require_some(
        signed_disconnect_claims(BTreeMap::new()),
        "disconnect JWT should sign",
    );
    let request = require_some(
        build_request(Request::post(DISCONNECT_PATH), Body::from(token)),
        "valid disconnect request should build",
    );
    let response = require_ok(
        app(test_state()).oneshot(request).await,
        "disconnect request should complete",
    );
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn disconnect_route_updates_metrics_for_all_outcomes() {
    let state = test_state();

    let invalid_utf8 = require_some(
        build_request(
            Request::post(DISCONNECT_PATH),
            Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
        ),
        "invalid UTF-8 disconnect request should build",
    );
    let invalid_utf8_response = require_ok(
        app(state.clone()).oneshot(invalid_utf8).await,
        "invalid UTF-8 disconnect request should complete",
    );
    assert_eq!(invalid_utf8_response.status(), StatusCode::BAD_REQUEST);

    let invalid_claims = require_some(
        build_request(Request::post(DISCONNECT_PATH), Body::from("invalid-token")),
        "invalid-token disconnect request should build",
    );
    let invalid_claims_response = require_ok(
        app(state.clone()).oneshot(invalid_claims).await,
        "invalid-token disconnect request should complete",
    );
    assert_eq!(
        invalid_claims_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let token = require_some(
        signed_disconnect_claims(BTreeMap::new()),
        "disconnect JWT should sign",
    );
    let success = require_some(
        build_request(Request::post(DISCONNECT_PATH), Body::from(token)),
        "valid disconnect request should build",
    );
    let success_response = require_ok(
        app(state.clone()).oneshot(success).await,
        "valid disconnect request should complete",
    );
    assert_eq!(success_response.status(), StatusCode::OK);

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_disconnect_requests(), 3);
    assert_eq!(metrics.http_disconnect_bad_request(), 1);
    assert_eq!(metrics.http_disconnect_unprocessable_entity(), 1);
    assert_eq!(metrics.http_disconnect_success(), 1);
}
