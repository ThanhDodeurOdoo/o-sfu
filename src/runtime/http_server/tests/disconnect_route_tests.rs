use super::fixtures::*;

#[tokio::test]
async fn disconnect_rejects_invalid_utf8_body() -> TestResult {
    route_status(
        &test_state(),
        Request::post(DISCONNECT_PATH),
        Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
        StatusCode::BAD_REQUEST,
        "invalid UTF-8 disconnect request should complete",
    )
    .await
}

#[tokio::test]
async fn disconnect_requires_valid_jwt() -> TestResult {
    route_status(
        &test_state(),
        Request::post(DISCONNECT_PATH),
        Body::from("invalid-token"),
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid-token disconnect request should complete",
    )
    .await
}

#[tokio::test]
async fn disconnect_rejects_oversized_body_before_auth_decode() -> TestResult {
    let oversized_body = "x".repeat(auth::MAX_JWT_TOKEN_BYTES + 1);
    route_status(
        &test_state(),
        Request::post(DISCONNECT_PATH),
        Body::from(oversized_body),
        StatusCode::PAYLOAD_TOO_LARGE,
        "oversized disconnect request should complete",
    )
    .await
}

#[tokio::test]
async fn disconnect_accepts_valid_jwt() -> TestResult {
    let token = require_some(
        signed_disconnect_claims(BTreeMap::new()),
        "disconnect JWT should sign",
    )?;
    route_status(
        &test_state(),
        Request::post(DISCONNECT_PATH),
        Body::from(token),
        StatusCode::OK,
        "disconnect request should complete",
    )
    .await
}

#[tokio::test]
async fn disconnect_route_updates_metrics_for_all_outcomes() -> TestResult {
    let state = test_state();

    route_status(
        &state,
        Request::post(DISCONNECT_PATH),
        Body::from(vec![0xF0_u8, 0x28, 0x8C, 0x28]),
        StatusCode::BAD_REQUEST,
        "invalid UTF-8 disconnect request should complete",
    )
    .await?;

    route_status(
        &state,
        Request::post(DISCONNECT_PATH),
        Body::from("invalid-token"),
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid-token disconnect request should complete",
    )
    .await?;

    let token = require_some(
        signed_disconnect_claims(BTreeMap::new()),
        "disconnect JWT should sign",
    )?;
    route_status(
        &state,
        Request::post(DISCONNECT_PATH),
        Body::from(token),
        StatusCode::OK,
        "valid disconnect request should complete",
    )
    .await?;

    let metrics = state.metrics.snapshot();
    assert_eq!(metrics.http_disconnect_requests(), 3);
    assert_eq!(metrics.http_disconnect_bad_request(), 1);
    assert_eq!(metrics.http_disconnect_unprocessable_entity(), 1);
    assert_eq!(metrics.http_disconnect_success(), 1);
    Ok(())
}
