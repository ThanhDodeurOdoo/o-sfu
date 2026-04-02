use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header, request::Builder as HttpRequestBuilder},
    response::Response as AxumResponse,
};
use serde::de::DeserializeOwned;
use tower::util::ServiceExt;

use super::app;
use crate::{
    config::Config,
    runtime::{RuntimeState, channel::ChannelManager},
    signaling::{
        auth::{self, HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims},
        http::{
            CHANNEL_PATH, ChannelResponse, DISCONNECT_PATH, NOOP_PATH, NoopResponse, STATS_PATH,
        },
        shared::SessionId,
    },
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fn test_config() -> Config {
    Config {
        auth_key: TEST_AUTH_KEY.to_owned(),
        bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
        authentication_timeout_ms: 10_000,
        channel_size: 100,
    }
}

fn test_state() -> RuntimeState {
    RuntimeState {
        config: test_config(),
        channels: Arc::new(ChannelManager::new()),
    }
}

fn signed_channel_claims(issuer: Option<&str>, key: Option<&str>) -> Option<String> {
    auth::sign(
        &HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: issuer.map(str::to_owned),
                ..RegisteredJwtClaims::default()
            },
            key: key.map(str::to_owned),
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

fn signed_disconnect_claims(
    session_ids_by_channel: BTreeMap<String, Vec<SessionId>>,
) -> Option<String> {
    auth::sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            session_ids_by_channel,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

fn build_request(builder: HttpRequestBuilder, body: Body) -> Option<Request<Body>> {
    builder.body(body).ok()
}

async fn parse_json<T>(response: AxumResponse) -> Option<T>
where
    T: DeserializeOwned,
{
    let bytes = to_bytes(response.into_body(), usize::MAX).await.ok()?;
    serde_json::from_slice::<T>(&bytes).ok()
}

#[tokio::test]
async fn noop_returns_ok_response() {
    let request = build_request(Request::get(NOOP_PATH), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "noop request should succeed: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<NoopResponse> = parse_json(response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload.result, "ok");
}

#[tokio::test]
async fn stats_returns_placeholder_data() {
    let request = build_request(Request::get(STATS_PATH), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state()).oneshot(request).await;
    assert!(
        response.is_ok(),
        "stats request should succeed: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<serde_json::Value> = parse_json(response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_eq!(payload, serde_json::json!([]));
}

#[tokio::test]
async fn channel_requires_authorization_header() {
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
        "channel request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn channel_requires_issuer_claim() {
    let token = signed_channel_claims(None, None);
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
        "channel request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn channel_rejects_recording_without_key() {
    let token = signed_channel_claims(Some("issuer-a"), None);
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
        "channel request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn channel_returns_uuid_and_request_base_url() {
    let token = signed_channel_claims(Some("issuer-a"), Some("Y2hhbm5lbC1rZXk="));
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
        "channel request should complete: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<ChannelResponse> = parse_json(response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert!(!payload.uuid.is_empty());
    assert_eq!(payload.url, "http://sfu.example.com");
}

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
