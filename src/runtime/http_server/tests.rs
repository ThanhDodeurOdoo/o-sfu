use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header, request::Builder as HttpRequestBuilder},
    response::Response as AxumResponse,
};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tower::util::ServiceExt;

use super::app;
use crate::{
    config::{Config, RtcPortRange, TransportBackend},
    runtime::{
        RuntimeState,
        channel::{ChannelConfig, ChannelManager},
        metrics::RuntimeMetrics,
        transport_adapter::RuntimeTransportAdapter,
    },
    signaling::{
        auth::{self, HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims},
        http::{
            CHANNEL_PATH, ChannelResponse, CreateChannelQuery, DISCONNECT_PATH, NOOP_PATH,
            NoopResponse, STATS_PATH, StatsResponse,
        },
        shared::{SessionId, SessionInfo, SessionPermissions},
    },
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fn test_config() -> Config {
    Config {
        auth_key: TEST_AUTH_KEY.to_owned(),
        bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
        authentication_timeout_ms: 10_000,
        channel_size: 100,
        session_timeout_ms: 10_000,
        ping_interval_ms: 60_000,
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        transport_backend: TransportBackend::Stub,
    }
}

fn test_state() -> RuntimeState {
    RuntimeState {
        config: test_config(),
        channels: Arc::new(ChannelManager::new()),
        metrics: Arc::new(RuntimeMetrics::default()),
        transport_adapter: RuntimeTransportAdapter::stub(),
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
async fn stats_returns_live_channel_data() {
    let state = test_state();
    let query = CreateChannelQuery::default();
    let channel = state
        .channels
        .create_or_get(
            "issuer-a",
            None,
            &ChannelConfig {
                web_rtc_enabled: query.web_rtc_enabled(),
                recording_address: query.recording_address.clone(),
            },
            Some("203.0.113.10"),
        )
        .await;
    let (alice_tx, _alice_rx) = mpsc::unbounded_channel();
    let (bob_tx, _bob_rx) = mpsc::unbounded_channel();
    let alice_join = channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions::default(),
            alice_tx,
            10,
        )
        .await;
    let bob_join = channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            bob_tx,
            10,
        )
        .await;
    assert!(alice_join.is_ok());
    assert!(bob_join.is_ok());
    channel
        .update_session_info(
            &SessionId::Integer(1),
            SessionInfo {
                is_camera_on: Some(true),
                ..SessionInfo::default()
            },
            false,
        )
        .await;
    channel
        .update_session_info(
            &SessionId::Integer(2),
            SessionInfo {
                is_screen_sharing_on: Some(true),
                ..SessionInfo::default()
            },
            false,
        )
        .await;

    let request = build_request(Request::get(STATS_PATH), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(state).oneshot(request).await;
    assert!(
        response.is_ok(),
        "stats request should succeed: {response:?}"
    );
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Option<StatsResponse> = parse_json(response).await;
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
    assert_eq!(first.uuid, channel.uuid());
    assert_eq!(first.remote_address, "203.0.113.10");
    assert_eq!(first.sessions_stats.count, 2);
    assert_eq!(first.sessions_stats.camera_count, 1);
    assert_eq!(first.sessions_stats.screen_count, 1);
    assert_eq!(first.sessions_stats.incoming_bit_rate.total, 0);
    assert_eq!(first.sessions_stats.incoming_bit_rate.audio, 0);
    assert_eq!(first.sessions_stats.incoming_bit_rate.camera, 0);
    assert_eq!(first.sessions_stats.incoming_bit_rate.screen, 0);
    assert!(first.web_rtc_enabled);
    assert!(first.create_date.contains('T'));
    assert!(first.create_date.ends_with('Z'));
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
async fn channel_uses_forwarded_remote_address_for_stats() {
    let token = signed_channel_claims(Some("issuer-a"), None);
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
        "channel request should complete: {create_response:?}"
    );
    let Some(create_response) = create_response.ok() else {
        return;
    };
    assert_eq!(create_response.status(), StatusCode::OK);

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
