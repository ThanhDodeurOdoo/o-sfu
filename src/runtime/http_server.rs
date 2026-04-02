use std::str;

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tokio::net::TcpListener;

use super::{RuntimeState, websocket_server};
use crate::{
    config::Config,
    signaling::{
        auth::{self, HttpChannelClaims, HttpDisconnectClaims},
        http::{
            CHANNEL_PATH, ChannelResponse, ChannelStats, CreateChannelQuery, DISCONNECT_PATH,
            NOOP_PATH, NoopResponse, STATS_PATH, StatsResponse,
        },
    },
};

pub async fn serve_http(state: RuntimeState) -> Result<()> {
    let listener = TcpListener::bind(state.config.bind_address).await?;
    axum::serve(listener, app(state)).await?;
    Ok(())
}

pub(super) fn app(state: RuntimeState) -> Router {
    Router::new()
        .route("/", get(websocket_server::upgrade))
        .route(NOOP_PATH, get(noop))
        .route(STATS_PATH, get(stats))
        .route(CHANNEL_PATH, get(channel))
        .route(DISCONNECT_PATH, post(disconnect))
        .with_state(state)
}

async fn noop() -> impl IntoResponse {
    axum::Json(NoopResponse::ok())
}

async fn stats() -> impl IntoResponse {
    let placeholder: StatsResponse = Vec::<ChannelStats>::new();
    axum::Json(placeholder)
}

async fn channel(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Query(query): Query<CreateChannelQuery>,
) -> Response {
    let Some(token) = authorization_token(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(claims) = auth::verify::<HttpChannelClaims>(token, &state.config.auth_key) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(issuer) = claims.registered.iss.as_deref() else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if query.recording_address.is_some() && claims.key.is_none() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let channel = state
        .channels
        .create_or_get(issuer, claims.key.as_deref(), &query)
        .await;
    (
        StatusCode::OK,
        axum::Json(ChannelResponse {
            uuid: channel.uuid().to_owned(),
            url: request_base_url(&headers, &state.config),
        }),
    )
        .into_response()
}

async fn disconnect(State(state): State<RuntimeState>, body: Bytes) -> Response {
    let Ok(token) = str::from_utf8(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(claims) = auth::verify::<HttpDisconnectClaims>(token, &state.config.auth_key) else {
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    };
    for (channel_uuid, session_ids) in &claims.session_ids_by_channel {
        state
            .channels
            .disconnect_sessions(channel_uuid, session_ids)
            .await;
    }
    StatusCode::OK.into_response()
}

fn authorization_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' ').map(|(_, token)| token))
}

fn request_base_url(headers: &HeaderMap, config: &Config) -> String {
    let scheme = forwarded_header(headers, "x-forwarded-proto").unwrap_or("http");
    let host = forwarded_header(headers, "x-forwarded-host")
        .map(str::to_owned)
        .or_else(|| {
            headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| config.bind_address.to_string());
    format!("{scheme}://{host}")
}

fn forwarded_header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let value = headers.get(name)?.to_str().ok()?;
    value.split(',').next().map(str::trim)
}

#[cfg(test)]
mod tests {
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
}
