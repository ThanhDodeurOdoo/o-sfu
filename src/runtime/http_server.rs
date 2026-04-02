use std::{str, sync::Arc};

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

use super::stub_channels::StubChannelRegistry;
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

#[derive(Debug, Clone)]
struct HttpState {
    config: Config,
    stub_channels: Arc<StubChannelRegistry>,
}

pub async fn serve_http(config: Config, stub_channels: Arc<StubChannelRegistry>) -> Result<()> {
    let listener = TcpListener::bind(config.bind_address).await?;
    axum::serve(listener, app(config, stub_channels)).await?;
    Ok(())
}

fn app(config: Config, stub_channels: Arc<StubChannelRegistry>) -> Router {
    let state = HttpState {
        config,
        stub_channels,
    };
    Router::new()
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
    State(state): State<HttpState>,
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
    let stub_channel = state
        .stub_channels
        .create_or_get(issuer, claims.key.as_deref(), &query)
        .await;
    (
        StatusCode::OK,
        axum::Json(ChannelResponse {
            uuid: stub_channel.uuid,
            url: request_base_url(&headers, &state.config),
        }),
    )
        .into_response()
}

async fn disconnect(State(state): State<HttpState>, body: Bytes) -> Response {
    let Ok(token) = str::from_utf8(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match auth::verify::<HttpDisconnectClaims>(token, &state.config.auth_key) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::UNPROCESSABLE_ENTITY.into_response(),
    }
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
        runtime::stub_channels::StubChannelRegistry,
        signaling::{
            auth::{self, HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims},
            http::{
                CHANNEL_PATH, ChannelResponse, DISCONNECT_PATH, NOOP_PATH, NoopResponse, STATS_PATH,
            },
        },
    };

    const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

    fn test_config() -> Config {
        Config {
            auth_key: TEST_AUTH_KEY.to_owned(),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
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

    fn signed_disconnect_claims() -> Option<String> {
        auth::sign(
            &HttpDisconnectClaims {
                registered: RegisteredJwtClaims::default(),
                session_ids_by_channel: BTreeMap::new(),
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
        let response = app(test_config(), Arc::new(StubChannelRegistry::new()))
            .oneshot(request)
            .await;
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
        let response = app(test_config(), Arc::new(StubChannelRegistry::new()))
            .oneshot(request)
            .await;
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
        let response = app(test_config(), Arc::new(StubChannelRegistry::new()))
            .oneshot(request)
            .await;
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
        let response = app(test_config(), Arc::new(StubChannelRegistry::new()))
            .oneshot(request)
            .await;
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
        let response = app(test_config(), Arc::new(StubChannelRegistry::new()))
            .oneshot(request)
            .await;
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
    async fn channel_is_idempotent_by_issuer() {
        let app = app(test_config(), Arc::new(StubChannelRegistry::new()));
        let first_token = signed_channel_claims(Some("issuer-a"), Some("Y2hhbm5lbC1rZXk="));
        assert!(first_token.is_some());
        let Some(first_token) = first_token else {
            return;
        };
        let first_request = build_request(
            Request::get(CHANNEL_PATH)
                .header(header::HOST, "sfu.example.com")
                .header(header::AUTHORIZATION, format!("Bearer {first_token}")),
            Body::empty(),
        );
        assert!(first_request.is_some());
        let Some(first_request) = first_request else {
            return;
        };
        let first_response = app.clone().oneshot(first_request).await;
        assert!(
            first_response.is_ok(),
            "first channel request should complete: {first_response:?}"
        );
        let Some(first_response) = first_response.ok() else {
            return;
        };
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_payload: Option<ChannelResponse> = parse_json(first_response).await;
        assert!(first_payload.is_some());
        let Some(first_payload) = first_payload else {
            return;
        };

        let second_token = signed_channel_claims(Some("issuer-a"), Some("Y2hhbm5lbC1rZXk="));
        assert!(second_token.is_some());
        let Some(second_token) = second_token else {
            return;
        };
        let second_request = build_request(
            Request::get(CHANNEL_PATH)
                .header(header::HOST, "sfu.example.com")
                .header(header::AUTHORIZATION, format!("Bearer {second_token}")),
            Body::empty(),
        );
        assert!(second_request.is_some());
        let Some(second_request) = second_request else {
            return;
        };
        let second_response = app.clone().oneshot(second_request).await;
        assert!(
            second_response.is_ok(),
            "second channel request should complete: {second_response:?}"
        );
        let Some(second_response) = second_response.ok() else {
            return;
        };
        let second_payload: Option<ChannelResponse> = parse_json(second_response).await;
        assert!(second_payload.is_some());
        let Some(second_payload) = second_payload else {
            return;
        };

        let third_token = signed_channel_claims(Some("issuer-b"), Some("Y2hhbm5lbC1rZXk="));
        assert!(third_token.is_some());
        let Some(third_token) = third_token else {
            return;
        };
        let third_request = build_request(
            Request::get(CHANNEL_PATH)
                .header(header::HOST, "sfu.example.com")
                .header(header::AUTHORIZATION, format!("Bearer {third_token}")),
            Body::empty(),
        );
        assert!(third_request.is_some());
        let Some(third_request) = third_request else {
            return;
        };
        let third_response = app.oneshot(third_request).await;
        assert!(
            third_response.is_ok(),
            "third channel request should complete: {third_response:?}"
        );
        let Some(third_response) = third_response.ok() else {
            return;
        };
        let third_payload: Option<ChannelResponse> = parse_json(third_response).await;
        assert!(third_payload.is_some());
        let Some(third_payload) = third_payload else {
            return;
        };

        assert_eq!(first_payload.uuid, second_payload.uuid);
        assert_ne!(first_payload.uuid, third_payload.uuid);
        assert_eq!(first_payload.url, "http://sfu.example.com");
    }

    #[tokio::test]
    async fn disconnect_validates_jwt() {
        let app = app(test_config(), Arc::new(StubChannelRegistry::new()));
        let valid_token = signed_disconnect_claims();
        assert!(valid_token.is_some());
        let Some(valid_token) = valid_token else {
            return;
        };
        let valid_request = build_request(Request::post(DISCONNECT_PATH), Body::from(valid_token));
        assert!(valid_request.is_some());
        let Some(valid_request) = valid_request else {
            return;
        };
        let valid_response = app.clone().oneshot(valid_request).await;
        assert!(
            valid_response.is_ok(),
            "valid disconnect request should complete: {valid_response:?}"
        );
        let Some(valid_response) = valid_response.ok() else {
            return;
        };
        assert_eq!(valid_response.status(), StatusCode::OK);

        let invalid_request =
            build_request(Request::post(DISCONNECT_PATH), Body::from("not-a-jwt"));
        assert!(invalid_request.is_some());
        let Some(invalid_request) = invalid_request else {
            return;
        };
        let invalid_response = app.oneshot(invalid_request).await;
        assert!(
            invalid_response.is_ok(),
            "invalid disconnect request should complete: {invalid_response:?}"
        );
        let Some(invalid_response) = invalid_response.ok() else {
            return;
        };
        assert_eq!(invalid_response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
