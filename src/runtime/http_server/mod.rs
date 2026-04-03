use std::{net::SocketAddr, str};

use anyhow::Result;
use axum::{
    Extension, Router,
    body::Bytes,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tokio::net::TcpListener;
use tracing::info;

use super::{RuntimeState, websocket_server};
use crate::{
    config::Config,
    signaling::{
        auth::{self, HttpChannelClaims, HttpDisconnectClaims},
        http::{
            CHANNEL_PATH, ChannelResponse, CreateChannelQuery, DISCONNECT_PATH, NOOP_PATH,
            NoopResponse, STATS_PATH,
        },
    },
};

pub async fn serve_http(state: RuntimeState) -> Result<()> {
    info!(
        bind_address = %state.config.bind_address,
        "starting HTTP and WebSocket listener"
    );
    let listener = TcpListener::bind(state.config.bind_address).await?;
    axum::serve(
        listener,
        app(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
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

async fn stats(State(state): State<RuntimeState>) -> impl IntoResponse {
    axum::Json(state.channels.stats().await)
}

async fn channel(
    State(state): State<RuntimeState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
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
    let remote_address = request_remote_address(
        &headers,
        connect_info.map(|Extension(ConnectInfo(addr))| addr),
    );
    let channel = state
        .channels
        .create_or_get_with_remote_address(issuer, claims.key.as_deref(), &remote_address, &query)
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

fn request_remote_address(headers: &HeaderMap, connect_info: Option<SocketAddr>) -> String {
    forwarded_header(headers, "x-forwarded-for")
        .map(str::to_owned)
        .or_else(|| connect_info.map(|addr| addr.ip().to_string()))
        .unwrap_or_else(|| String::from("unknown"))
}

fn forwarded_header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let value = headers.get(name)?.to_str().ok()?;
    value.split(',').next().map(str::trim)
}

#[cfg(test)]
mod tests;
