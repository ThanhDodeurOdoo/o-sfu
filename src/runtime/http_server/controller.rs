use std::{net::SocketAddr, str};

use anyhow::Result;
use axum::{
    Extension, Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    config::Config,
    runtime::{
        RuntimeState,
        auth::{self, HttpChannelClaims, HttpDisconnectClaims},
        channel::{ChannelConfig, RuntimeChannelStatsSnapshot},
        http_server::contract::{
            CHANNEL_PATH, ChannelResponse, ChannelStats, CreateChannelQuery, DISCONNECT_PATH,
            IncomingBitRateStats, METRICS_PATH, NOOP_PATH, NoopResponse, STATS_PATH, SessionsStats,
        },
        metrics_export::{PROMETHEUS_CONTENT_TYPE, render_prometheus},
        websocket_server,
    },
};

const MAX_DISCONNECT_BODY_BYTES: usize = 16 * 1024;

pub(crate) async fn serve_http(state: RuntimeState) -> Result<()> {
    let listener = TcpListener::bind(state.config.bind_address).await?;
    let local_address = listener.local_addr()?;
    info!(
        bind_address = %state.config.bind_address,
        local_address = %local_address,
        trust_proxy_headers = state.config.trust_proxy_headers,
        "booted HTTP and WebSocket listener"
    );
    axum::serve(
        listener,
        app(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// Builds the Axum router for the HTTP control plane and WebSocket listener.
pub(crate) fn app(state: RuntimeState) -> Router {
    Router::new()
        .route("/", get(websocket_server::upgrade))
        .route(METRICS_PATH, get(metrics))
        .route(NOOP_PATH, get(noop))
        .route(STATS_PATH, get(stats))
        .route(CHANNEL_PATH, get(channel))
        .route(
            DISCONNECT_PATH,
            post(disconnect).layer(DefaultBodyLimit::max(MAX_DISCONNECT_BODY_BYTES)),
        )
        .with_state(state)
}

async fn noop(State(state): State<RuntimeState>) -> impl IntoResponse {
    state.metrics.record_http_noop_request();
    axum::Json(NoopResponse::ok())
}

async fn stats(State(state): State<RuntimeState>) -> impl IntoResponse {
    state.metrics.record_http_stats_request();
    axum::Json(
        state
            .channels
            .stats_snapshots(&state.transport_adapter)
            .await
            .into_iter()
            .map(http_channel_stats)
            .collect::<Vec<_>>(),
    )
}

async fn metrics(State(state): State<RuntimeState>) -> impl IntoResponse {
    state.metrics.record_http_metrics_request();
    (
        [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        render_prometheus(&state.metrics),
    )
}

/// This is the entry point for the Odoo server to request a channel.
///
/// The bearer token is decoded through `auth::verify`, so JWT header, payload, and signature
/// segments must use the JOSE base64url alphabet without padding.
///
/// Query parameters (defined in [`CreateChannelQuery`]) specify whether the channel
/// should have WebRTC enabled and optional webhook endpoints for recordings.
async fn channel(
    State(state): State<RuntimeState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    Query(query): Query<CreateChannelQuery>,
) -> Response {
    state.metrics.record_http_channel_request();
    let Some(token) = authorization_token(&headers) else {
        state.metrics.record_http_channel_unauthorized();
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(claims) = auth::verify::<HttpChannelClaims>(token, &state.config.auth_key) else {
        state.metrics.record_http_channel_unauthorized();
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(issuer) = claims.registered.iss.as_deref() else {
        state.metrics.record_http_channel_forbidden();
        return StatusCode::FORBIDDEN.into_response();
    };
    if query.recording_address.is_some() && claims.key.is_none() {
        state.metrics.record_http_channel_bad_request();
        return StatusCode::BAD_REQUEST.into_response();
    }
    let remote_address = request_remote_address(
        &headers,
        &state.config,
        connect_info.map(|Extension(ConnectInfo(addr))| addr),
    );
    let channel = state
        .channels
        .create_or_get(
            issuer,
            claims.key.as_deref(),
            &ChannelConfig {
                web_rtc_enabled: query.web_rtc_enabled(),
                recording_address: query.recording_address.clone(),
            },
            Some(&remote_address),
        )
        .await;
    state.metrics.record_http_channel_success();
    (
        StatusCode::OK,
        axum::Json(ChannelResponse {
            uuid: channel.uuid().to_owned(),
            url: request_base_url(&headers, &state.config),
        }),
    )
        .into_response()
}

/// Authorized bulk-disconnect route.
///
/// Disconnects multiple users from a channel. This is used by the Odoo server to
/// forcefully kick users out or clean up abandoned sessions.
///
/// The request body is decoded through `auth::verify`, so JWT header, payload, and signature
/// segments must use the JOSE base64url alphabet without padding.
async fn disconnect(State(state): State<RuntimeState>, body: Bytes) -> Response {
    state.metrics.record_http_disconnect_request();
    let Ok(token) = str::from_utf8(&body) else {
        state.metrics.record_http_disconnect_bad_request();
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(claims) = auth::verify::<HttpDisconnectClaims>(token, &state.config.auth_key) else {
        state.metrics.record_http_disconnect_unprocessable_entity();
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    };
    for (channel_uuid, session_ids) in &claims.session_ids_by_channel {
        state
            .channels
            .disconnect_sessions(
                channel_uuid,
                session_ids,
                &state.transport_adapter,
                RuntimeState::session_cleanup_policy(),
            )
            .await;
    }
    state.metrics.record_http_disconnect_success();
    StatusCode::OK.into_response()
}

fn authorization_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' ').map(|(_, token)| token))
}

fn request_base_url(headers: &HeaderMap, config: &Config) -> String {
    let scheme = trusted_forwarded_header(headers, config, "x-forwarded-proto").unwrap_or("http");
    let host = trusted_forwarded_header(headers, config, "x-forwarded-host")
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

fn request_remote_address(
    headers: &HeaderMap,
    config: &Config,
    connect_info: Option<SocketAddr>,
) -> String {
    trusted_forwarded_header(headers, config, "x-forwarded-for")
        .map(str::to_owned)
        .or_else(|| connect_info.map(|addr| addr.ip().to_string()))
        .unwrap_or_else(|| String::from("unknown"))
}

fn trusted_forwarded_header<'headers>(
    headers: &'headers HeaderMap,
    config: &Config,
    name: &str,
) -> Option<&'headers str> {
    if !config.trust_proxy_headers {
        return None;
    }
    forwarded_header(headers, name)
}

fn http_channel_stats(snapshot: RuntimeChannelStatsSnapshot) -> ChannelStats {
    ChannelStats {
        create_date: snapshot.create_date,
        uuid: snapshot.uuid,
        remote_address: snapshot.remote_address,
        sessions_stats: SessionsStats {
            incoming_bit_rate: IncomingBitRateStats {
                total: snapshot.sessions_stats.incoming_bitrate.total,
                audio: snapshot.sessions_stats.incoming_bitrate.audio,
                camera: snapshot.sessions_stats.incoming_bitrate.camera,
                screen: snapshot.sessions_stats.incoming_bitrate.screen,
            },
            count: snapshot.sessions_stats.count,
            camera_count: snapshot.sessions_stats.camera_count,
            screen_count: snapshot.sessions_stats.screen_count,
        },
        web_rtc_enabled: snapshot.web_rtc_enabled,
    }
}

fn forwarded_header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let value = headers.get(name)?.to_str().ok()?;
    value.split(',').next().map(str::trim)
}
