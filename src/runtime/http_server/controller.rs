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

use crate::{
    config::Config,
    runtime::{
        RuntimeState,
        channel::{ChannelConfig, RuntimeChannelStatsSnapshot},
        websocket_server,
    },
    signaling::{
        auth::{self, HttpChannelClaims, HttpDisconnectClaims},
        http::{
            CHANNEL_PATH, ChannelResponse, ChannelStats, CreateChannelQuery, DISCONNECT_PATH,
            IncomingBitRateStats, NOOP_PATH, NoopResponse, STATS_PATH, SessionsStats,
        },
    },
};

pub(crate) async fn serve_http(state: RuntimeState) -> Result<()> {
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

pub(crate) fn app(state: RuntimeState) -> Router {
    Router::new()
        .route("/", get(websocket_server::upgrade))
        .route(NOOP_PATH, get(noop))
        .route(STATS_PATH, get(stats))
        .route(CHANNEL_PATH, get(channel))
        .route(DISCONNECT_PATH, post(disconnect))
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
            .disconnect_sessions(channel_uuid, session_ids, &state.transport_adapter)
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
