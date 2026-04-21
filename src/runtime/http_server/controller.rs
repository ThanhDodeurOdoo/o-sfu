use std::{net::SocketAddr, str};

use anyhow::Result;
use axum::{
    Extension, Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tokio::net::TcpListener;
use tracing::{Instrument, info};

use crate::{
    config::Config,
    runtime::{
        RuntimeState,
        auth::{self, HttpChannelClaims, HttpDisconnectClaims},
        channel::{ChannelConfig, RuntimeChannelStatsSnapshot},
        diagnostics::{self, DiagnosticsSessionLookup},
        http_server::contract::{
            CHANNEL_PATH, ChannelResponse, ChannelStats, CreateChannelQuery,
            DIAGNOSTICS_CHANNELS_PATH, DIAGNOSTICS_SUMMARY_PATH, DISCONNECT_PATH,
            IncomingBitRateStats, METRICS_PATH, NOOP_PATH, NoopResponse, STATS_PATH, SessionsStats,
        },
        metrics::HttpRoute,
        metrics_export::{PROMETHEUS_CONTENT_TYPE, render_prometheus},
        request_origin::{resolve_remote_address, trusted_forwarded_header},
        telemetry, websocket_server,
    },
};

const MAX_DISCONNECT_BODY_BYTES: usize = 16 * 1024;

pub(crate) async fn serve_http(state: RuntimeState) -> Result<()> {
    let listener = TcpListener::bind(state.config.bind_address).await?;
    let local_address = listener.local_addr()?;
    info!(
        event = telemetry::schema::event::HTTP_LISTENER_READY,
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
        .route(DIAGNOSTICS_SUMMARY_PATH, get(diagnostics_summary))
        .route(DIAGNOSTICS_CHANNELS_PATH, get(diagnostics_channels))
        .route(
            "/internal/diagnostics/channels/{uuid}",
            get(diagnostics_channel_detail),
        )
        .route(
            "/internal/diagnostics/sessions/{id}",
            get(diagnostics_session_detail),
        )
        .route(
            DISCONNECT_PATH,
            post(disconnect).layer(DefaultBodyLimit::max(MAX_DISCONNECT_BODY_BYTES)),
        )
        .with_state(state)
}

#[o_sfu_telemetry::measure_http_request(
    metrics = "state.metrics",
    request = "record_http_noop_request",
    route = "HttpRoute::Noop"
)]
async fn noop(State(state): State<RuntimeState>) -> impl IntoResponse {
    async { axum::Json(NoopResponse::ok()) }
        .instrument(telemetry::http_request_span("noop"))
        .await
}

#[o_sfu_telemetry::measure_http_request(
    metrics = "state.metrics",
    request = "record_http_stats_request",
    route = "HttpRoute::Stats"
)]
async fn stats(State(state): State<RuntimeState>) -> impl IntoResponse {
    async {
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
    .instrument(telemetry::http_request_span("stats"))
    .await
}

#[o_sfu_telemetry::measure_http_request(
    metrics = "state.metrics",
    request = "record_http_metrics_request",
    route = "HttpRoute::Metrics"
)]
async fn metrics(State(state): State<RuntimeState>) -> impl IntoResponse {
    async {
        (
            [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
            render_prometheus(&state.metrics),
        )
    }
    .instrument(telemetry::http_request_span("metrics"))
    .await
}

/// This is the entry point for the Odoo server to request a channel.
///
/// The bearer token is decoded through `auth::verify`, so JWT header, payload, and signature
/// segments must use the JOSE base64url alphabet without padding.
///
/// Query parameters (defined in [`CreateChannelQuery`]) specify whether the channel
/// should have WebRTC enabled and optional webhook endpoints for recordings.
#[o_sfu_telemetry::measure_http_request(
    metrics = "state.metrics",
    request = "record_http_channel_request",
    route = "HttpRoute::Channel"
)]
async fn channel(
    State(state): State<RuntimeState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    Query(query): Query<CreateChannelQuery>,
) -> Response {
    async {
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
        let remote_address = resolve_remote_address(
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
    .instrument(telemetry::http_request_span("channel"))
    .await
}

/// Authorized bulk-disconnect route.
///
/// Disconnects multiple users from a channel. This is used by the Odoo server to
/// forcefully kick users out or clean up abandoned sessions.
///
/// The request body is decoded through `auth::verify`, so JWT header, payload, and signature
/// segments must use the JOSE base64url alphabet without padding.
#[o_sfu_telemetry::measure_http_request(
    metrics = "state.metrics",
    request = "record_http_disconnect_request",
    route = "HttpRoute::Disconnect"
)]
async fn disconnect(State(state): State<RuntimeState>, body: Bytes) -> Response {
    async {
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
    .instrument(telemetry::http_request_span("disconnect"))
    .await
}

async fn diagnostics_summary(State(state): State<RuntimeState>, headers: HeaderMap) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.config) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        axum::Json(
            diagnostics::summary_response(
                &state.channels,
                &state.transport_adapter,
                &state.diagnostics,
            )
            .await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_channels(State(state): State<RuntimeState>, headers: HeaderMap) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.config) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        axum::Json(
            diagnostics::channels_response(
                &state.channels,
                &state.transport_adapter,
                &state.diagnostics,
            )
            .await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_channel_detail(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path(channel_uuid): Path<String>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.config) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        let Some(payload) = diagnostics::channel_detail_response(
            &state.channels,
            &state.transport_adapter,
            &state.diagnostics,
            &channel_uuid,
        )
        .await
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        axum::Json(payload).into_response()
    }
    .await
}

async fn diagnostics_session_detail(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.config) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        match diagnostics::session_detail_response(
            &state.channels,
            &state.transport_adapter,
            &state.diagnostics,
            &session_id,
        )
        .await
        {
            DiagnosticsSessionLookup::Missing => StatusCode::NOT_FOUND.into_response(),
            DiagnosticsSessionLookup::Found(payload) => axum::Json(payload).into_response(),
            DiagnosticsSessionLookup::Conflict(payload) => {
                (StatusCode::CONFLICT, axum::Json(payload)).into_response()
            }
        }
    }
    .await
}

fn authorization_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' ').map(|(_, token)| token))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticsAccess {
    Allowed,
    Unauthorized,
    Disabled,
}

fn ensure_diagnostics_access(headers: &HeaderMap, config: &Config) -> DiagnosticsAccess {
    if let Some(expected_token) = config.diagnostics.auth_token.as_deref() {
        return match authorization_token(headers) {
            Some(actual_token) if actual_token == expected_token => DiagnosticsAccess::Allowed,
            _ => DiagnosticsAccess::Unauthorized,
        };
    }
    // Without an explicit token we only allow diagnostics on loopback listeners.
    // for example if a reverse proxy is the network boundary we expect it handle that
    if config.bind_address.ip().is_loopback() {
        DiagnosticsAccess::Allowed
    } else {
        DiagnosticsAccess::Disabled
    }
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
