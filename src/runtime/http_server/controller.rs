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

use crate::runtime::{
    RuntimeState,
    auth::{self, HttpDisconnectClaims, HttpRoomClaims},
    diagnostics,
    diagnostics::DiagnosticsUserLookup,
    http_server::contract::{
        CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
        DISCONNECT_PATH, IncomingBitRateStatsResponse, METRICS_PATH, NOOP_PATH, NoopResponse,
        RoomResponse, RoomStatsResponse, STATS_PATH, UsersStatsResponse,
    },
    metrics::HttpRoute,
    options::HttpOptions,
    prometheus::{PROMETHEUS_CONTENT_TYPE, render_prometheus},
    request_origin::{resolve_remote_address, trusted_forwarded_header},
    room::{RoomConfig, RuntimeRoomStatsSnapshot},
    telemetry::{self, schema::event as telemetry_event},
    websocket_server,
};

const MAX_DISCONNECT_BODY_BYTES: usize = 16 * 1024;

pub(crate) async fn serve_http(state: RuntimeState) -> Result<()> {
    let listener = TcpListener::bind(state.http_options.bind_address).await?;
    let local_address = listener.local_addr()?;
    info!(
        event = telemetry_event::HTTP_LISTENER_READY,
        bind_address = %state.http_options.bind_address,
        local_address = %local_address,
        trust_proxy_headers = state.http_options.trust_proxy_headers,
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
        .route(CHANNEL_PATH, get(room))
        .route(DIAGNOSTICS_SUMMARY_PATH, get(diagnostics_summary))
        .route(DIAGNOSTICS_ROOMS_PATH, get(diagnostics_rooms))
        .route(
            "/internal/diagnostics/rooms/{uuid}",
            get(diagnostics_room_detail),
        )
        .route(
            "/internal/diagnostics/rooms/{uuid}/users",
            get(diagnostics_room_users),
        )
        .route(
            "/internal/diagnostics/node-graph/rooms/{uuid}",
            get(diagnostics_room_graph),
        )
        .route(
            "/internal/diagnostics/node-graph/rooms/{uuid}/users/{id}",
            get(diagnostics_user_graph),
        )
        .route(
            "/internal/diagnostics/users/{id}",
            get(diagnostics_user_detail),
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
                .rooms
                .stats_snapshots(&state.media_transport)
                .await
                .into_iter()
                .map(http_room_stats)
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

/// This is the entry point for the Odoo server to request a room.
///
/// The bearer token is decoded through `auth::verify`, so JWT header, payload, and signature
/// segments must use the JOSE base64url alphabet without padding.
///
/// Query parameters (defined in [`CreateRoomQuery`]) specify whether the room
/// should have WebRTC enabled and optional webhook endpoints for recordings.
#[o_sfu_telemetry::measure_http_request(
    metrics = "state.metrics",
    request = "record_http_room_request",
    route = "HttpRoute::Room"
)]
async fn room(
    State(state): State<RuntimeState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    Query(query): Query<CreateRoomQuery>,
) -> Response {
    async {
        let Some(token) = authorization_token(&headers) else {
            state.metrics.record_http_room_unauthorized();
            return StatusCode::UNAUTHORIZED.into_response();
        };
        let Ok(claims) = auth::verify::<HttpRoomClaims>(token, &state.http_options.auth.key) else {
            state.metrics.record_http_room_unauthorized();
            return StatusCode::UNAUTHORIZED.into_response();
        };
        let Some(issuer) = claims.registered.iss.as_deref() else {
            state.metrics.record_http_room_forbidden();
            return StatusCode::FORBIDDEN.into_response();
        };
        if query.recording_address.is_some() && claims.key.is_none() {
            state.metrics.record_http_room_bad_request();
            return StatusCode::BAD_REQUEST.into_response();
        }

        let remote_address = resolve_remote_address(
            &headers,
            state.http_options.trust_proxy_headers,
            connect_info.map(|Extension(ConnectInfo(addr))| addr),
        );
        let room_config = RoomConfig {
            web_rtc_enabled: query.web_rtc_enabled(),
            recording_address: query.recording_address,
        };
        let room = state
            .rooms
            .serve_room(
                issuer,
                claims.key.as_deref(),
                &room_config,
                Some(remote_address.as_str()),
            )
            .await;
        state.metrics.record_http_room_success();
        (
            StatusCode::OK,
            axum::Json(RoomResponse {
                uuid: room.uuid().to_owned(),
                url: request_base_url(&headers, &state.http_options),
            }),
        )
            .into_response()
    }
    .instrument(telemetry::http_request_span("room"))
    .await
}

/// Authorized bulk-disconnect route.
///
/// Disconnects multiple users from a room. This is used by the Odoo server to
/// forcefully kick users out or clean up abandoned users.
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
        let Ok(mut claims) =
            auth::verify::<HttpDisconnectClaims>(token, &state.http_options.auth.key)
        else {
            state.metrics.record_http_disconnect_unprocessable_entity();
            return StatusCode::UNPROCESSABLE_ENTITY.into_response();
        };
        claims.normalize_runtime_user_ids();
        for (room_id, user_ids) in &claims.user_ids_by_room {
            state
                .rooms
                .disconnect_users(room_id, user_ids, &state.media_transport)
                .await;
        }
        state.metrics.record_http_disconnect_success();
        StatusCode::OK.into_response()
    }
    .instrument(telemetry::http_request_span("disconnect"))
    .await
}

fn authorization_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' ').map(|(_, token)| token))
}

pub(crate) fn request_base_url(headers: &HeaderMap, options: &HttpOptions) -> String {
    let scheme =
        trusted_forwarded_header(headers, options.trust_proxy_headers, "x-forwarded-proto")
            .unwrap_or("http");
    let host = trusted_forwarded_header(headers, options.trust_proxy_headers, "x-forwarded-host")
        .map(str::to_owned)
        .or_else(|| {
            headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| options.bind_address.to_string());
    format!("{scheme}://{host}")
}

async fn diagnostics_summary(State(state): State<RuntimeState>, headers: HeaderMap) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        axum::Json(
            diagnostics::summary_response(&state.rooms, &state.media_transport, &state.diagnostics)
                .await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_rooms(State(state): State<RuntimeState>, headers: HeaderMap) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        axum::Json(
            diagnostics::rooms_response(&state.rooms, &state.media_transport, &state.diagnostics)
                .await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_room_detail(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        let Some(payload) = diagnostics::room_detail_response(
            &state.rooms,
            &state.media_transport,
            &state.diagnostics,
            &room_id,
        )
        .await
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        axum::Json(payload).into_response()
    }
    .await
}

async fn diagnostics_room_users(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        let Some(payload) = diagnostics::room_users_response(
            &state.rooms,
            &state.media_transport,
            &state.diagnostics,
            &room_id,
        )
        .await
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        axum::Json(payload).into_response()
    }
    .await
}

async fn diagnostics_room_graph(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        let Some(payload) = diagnostics::room_detail_response(
            &state.rooms,
            &state.media_transport,
            &state.diagnostics,
            &room_id,
        )
        .await
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let graph = diagnostics::build_graph(&payload);
        axum::Json(graph).into_response()
    }
    .await
}

async fn diagnostics_user_graph(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path((room_id, user_id)): Path<(String, String)>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        let Some(payload) = diagnostics::room_detail_response(
            &state.rooms,
            &state.media_transport,
            &state.diagnostics,
            &room_id,
        )
        .await
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let Some(graph) = diagnostics::build_user_graph(&payload, &user_id) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        axum::Json(graph).into_response()
    }
    .await
}

async fn diagnostics_user_detail(
    State(state): State<RuntimeState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Response {
    async {
        match ensure_diagnostics_access(&headers, &state.http_options) {
            DiagnosticsAccess::Allowed => {}
            DiagnosticsAccess::Unauthorized => return StatusCode::UNAUTHORIZED.into_response(),
            DiagnosticsAccess::Disabled => return StatusCode::FORBIDDEN.into_response(),
        }
        match diagnostics::user_detail_response(
            &state.rooms,
            &state.media_transport,
            &state.diagnostics,
            &user_id,
        )
        .await
        {
            DiagnosticsUserLookup::Missing => StatusCode::NOT_FOUND.into_response(),
            DiagnosticsUserLookup::Found(payload) => axum::Json(payload).into_response(),
            DiagnosticsUserLookup::Conflict(payload) => {
                (StatusCode::CONFLICT, axum::Json(payload)).into_response()
            }
        }
    }
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticsAccess {
    Allowed,
    Unauthorized,
    Disabled,
}

fn ensure_diagnostics_access(headers: &HeaderMap, options: &HttpOptions) -> DiagnosticsAccess {
    if let Some(expected_token) = options.diagnostics.auth_token.as_deref() {
        return match authorization_token(headers) {
            Some(actual_token) if tokens_match(actual_token, expected_token) => {
                DiagnosticsAccess::Allowed
            }
            _ => DiagnosticsAccess::Unauthorized,
        };
    }
    // Without an explicit token we only allow diagnostics on loopback listeners.
    // for example if a reverse proxy is the network boundary we expect it handle that
    if options.bind_address.ip().is_loopback() {
        DiagnosticsAccess::Allowed
    } else {
        DiagnosticsAccess::Disabled
    }
}

fn tokens_match(actual: &str, expected: &str) -> bool {
    let mut diff = actual.len() ^ expected.len();
    for (actual, expected) in actual.bytes().zip(expected.bytes()) {
        diff |= usize::from(actual ^ expected);
    }
    diff == 0
}

fn http_room_stats(snapshot: RuntimeRoomStatsSnapshot) -> RoomStatsResponse {
    RoomStatsResponse {
        create_date: snapshot.create_date,
        uuid: snapshot.uuid,
        remote_address: snapshot.remote_address,
        users_stats: UsersStatsResponse {
            incoming_bit_rate: IncomingBitRateStatsResponse {
                total: snapshot.users_stats.incoming_bitrate.total,
                audio: snapshot.users_stats.incoming_bitrate.audio,
                camera: snapshot.users_stats.incoming_bitrate.camera,
                screen: snapshot.users_stats.incoming_bitrate.screen,
            },
            count: snapshot.users_stats.count,
            camera_count: snapshot.users_stats.camera_count,
            screen_count: snapshot.users_stats.screen_count,
        },
        web_rtc_enabled: snapshot.web_rtc_enabled,
    }
}
