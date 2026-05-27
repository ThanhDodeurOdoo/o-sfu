use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use o_sfu_protocol::wire::StreamType;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info};

use crate::{
    application::stream_catalog::counter_for_stream_type,
    runtime::{
        RuntimeState, diagnostics,
        diagnostics::DiagnosticsUserLookup,
        http_server::{
            contract::{
                CHANNEL_PATH, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
                DIAGNOSTICS_WORKERS_PATH, DISCONNECT_PATH, IncomingBitRateStatsResponse,
                METRICS_PATH, NOOP_PATH, NoopResponse, RoomResponse, RoomStatsResponse, STATS_PATH,
                UsersStatsResponse,
            },
            extractors::{
                DiagnosticsAccess, DiagnosticsServices, RoomServices, VerifiedDisconnectClaims,
                VerifiedRoomRequest,
            },
        },
        metrics::{HttpRoute, RuntimeMetrics},
        prometheus::{PROMETHEUS_CONTENT_TYPE, render_prometheus},
        room::RuntimeRoomStatsSnapshot,
        telemetry::{self, schema::event as telemetry_event},
        websocket_server,
    },
};

const MAX_DISCONNECT_BODY_BYTES: usize = 16 * 1024;

pub(crate) async fn serve_http(
    state: RuntimeState,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(state.config.http.bind_address).await?;
    serve_http_on(listener, state, shutdown_token).await
}

pub(crate) async fn serve_http_on(
    listener: TcpListener,
    state: RuntimeState,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let local_address = listener.local_addr()?;
    info!(
        event = telemetry_event::HTTP_LISTENER_READY,
        bind_address = %state.config.http.bind_address,
        local_address = %local_address,
        trust_proxy_headers = state.config.http.trust_proxy_headers,
        "booted HTTP and WebSocket listener"
    );
    axum::serve(
        listener,
        app(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_token.cancelled_owned())
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
        .merge(diagnostics_router(state.clone()))
        .route(
            DISCONNECT_PATH,
            post(disconnect).layer(DefaultBodyLimit::max(MAX_DISCONNECT_BODY_BYTES)),
        )
        .with_state(state)
}

fn diagnostics_router(state: RuntimeState) -> Router<RuntimeState> {
    Router::new()
        .route(DIAGNOSTICS_SUMMARY_PATH, get(diagnostics_summary))
        .route(DIAGNOSTICS_ROOMS_PATH, get(diagnostics_rooms))
        .route(DIAGNOSTICS_WORKERS_PATH, get(diagnostics_workers))
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
        .route_layer(middleware::from_fn_with_state(
            state,
            require_diagnostics_access,
        ))
}

#[o_sfu_telemetry::measure_http_request(
    metrics = "metrics",
    request = "record_http_noop_request",
    route = "HttpRoute::Noop"
)]
async fn noop(State(metrics): State<Arc<RuntimeMetrics>>) -> impl IntoResponse {
    async { axum::Json(NoopResponse::ok()) }
        .instrument(telemetry::http_request_span("noop"))
        .await
}

#[o_sfu_telemetry::measure_http_request(
    metrics = "services.metrics",
    request = "record_http_stats_request",
    route = "HttpRoute::Stats"
)]
async fn stats(State(services): State<RoomServices>) -> impl IntoResponse {
    async {
        axum::Json(
            services
                .room_manager
                .stats_snapshots(&services.media_transport)
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
    metrics = "metrics",
    request = "record_http_metrics_request",
    route = "HttpRoute::Metrics"
)]
async fn metrics(State(metrics): State<Arc<RuntimeMetrics>>) -> impl IntoResponse {
    async {
        (
            [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
            render_prometheus(&metrics),
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
/// Query parameters specify whether the room should have WebRTC enabled and
/// optional webhook endpoints for recordings.
#[o_sfu_telemetry::measure_http_request(
    metrics = "services.metrics",
    request = "record_http_room_request",
    route = "HttpRoute::Room"
)]
async fn room(State(services): State<RoomServices>, request: VerifiedRoomRequest) -> Response {
    async {
        let room = services
            .room_manager
            .serve_room(
                &request.issuer,
                &request.room_key,
                &request.config,
                Some(request.origin.remote_address.as_str()),
            )
            .await;
        services.metrics.record_http_room_success();
        (
            StatusCode::OK,
            axum::Json(RoomResponse {
                uuid: room.uuid().to_owned(),
                url: request.origin.base_url,
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
    metrics = "services.metrics",
    request = "record_http_disconnect_request",
    route = "HttpRoute::Disconnect"
)]
async fn disconnect(
    State(services): State<RoomServices>,
    VerifiedDisconnectClaims(claims): VerifiedDisconnectClaims,
) -> Response {
    async {
        for (room_id, user_ids) in &claims.user_ids_by_room {
            services
                .room_manager
                .disconnect_users(room_id, user_ids, &services.media_transport)
                .await;
        }
        services.metrics.record_http_disconnect_success();
        StatusCode::OK.into_response()
    }
    .instrument(telemetry::http_request_span("disconnect"))
    .await
}

async fn diagnostics_summary(State(services): State<DiagnosticsServices>) -> Response {
    async {
        axum::Json(
            diagnostics::summary_response(
                &services.room_manager,
                &services.media_transport,
                &services.diagnostics,
            )
            .await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_rooms(State(services): State<DiagnosticsServices>) -> Response {
    async {
        axum::Json(
            diagnostics::rooms_response(
                &services.room_manager,
                &services.media_transport,
                &services.diagnostics,
            )
            .await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_workers(State(services): State<DiagnosticsServices>) -> Response {
    async {
        axum::Json(
            diagnostics::workers_response(&services.room_manager, &services.media_transport).await,
        )
        .into_response()
    }
    .await
}

async fn diagnostics_room_detail(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    async {
        let payload = diagnostics::room_detail_response(
            &services.room_manager,
            &services.media_transport,
            &services.diagnostics,
            &room_id,
        )
        .await;
        diagnostics_optional_response(payload)
    }
    .await
}

async fn diagnostics_room_users(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    async {
        let payload = diagnostics::room_users_response(
            &services.room_manager,
            &services.media_transport,
            &services.diagnostics,
            &room_id,
        )
        .await;
        diagnostics_optional_response(payload)
    }
    .await
}

async fn diagnostics_room_graph(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    async {
        let payload = diagnostics::room_detail_response(
            &services.room_manager,
            &services.media_transport,
            &services.diagnostics,
            &room_id,
        )
        .await
        .map(|payload| diagnostics::build_graph(&payload));
        diagnostics_optional_response(payload)
    }
    .await
}

async fn diagnostics_user_graph(
    State(services): State<DiagnosticsServices>,
    Path((room_id, user_id)): Path<(String, String)>,
) -> Response {
    async {
        let payload = diagnostics::room_detail_response(
            &services.room_manager,
            &services.media_transport,
            &services.diagnostics,
            &room_id,
        )
        .await
        .and_then(|payload| diagnostics::build_user_graph(&payload, &user_id));
        diagnostics_optional_response(payload)
    }
    .await
}

fn diagnostics_optional_response<T>(payload: Option<T>) -> Response
where
    axum::Json<T>: IntoResponse,
{
    payload.map_or_else(
        || StatusCode::NOT_FOUND.into_response(),
        |payload| axum::Json(payload).into_response(),
    )
}

async fn diagnostics_user_detail(
    State(services): State<DiagnosticsServices>,
    Path(user_id): Path<String>,
) -> Response {
    async {
        match diagnostics::user_detail_response(
            &services.room_manager,
            &services.media_transport,
            &services.diagnostics,
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

async fn require_diagnostics_access(
    _access: DiagnosticsAccess,
    request: Request,
    next: Next,
) -> Response {
    next.run(request).await
}

fn http_room_stats(snapshot: RuntimeRoomStatsSnapshot) -> RoomStatsResponse {
    let incoming_bitrate = &snapshot.users_stats.incoming_bitrate;
    let active_stream_counts = &snapshot.users_stats.active_stream_counts;
    RoomStatsResponse {
        create_date: snapshot.create_date,
        uuid: snapshot.uuid,
        remote_address: snapshot.remote_address,
        users_stats: UsersStatsResponse {
            incoming_bit_rate: IncomingBitRateStatsResponse {
                total: incoming_bitrate.total,
                audio: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Audio),
                camera: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Camera),
                screen: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Screen),
            },
            count: snapshot.users_stats.count,
            camera_count: counter_for_stream_type(active_stream_counts, StreamType::Camera),
            screen_count: counter_for_stream_type(active_stream_counts, StreamType::Screen),
        },
        web_rtc_enabled: snapshot.web_rtc_enabled,
    }
}
