//! websocket controller for one upgraded socket
//!
//! this module accepts the HTTP upgrade, delegates first-frame auth to
//! [`super::handshake::establish_user`], runs [`super::session_loop::run`] then
//! performs the single room cleanup path for the admitted user

use std::sync::Arc;

use axum::{
    extract::{
        FromRef, State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream::SplitStream};
use o_sfu_protocol::wire::UserId;
use tracing::{Instrument, Span, field, info, warn};

use super::{
    admission::{PreAuthWebSocketAdmissionRejection, PreAuthWebSocketPermit},
    io::MAX_CLIENT_FRAME_BYTES,
};
use crate::{
    application::user_session::User,
    config::{AuthConfig, UserConfig},
    core::{
        prelude::SfuCore,
        server::room::{Room, UserOutboundReceiver},
    },
    runtime::{
        ConnectionId, MediaTransport, RuntimeMetrics, RuntimeState,
        request_origin::ResolvedRequestOrigin,
        room::RoomManager,
        telemetry::{
            self,
            schema::{event as telemetry_event, field as telemetry_field},
        },
    },
};

pub(super) type WsReader = SplitStream<WebSocket>;

#[derive(Debug, Clone)]
pub(crate) struct WebSocketServices {
    pub(super) auth: AuthConfig,
    pub(super) user: UserConfig,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
    pub(super) sfu_core: SfuCore,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pre_auth_websocket_admission: super::PreAuthWebSocketAdmission,
}

pub(super) struct ConnectedUser {
    pub(super) room: Arc<Room>,
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) remote_address: Arc<str>,
    pub(super) outbound_rx: UserOutboundReceiver,
    pub(super) user: User,
}

impl FromRef<RuntimeState> for WebSocketServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            auth: state.config.auth.clone(),
            user: state.config.user,
            room_manager: Arc::clone(&state.room_manager),
            media_transport: state.media_transport.clone(),
            sfu_core: state.sfu_core.clone(),
            metrics: Arc::clone(&state.metrics),
            pre_auth_websocket_admission: state.pre_auth_websocket_admission.clone(),
        }
    }
}

pub(crate) async fn upgrade(
    State(services): State<WebSocketServices>,
    ResolvedRequestOrigin(origin): ResolvedRequestOrigin,
    websocket: WebSocketUpgrade,
) -> Response {
    let remote_address = Arc::<str>::from(origin.remote_address);
    let pre_auth_permit = match services
        .pre_auth_websocket_admission
        .try_acquire(remote_address.as_ref())
    {
        Ok(permit) => permit,
        Err(rejection) => {
            reject_pre_auth_admission(&services, remote_address.as_ref(), rejection);
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    websocket
        .max_message_size(MAX_CLIENT_FRAME_BYTES)
        .max_frame_size(MAX_CLIENT_FRAME_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, services, remote_address, pre_auth_permit))
}

fn reject_pre_auth_admission(
    services: &WebSocketServices,
    remote_address: &str,
    rejection: PreAuthWebSocketAdmissionRejection,
) {
    match rejection {
        PreAuthWebSocketAdmissionRejection::Global => {
            warn!(
                event = telemetry_event::WS_HANDSHAKE_REJECTED,
                remote_address,
                max_pre_auth_websocket_sessions = services.auth.max_pre_auth_websocket_sessions,
                "rejecting websocket upgrade because global pre-auth admission is full"
            );
        }
        PreAuthWebSocketAdmissionRejection::Origin => {
            warn!(
                event = telemetry_event::WS_HANDSHAKE_REJECTED,
                remote_address,
                max_pre_auth_websocket_sessions_per_origin =
                    services.auth.max_pre_auth_websocket_sessions_per_origin,
                "rejecting websocket upgrade because origin pre-auth admission is full"
            );
        }
    }
}

/// Drives one upgraded WebSocket from first split through final room cleanup.
///
/// After the HTTP upgrade completes, this function records connection metrics,
/// delegates authenticated admission to [`super::handshake::establish_user`],
/// runs the steady-state user loop then closes the logical room user exactly
/// once. Keeping that sequencing here prevents handshake code and steady-state
/// protocol code from racing to clean up the same room user.
async fn handle_socket(
    socket: WebSocket,
    services: WebSocketServices,
    remote_address: Arc<str>,
    pre_auth_permit: PreAuthWebSocketPermit,
) {
    async move {
        let current_span = Span::current();
        current_span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(remote_address.as_ref()),
        );
        let (mut ws_writer, mut ws_reader) = socket.split();
        services.metrics.record_ws_connection_accepted();
        info!(
            event = telemetry_event::WS_CONNECTION_ACCEPTED,
            remote_address = remote_address.as_ref(),
            "accepted websocket connection"
        );
        let handshake_span = telemetry::ws_handshake_span();
        handshake_span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(remote_address.as_ref()),
        );
        let Some(mut user_session) = super::handshake::establish_user(
            &services,
            &mut ws_writer,
            &mut ws_reader,
            Arc::clone(&remote_address),
            pre_auth_permit,
        )
        .instrument(handshake_span)
        .await
        else {
            return;
        };
        current_span.record("room_id", field::display(user_session.room.uuid()));
        current_span.record("user_id", field::debug(&user_session.user_id));
        current_span.record("connection_id", field::debug(user_session.connection_id));
        services.metrics.record_ws_user_loop_started();
        let exit_reason = super::session_loop::run(super::session_loop::UserLoop {
            socket: super::session_loop::UserSocket {
                writer: &mut ws_writer,
                reader: &mut ws_reader,
                outbound_rx: &mut user_session.outbound_rx,
            },
            session: super::session_loop::VerifiedUserSession {
                room_manager: services.room_manager.as_ref(),
                room: user_session.room.as_ref(),
                user_id: &user_session.user_id,
                connection_id: user_session.connection_id,
                user: &mut user_session.user,
                media_transport: &services.media_transport,
            },
            config: super::session_loop::UserLoopConfig {
                user_timeout_ms: services.user.timeout_ms,
                ping_interval_ms: services.user.ping_interval_ms,
            },
            metrics: &services.metrics,
        })
        .await;
        services.metrics.record_ws_user_loop_exit(exit_reason);
        info!(
            event = telemetry_event::WS_CONNECTION_CLOSED,
            connection_id = ?user_session.connection_id,
            remote_address = user_session.remote_address.as_ref(),
            ?exit_reason,
            "closing websocket user"
        );
    }
    .instrument(telemetry::ws_upgrade_span())
    .await;
}
