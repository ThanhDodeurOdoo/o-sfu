//! websocket controller for one upgraded socket
//!
//! this module accepts the HTTP upgrade, delegates first-frame auth to
//! [`super::session_loop::ActiveWebSocketSession`] then leaves authenticated
//! socket lifecycle and room cleanup to that session owner

use std::sync::Arc;

use axum::{
    extract::{
        FromRef, State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::{Instrument, Span, field, warn};

use super::{
    admission::{PreAuthWebSocketAdmissionRejection, PreAuthWebSocketPermit},
    io::MAX_CLIENT_FRAME_BYTES,
    session_loop::ActiveWebSocketSession,
};
use crate::{
    config::{AuthConfig, UserConfig},
    core::prelude::SfuCore,
    runtime::{
        MediaTransport, RuntimeMetrics, RuntimeState,
        request_origin::RequestOrigin,
        room::RoomManager,
        telemetry::{
            self,
            schema::{event as telemetry_event, field as telemetry_field},
        },
    },
};

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
    origin: RequestOrigin,
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
        services.metrics.record_ws_connection_accepted();
        let handshake_span = telemetry::ws_handshake_span();
        handshake_span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(remote_address.as_ref()),
        );
        let Some(session) =
            ActiveWebSocketSession::accept(socket, services, remote_address, pre_auth_permit)
                .instrument(handshake_span)
                .await
        else {
            return;
        };
        session.record_upgrade_span();
        session.serve().await;
    }
    .instrument(telemetry::ws_upgrade_span())
    .await;
}
