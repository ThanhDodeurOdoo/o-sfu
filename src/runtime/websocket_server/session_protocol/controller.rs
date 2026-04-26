use std::sync::Arc;

use axum::extract::ws::Message;
use o_sfu_protocol::{
    shared::UserId,
    signaling::{ClientEnvelope, WebSocketCloseCode},
};
use tracing::warn;

use super::{super::WsWriter, frame_codec::send_call_outcome};
use crate::{
    application::{
        outcomes::{CallOutcome, UserEndReason, UserError},
        rooms::{RoomHandle, UserCloseReasonEvent, UserOutboundEvent},
        users::{RoomEvent, User, UserIntent},
    },
    core::SfuCore,
    runtime::{
        ConnectionId,
        metrics::RuntimeMetrics,
        websocket_server::{
            ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES, decode_client_batch,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::websocket_server) enum SessionProtocolOutcome {
    Continue,
    Break,
    Close(WebSocketCloseCode),
}

/// Stateful manager for a single authenticated signaling user.
///
/// It acts as the facade between the raw WebSocket stream (handled by the user loop)
/// and the business logic below.
#[derive(Debug)]
pub(in crate::runtime::websocket_server) struct SessionProtocol {
    user: User,
    metrics: Arc<RuntimeMetrics>,
}

impl SessionProtocol {
    pub(in crate::runtime::websocket_server) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: RoomHandle,
        media_core: SfuCore,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            user: User::new(user_id, connection_id, remote_address, room, media_core),
            metrics,
        }
    }

    pub(in crate::runtime::websocket_server) async fn initialize(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        let outcome = self.user.bootstrap().await.map_err(|_error| ())?;
        send_call_outcome(writer, outcome)
            .await
            .map(|_sent| ())
            .map_err(|_error| ())
    }

    pub(in crate::runtime::websocket_server) fn transport_close_code(
        &self,
    ) -> Option<WebSocketCloseCode> {
        match self.user.end_reason() {
            Some(UserEndReason::TransportDisconnected) => Some(WebSocketCloseCode::Error),
            Some(
                UserEndReason::Completed
                | UserEndReason::Replaced
                | UserEndReason::RemovedByRuntime
                | UserEndReason::ProtocolViolation
                | UserEndReason::InternalError,
            )
            | None => None,
        }
    }

    pub(in crate::runtime::websocket_server) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        match self.handle_socket_message(message).await {
            SocketMessageOutcome::Continue(call_outcome) => {
                Self::render_call_outcome(writer, call_outcome).await
            }
            SocketMessageOutcome::Break => SessionProtocolOutcome::Break,
            SocketMessageOutcome::Close(code) => SessionProtocolOutcome::Close(code),
        }
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound(
        &mut self,
        writer: &mut WsWriter,
        outbound: UserOutboundEvent,
    ) -> Result<usize, WebSocketCloseCode> {
        match outbound {
            UserOutboundEvent::Close(reason) => Err(map_session_close_reason(reason)),
            UserOutboundEvent::Message(message) => {
                let outcome = self
                    .user
                    .handle_room_event(RoomEvent::Message(message))
                    .await
                    .map_err(map_user_error)?;
                send_call_outcome(writer, outcome).await
            }
            UserOutboundEvent::Request(request) => {
                let outcome = self
                    .user
                    .handle_room_event(RoomEvent::Request(*request))
                    .await
                    .map_err(map_user_error)?;
                send_call_outcome(writer, outcome).await
            }
            UserOutboundEvent::TrackBindingUpdate(update) => {
                let outcome = self
                    .user
                    .handle_room_event(RoomEvent::TrackBindingUpdate(update))
                    .await
                    .map_err(map_user_error)?;
                send_call_outcome(writer, outcome).await
            }
        }
    }

    pub(in crate::runtime::websocket_server) async fn finish(&mut self) {
        self.user.finish().await;
    }

    async fn handle_socket_message(&mut self, message: Message) -> SocketMessageOutcome {
        match message {
            Message::Text(payload) => self.handle_text_payload(&payload).await,
            Message::Binary(payload) => self.handle_binary_payload(&payload).await,
            Message::Close(frame) => {
                tracing::info!(?frame, "websocket peer sent close frame");
                SocketMessageOutcome::Break
            }
            Message::Ping(_) | Message::Pong(_) => {
                SocketMessageOutcome::Continue(CallOutcome::new())
            }
        }
    }

    async fn handle_binary_payload(&mut self, payload: &[u8]) -> SocketMessageOutcome {
        if payload.len() > MAX_CLIENT_FRAME_BYTES {
            self.metrics.record_ws_bus_invalid_input_failure();
            warn!(
                payload_len = payload.len(),
                max_len = MAX_CLIENT_FRAME_BYTES,
                "received oversized websocket binary frame"
            );
            return SocketMessageOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        match String::from_utf8(payload.to_vec()) {
            Ok(payload) => self.handle_text_payload(&payload).await,
            Err(_error) => {
                self.metrics.record_ws_bus_invalid_input_failure();
                warn!("received websocket binary frame with invalid UTF-8");
                SocketMessageOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }

    async fn handle_text_payload(&mut self, payload: &str) -> SocketMessageOutcome {
        let batch = match decode_client_batch(payload) {
            Ok(batch) => batch,
            Err(error) => {
                match error.kind() {
                    ClientBatchDecodeFailureKind::InvalidInput => {
                        self.metrics.record_ws_bus_invalid_input_failure();
                        warn!(
                            "failed to decode client websocket batch because the payload was invalid"
                        );
                    }
                    ClientBatchDecodeFailureKind::UnsupportedFeature => {
                        self.metrics.record_ws_bus_unsupported_feature_failure();
                        warn!(
                            "failed to decode client websocket batch because it used an unsupported feature"
                        );
                    }
                }
                return SocketMessageOutcome::Close(WebSocketCloseCode::ProtocolError);
            }
        };
        self.metrics.record_ws_bus_batch_received(batch.len());
        let mut call_outcome = CallOutcome::new();
        for envelope in batch {
            self.record_client_envelope_metrics(&envelope);
            match self
                .user
                .handle_intent(UserIntent::ClientEnvelope(envelope))
                .await
            {
                Ok(outcome) => call_outcome.extend(outcome),
                Err(error) => return SocketMessageOutcome::Close(map_user_error(error)),
            }
        }
        SocketMessageOutcome::Continue(call_outcome)
    }

    fn record_client_envelope_metrics(&self, envelope: &ClientEnvelope) {
        match envelope {
            ClientEnvelope::Request { .. } => self.metrics.record_ws_bus_client_request(),
            ClientEnvelope::Message(_) => self.metrics.record_ws_bus_client_message(),
            ClientEnvelope::Response { .. } => {}
        }
    }

    async fn render_call_outcome(
        writer: &mut WsWriter,
        call_outcome: CallOutcome,
    ) -> SessionProtocolOutcome {
        match send_call_outcome(writer, call_outcome).await {
            Ok(_sent) => SessionProtocolOutcome::Continue,
            Err(code) => SessionProtocolOutcome::Close(code),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SocketMessageOutcome {
    Continue(CallOutcome),
    Break,
    Close(WebSocketCloseCode),
}

fn map_session_close_reason(reason: UserCloseReasonEvent) -> WebSocketCloseCode {
    match reason {
        UserCloseReasonEvent::Replaced | UserCloseReasonEvent::RemovedByRuntime => {
            WebSocketCloseCode::Kicked
        }
    }
}

fn map_user_error(error: UserError) -> WebSocketCloseCode {
    match error {
        UserError::ProtocolViolation => WebSocketCloseCode::ProtocolError,
        UserError::Kicked => WebSocketCloseCode::Kicked,
        UserError::InternalError => WebSocketCloseCode::Error,
    }
}
