use std::sync::Arc;

use axum::extract::ws::Message;
use o_sfu_protocol::{
    shared::UserId,
    signaling::{ClientEnvelope, RequestId, ServerMessage, WebSocketCloseCode},
};
use tokio::runtime::Handle;
use tracing::warn;

use super::super::{flow_state::SessionFlowState, track_projection::RemoteTrackProjection};
use crate::{
    application::outcomes::{CallOutcome, UserSignal},
    runtime::{
        ConnectionId,
        metrics::RuntimeMetrics,
        room::{Room, RoomEventMessage, RoomEventRequest, TrackBindingUpdate},
        rtc_adapter::TransportSessionHealth,
        transport_adapter::{ObservabilityPort, RuntimeTransportAdapter},
        websocket_server::{
            ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES, decode_client_batch,
        },
    },
};

#[derive(Debug, Default)]
pub(super) struct ServerRequestIdState {
    next_request_counter: u64,
}

impl ServerRequestIdState {
    pub(super) fn next_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::websocket_server) enum PostAuthProtocolOutcome {
    Continue(CallOutcome),
    Break,
    Close(WebSocketCloseCode),
}

/// The main orchestrator for an authenticated user.
///
/// It centralizes envelope dispatch, renegotiation sequencing, and staged publish
/// transitions behind one user-scoped owner. It bridges the gap between the
/// authenticated protocol surface and the `room` runtime.
#[derive(Debug)]
pub(in crate::runtime::websocket_server) struct PostAuthSessionProtocol {
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) remote_address: Arc<str>,
    pub(super) room: Arc<Room>,
    pub(super) transport_adapter: RuntimeTransportAdapter,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) request_ids: ServerRequestIdState,
    pub(super) flow_state: SessionFlowState,
    pub(super) track_projection: RemoteTrackProjection,
}

impl PostAuthSessionProtocol {
    pub(in crate::runtime::websocket_server) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: Arc<Room>,
        transport_adapter: RuntimeTransportAdapter,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            user_id,
            connection_id,
            remote_address,
            room,
            transport_adapter,
            metrics,
            request_ids: ServerRequestIdState::default(),
            flow_state: SessionFlowState::default(),
            track_projection: RemoteTrackProjection::default(),
        }
    }

    pub(in crate::runtime::websocket_server) fn transport_close_code(
        &self,
    ) -> Option<WebSocketCloseCode> {
        let session_key = self
            .room
            .transport_user_key(&self.user_id, self.connection_id);
        self.transport_adapter
            .session_transport_health(&session_key)
            .and_then(|health| match health {
                TransportSessionHealth::Disconnected => Some(WebSocketCloseCode::Error),
                TransportSessionHealth::Connected => None,
            })
    }

    pub(in crate::runtime::websocket_server) async fn handle_frame(
        &mut self,
        message: Message,
    ) -> PostAuthProtocolOutcome {
        match message {
            Message::Text(payload) => self.handle_text_payload(&payload).await,
            Message::Binary(payload) => self.handle_binary_payload(&payload).await,
            Message::Close(frame) => {
                tracing::info!(
                    remote_address = self.remote_address.as_ref(),
                    ?frame,
                    "websocket peer sent close frame"
                );
                PostAuthProtocolOutcome::Break
            }
            Message::Ping(_) | Message::Pong(_) => {
                PostAuthProtocolOutcome::Continue(CallOutcome::new())
            }
        }
    }

    async fn handle_binary_payload(&mut self, payload: &[u8]) -> PostAuthProtocolOutcome {
        if payload.len() > MAX_CLIENT_FRAME_BYTES {
            self.metrics.record_ws_bus_invalid_input_failure();
            warn!(
                user_id = ?self.user_id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                payload_len = payload.len(),
                max_len = MAX_CLIENT_FRAME_BYTES,
                "received oversized websocket binary frame"
            );
            return PostAuthProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        match String::from_utf8(payload.to_vec()) {
            Ok(payload) => self.handle_text_payload(&payload).await,
            Err(_error) => {
                self.metrics.record_ws_bus_invalid_input_failure();
                warn!(
                    user_id = ?self.user_id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    "received websocket binary frame with invalid UTF-8"
                );
                PostAuthProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }

    async fn handle_text_payload(&mut self, payload: &str) -> PostAuthProtocolOutcome {
        let batch = match decode_client_batch(payload) {
            Ok(batch) => batch,
            Err(error) => {
                match error.kind() {
                    ClientBatchDecodeFailureKind::InvalidInput => {
                        self.metrics.record_ws_bus_invalid_input_failure();
                        warn!(
                            user_id = ?self.user_id,
                            connection_id = ?self.connection_id,
                            remote_address = self.remote_address.as_ref(),
                            "failed to decode client websocket batch because the payload was invalid"
                        );
                    }
                    ClientBatchDecodeFailureKind::UnsupportedFeature => {
                        self.metrics.record_ws_bus_unsupported_feature_failure();
                        warn!(
                            user_id = ?self.user_id,
                            connection_id = ?self.connection_id,
                            remote_address = self.remote_address.as_ref(),
                            "failed to decode client websocket batch because it used an unsupported feature"
                        );
                    }
                }
                return PostAuthProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
            }
        };
        self.metrics.record_ws_bus_batch_received(batch.len());
        let mut call_outcome = CallOutcome::new();
        for envelope in batch {
            match &envelope {
                ClientEnvelope::Request { .. } => self.metrics.record_ws_bus_client_request(),
                ClientEnvelope::Message(_) => self.metrics.record_ws_bus_client_message(),
                ClientEnvelope::Response { .. } => {}
            }
            match self.dispatch_client_envelope(envelope).await {
                Ok(outcome) => call_outcome.extend(outcome),
                Err(code) => return PostAuthProtocolOutcome::Close(code),
            }
        }
        PostAuthProtocolOutcome::Continue(call_outcome)
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound_message(
        &mut self,
        message: RoomEventMessage,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        let translated = self.track_projection.translate_server_message(message);
        let mut call_outcome =
            CallOutcome::new().with_signals(translated.messages.into_iter().map(UserSignal::from));
        if translated.needs_renegotiation {
            call_outcome.extend(self.request_renegotiation().await?);
        }
        Ok(call_outcome)
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound_request(
        &mut self,
        request: RoomEventRequest,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        match request {
            RoomEventRequest::BootstrapRemoteTrack(payload) => {
                self.track_projection.apply_remote_track_bootstrap(&payload);
                let mut call_outcome = CallOutcome::new()
                    .with_signal(ServerMessage::Tracks(self.track_projection.snapshot()).into());
                call_outcome.extend(self.request_renegotiation().await?);
                Ok(call_outcome)
            }
        }
    }

    pub(in crate::runtime::websocket_server) async fn send_track_binding_update(
        &mut self,
        update: TrackBindingUpdate,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        let translated = self
            .track_projection
            .translate_track_binding_update(&update);
        let mut call_outcome =
            CallOutcome::new().with_signals(translated.messages.into_iter().map(UserSignal::from));
        if translated.needs_renegotiation {
            call_outcome.extend(self.request_renegotiation().await?);
        }
        Ok(call_outcome)
    }
}

impl Drop for PostAuthSessionProtocol {
    fn drop(&mut self) {
        let room = Arc::clone(&self.room);
        let transport_adapter = self.transport_adapter.clone();
        let user_id = self.user_id.clone();
        let connection_id = self.connection_id;
        if let Ok(runtime_handle) = Handle::try_current() {
            runtime_handle.spawn(async move {
                room.rollback_staged_publishes_for_connection(
                    &user_id,
                    connection_id,
                    &transport_adapter,
                )
                .await;
            });
        }
    }
}
