use std::sync::Arc;

use axum::extract::ws::Message;
use o_sfu_protocol::{
    shared::SessionId,
    signaling::{ClientEnvelope, RequestId, ServerMessage, WebSocketCloseCode},
};
use tokio::runtime::Handle;
use tracing::warn;

use super::super::{
    controller::SessionProtocolOutcome, flow_state::SessionFlowState,
    frame_codec::send_server_messages, track_projection::RemoteTrackProjection,
};
use crate::runtime::{
    ConnectionId,
    channel::{Channel, ChannelEventMessage, ChannelEventRequest, TrackBindingUpdate},
    metrics::RuntimeMetrics,
    rtc_adapter::TransportSessionHealth,
    transport_adapter::{ObservabilityPort, RuntimeTransportAdapter},
    websocket_server::{
        ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES, WsWriter, decode_client_batch,
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

/// The main orchestrator for an authenticated session.
///
/// It centralizes envelope dispatch, renegotiation sequencing, and staged publish
/// transitions behind one session-scoped owner. It bridges the gap between the
/// authenticated protocol surface and the `channel` runtime.
#[derive(Debug)]
pub(in crate::runtime::websocket_server) struct PostAuthSessionProtocol {
    pub(super) session_id: SessionId,
    pub(super) connection_id: ConnectionId,
    pub(super) remote_address: Arc<str>,
    pub(super) channel: Arc<Channel>,
    pub(super) transport_adapter: RuntimeTransportAdapter,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) request_ids: ServerRequestIdState,
    pub(super) flow_state: SessionFlowState,
    pub(super) track_projection: RemoteTrackProjection,
}

impl PostAuthSessionProtocol {
    pub(in crate::runtime::websocket_server) fn new(
        session_id: SessionId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        channel: Arc<Channel>,
        transport_adapter: RuntimeTransportAdapter,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            remote_address,
            channel,
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
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        self.transport_adapter
            .session_transport_health(&session_key)
            .and_then(|health| match health {
                TransportSessionHealth::Disconnected => Some(WebSocketCloseCode::Error),
                TransportSessionHealth::Connected => None,
            })
    }

    pub(in crate::runtime::websocket_server) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        match message {
            Message::Text(payload) => self.handle_text_payload(writer, &payload).await,
            Message::Binary(payload) => self.handle_binary_payload(writer, &payload).await,
            Message::Close(frame) => {
                tracing::info!(
                    remote_address = self.remote_address.as_ref(),
                    ?frame,
                    "websocket peer sent close frame"
                );
                SessionProtocolOutcome::Break
            }
            Message::Ping(_) | Message::Pong(_) => SessionProtocolOutcome::Continue,
        }
    }

    async fn handle_binary_payload(
        &mut self,
        writer: &mut WsWriter,
        payload: &[u8],
    ) -> SessionProtocolOutcome {
        if payload.len() > MAX_CLIENT_FRAME_BYTES {
            self.metrics.record_ws_bus_invalid_input_failure();
            warn!(
                session_id = ?self.session_id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                payload_len = payload.len(),
                max_len = MAX_CLIENT_FRAME_BYTES,
                "received oversized websocket binary frame"
            );
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        match String::from_utf8(payload.to_vec()) {
            Ok(payload) => self.handle_text_payload(writer, &payload).await,
            Err(_error) => {
                self.metrics.record_ws_bus_invalid_input_failure();
                warn!(
                    session_id = ?self.session_id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    "received websocket binary frame with invalid UTF-8"
                );
                SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }

    async fn handle_text_payload(
        &mut self,
        writer: &mut WsWriter,
        payload: &str,
    ) -> SessionProtocolOutcome {
        let batch = match decode_client_batch(payload) {
            Ok(batch) => batch,
            Err(error) => {
                match error.kind() {
                    ClientBatchDecodeFailureKind::InvalidInput => {
                        self.metrics.record_ws_bus_invalid_input_failure();
                        warn!(
                            session_id = ?self.session_id,
                            connection_id = ?self.connection_id,
                            remote_address = self.remote_address.as_ref(),
                            "failed to decode client websocket batch because the payload was invalid"
                        );
                    }
                    ClientBatchDecodeFailureKind::UnsupportedFeature => {
                        self.metrics.record_ws_bus_unsupported_feature_failure();
                        warn!(
                            session_id = ?self.session_id,
                            connection_id = ?self.connection_id,
                            remote_address = self.remote_address.as_ref(),
                            "failed to decode client websocket batch because it used an unsupported feature"
                        );
                    }
                }
                return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
            }
        };
        self.metrics.record_ws_bus_batch_received(batch.len());
        for envelope in batch {
            match &envelope {
                ClientEnvelope::Request { .. } => self.metrics.record_ws_bus_client_request(),
                ClientEnvelope::Message(_) => self.metrics.record_ws_bus_client_message(),
                ClientEnvelope::Response { .. } => {}
            }
            let outcome = self.dispatch_client_envelope(writer, envelope).await;
            if !matches!(outcome, SessionProtocolOutcome::Continue) {
                return outcome;
            }
        }
        SessionProtocolOutcome::Continue
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound_message(
        &mut self,
        writer: &mut WsWriter,
        message: ChannelEventMessage,
    ) -> Result<usize, WebSocketCloseCode> {
        let translated = self.track_projection.translate_server_message(message);
        let mut batch_len = send_server_messages(writer, translated.messages).await?;
        if translated.needs_renegotiation && self.request_renegotiation(writer).await? {
            batch_len += 1;
        }
        Ok(batch_len)
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound_request(
        &mut self,
        writer: &mut WsWriter,
        request: ChannelEventRequest,
    ) -> Result<usize, WebSocketCloseCode> {
        match request {
            ChannelEventRequest::BootstrapRemoteTrack(payload) => {
                self.track_projection.apply_remote_track_bootstrap(&payload);
                let mut batch_len = send_server_messages(
                    writer,
                    vec![ServerMessage::Tracks(self.track_projection.snapshot())],
                )
                .await?;
                if self.request_renegotiation(writer).await? {
                    batch_len += 1;
                }
                Ok(batch_len)
            }
        }
    }

    pub(in crate::runtime::websocket_server) async fn send_track_binding_update(
        &mut self,
        writer: &mut WsWriter,
        update: TrackBindingUpdate,
    ) -> Result<usize, WebSocketCloseCode> {
        let translated = self
            .track_projection
            .translate_track_binding_update(&update);
        let mut batch_len = send_server_messages(writer, translated.messages).await?;
        if translated.needs_renegotiation && self.request_renegotiation(writer).await? {
            batch_len += 1;
        }
        Ok(batch_len)
    }
}

impl Drop for PostAuthSessionProtocol {
    fn drop(&mut self) {
        let channel = Arc::clone(&self.channel);
        let transport_adapter = self.transport_adapter.clone();
        let session_id = self.session_id.clone();
        let connection_id = self.connection_id;
        if let Ok(runtime_handle) = Handle::try_current() {
            runtime_handle.spawn(async move {
                channel
                    .rollback_staged_publishes_for_connection(
                        &session_id,
                        connection_id,
                        &transport_adapter,
                    )
                    .await;
            });
        }
    }
}
