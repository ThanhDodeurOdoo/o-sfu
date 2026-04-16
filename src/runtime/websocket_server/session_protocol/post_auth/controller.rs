use std::sync::Arc;

use axum::extract::ws::Message;

use crate::runtime::{
    channel::{Channel, ChannelEventMessage, ChannelEventRequest, TrackBindingUpdate},
    metrics::RuntimeMetrics,
    rtc_adapter::TransportSessionHealth,
    transport_adapter::RuntimeTransportAdapter,
    websocket_server::WsWriter,
};
use crate::signaling::{
    client_batch::{ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES, decode_client_batch},
    protocol::{ClientEnvelope, ServerMessage, ServerRequest, WebSocketCloseCode},
    shared::SessionId,
};

use super::super::{
    controller::SessionProtocolOutcome,
    frame_codec::{send_server_messages, send_server_request},
    negotiation::NegotiationState,
    request_state::SessionRequestState,
    track_projection::RemoteTrackProjection,
};
use super::state::PostAuthSessionState;

#[derive(Debug)]
pub(in crate::runtime::websocket_server) struct PostAuthSessionProtocol {
    pub(super) session_id: SessionId,
    pub(super) connection_id: u64,
    pub(super) channel: Arc<Channel>,
    pub(super) transport_adapter: RuntimeTransportAdapter,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) request_state: SessionRequestState,
    pub(super) negotiation: NegotiationState,
    pub(super) track_projection: RemoteTrackProjection,
    pub(super) state: PostAuthSessionState,
}

impl PostAuthSessionProtocol {
    pub(in crate::runtime::websocket_server) fn new(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        transport_adapter: RuntimeTransportAdapter,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            channel,
            transport_adapter,
            metrics,
            request_state: SessionRequestState::default(),
            negotiation: NegotiationState::default(),
            track_projection: RemoteTrackProjection::default(),
            state: PostAuthSessionState::default(),
        }
    }

    pub(in crate::runtime::websocket_server) fn awaiting_ping_response(&self) -> bool {
        self.request_state.awaiting_ping_response()
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

    pub(in crate::runtime::websocket_server) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        let Some(request_id) = self.request_state.start_ping() else {
            return Ok(());
        };
        send_server_request(writer, request_id, ServerRequest::Ping).await
    }

    pub(in crate::runtime::websocket_server) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        match message {
            Message::Text(payload) => self.handle_text_payload(writer, &payload).await,
            Message::Binary(payload) => {
                if payload.len() > MAX_CLIENT_FRAME_BYTES {
                    self.metrics.record_ws_bus_invalid_input_failure();
                    return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
                }
                match String::from_utf8(payload.to_vec()) {
                    Ok(payload) => self.handle_text_payload(writer, &payload).await,
                    Err(_error) => {
                        self.metrics.record_ws_bus_invalid_input_failure();
                        SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
                    }
                }
            }
            Message::Close(_) => SessionProtocolOutcome::Break,
            Message::Ping(_) | Message::Pong(_) => SessionProtocolOutcome::Continue,
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
                    }
                    ClientBatchDecodeFailureKind::UnsupportedFeature => {
                        self.metrics.record_ws_bus_unsupported_feature_failure();
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
