use std::sync::Arc;

use axum::extract::ws::Message;
use o_sfu_protocol::{shared::SessionId, signaling::WebSocketCloseCode};

use crate::runtime::{
    channel::{Channel, SessionOutbound},
    metrics::RuntimeMetrics,
    transport_adapter::RuntimeTransportAdapter,
};

use super::super::WsWriter;
use super::post_auth::PostAuthSessionProtocol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::websocket_server) enum SessionProtocolOutcome {
    Continue,
    Break,
    Close(WebSocketCloseCode),
}

#[derive(Debug)]
pub(in crate::runtime::websocket_server) struct SessionProtocol(PostAuthSessionProtocol);

impl SessionProtocol {
    pub(in crate::runtime::websocket_server) fn new(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        transport_adapter: RuntimeTransportAdapter,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self(PostAuthSessionProtocol::new(
            session_id,
            connection_id,
            channel,
            transport_adapter,
            metrics,
        ))
    }

    pub(in crate::runtime::websocket_server) async fn initialize(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        self.0.send_initial_offer(writer).await.map_err(|_error| ())
    }

    pub(in crate::runtime::websocket_server) fn awaiting_ping_response(&self) -> bool {
        self.0.awaiting_ping_response()
    }

    pub(in crate::runtime::websocket_server) fn transport_close_code(
        &self,
    ) -> Option<WebSocketCloseCode> {
        self.0.transport_close_code()
    }

    pub(in crate::runtime::websocket_server) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        self.0.send_ping(writer).await
    }

    pub(in crate::runtime::websocket_server) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        self.0.handle_frame(writer, message).await
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound(
        &mut self,
        writer: &mut WsWriter,
        outbound: SessionOutbound,
    ) -> Result<usize, WebSocketCloseCode> {
        match outbound {
            SessionOutbound::Close(code) => Err(code),
            SessionOutbound::Message(message) => {
                self.0.send_outbound_message(writer, message).await
            }
            SessionOutbound::Request(request) => {
                self.0.send_outbound_request(writer, *request).await
            }
            SessionOutbound::TrackBindingUpdate(update) => {
                self.0.send_track_binding_update(writer, update).await
            }
        }
    }
}
