use std::sync::Arc;

use axum::extract::ws::Message;
use o_sfu_protocol::{shared::UserId, signaling::WebSocketCloseCode};

use super::{super::WsWriter, post_auth::PostAuthSessionProtocol};
use crate::runtime::{
    ConnectionId,
    metrics::RuntimeMetrics,
    room::{Room, UserCloseReason, UserOutbound},
    transport_adapter::RuntimeTransportAdapter,
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
pub(in crate::runtime::websocket_server) struct SessionProtocol(PostAuthSessionProtocol);

impl SessionProtocol {
    pub(in crate::runtime::websocket_server) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: Arc<Room>,
        transport_adapter: RuntimeTransportAdapter,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self(PostAuthSessionProtocol::new(
            user_id,
            connection_id,
            remote_address,
            room,
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

    pub(in crate::runtime::websocket_server) fn transport_close_code(
        &self,
    ) -> Option<WebSocketCloseCode> {
        self.0.transport_close_code()
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
        outbound: UserOutbound,
    ) -> Result<usize, WebSocketCloseCode> {
        match outbound {
            UserOutbound::Close(reason) => Err(map_session_close_reason(reason)),
            UserOutbound::Message(message) => self.0.send_outbound_message(writer, message).await,
            UserOutbound::Request(request) => self.0.send_outbound_request(writer, *request).await,
            UserOutbound::TrackBindingUpdate(update) => {
                self.0.send_track_binding_update(writer, update).await
            }
        }
    }
}

fn map_session_close_reason(reason: UserCloseReason) -> WebSocketCloseCode {
    match reason {
        UserCloseReason::Replaced | UserCloseReason::RemovedByRuntime => WebSocketCloseCode::Kicked,
    }
}
