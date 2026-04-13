use std::sync::Arc;

use axum::extract::ws::Message;

use crate::runtime::{
    channel::{Channel, SessionOutbound},
    metrics::RuntimeMetrics,
    stub_bus::{
        StubBusOutcome, StubBusSession, WsWriter, send_server_message_batch,
        send_server_request_batch,
    },
    transport_adapter::RuntimeTransportAdapter,
};
use crate::signaling::{protocol::WebSocketCloseCode, shared::SessionId};

use super::native::NativeSessionProtocol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::websocket_server) enum SessionProtocolOutcome {
    Continue,
    Break,
    Close(WebSocketCloseCode),
}

impl From<StubBusOutcome> for SessionProtocolOutcome {
    fn from(value: StubBusOutcome) -> Self {
        match value {
            StubBusOutcome::Continue => Self::Continue,
            StubBusOutcome::Break => Self::Break,
            StubBusOutcome::Close(code) => Self::Close(code),
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime::websocket_server) enum SessionProtocol {
    LegacyStubBus(StubBusSession),
    Native(NativeSessionProtocol),
}

impl SessionProtocol {
    pub(in crate::runtime::websocket_server) fn legacy_stub_bus(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        metrics: Arc<RuntimeMetrics>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self::LegacyStubBus(StubBusSession::new(
            session_id,
            connection_id,
            channel,
            metrics,
            transport_adapter,
        ))
    }

    pub(in crate::runtime::websocket_server) fn native(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self::Native(NativeSessionProtocol::new(
            session_id,
            connection_id,
            channel,
            transport_adapter,
        ))
    }

    pub(in crate::runtime::websocket_server) async fn initialize(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        match self {
            Self::LegacyStubBus(session) => session.send_transport_bootstrap(writer).await,
            Self::Native(session) => session
                .send_initial_offer(writer)
                .await
                .map_err(|_error| ()),
        }
    }

    pub(in crate::runtime::websocket_server) fn awaiting_ping_response(&self) -> bool {
        match self {
            Self::LegacyStubBus(session) => session.awaiting_ping_response(),
            Self::Native(session) => session.awaiting_ping_response(),
        }
    }

    pub(in crate::runtime::websocket_server) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        match self {
            Self::LegacyStubBus(session) => session.send_ping(writer).await,
            Self::Native(session) => session.send_ping(writer).await,
        }
    }

    pub(in crate::runtime::websocket_server) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        match self {
            Self::LegacyStubBus(session) => session.handle_frame(writer, message).await.into(),
            Self::Native(session) => session.handle_frame(writer, message).await,
        }
    }

    pub(in crate::runtime::websocket_server) async fn send_outbound(
        &mut self,
        writer: &mut WsWriter,
        outbound: SessionOutbound,
    ) -> Result<usize, WebSocketCloseCode> {
        match (self, outbound) {
            (Self::LegacyStubBus(_), SessionOutbound::Message(message)) => {
                send_server_message_batch(writer, &message).await?;
                Ok(1)
            }
            (Self::LegacyStubBus(_), SessionOutbound::Request(request)) => {
                send_server_request_batch(writer, &request).await?;
                Ok(1)
            }
            (Self::LegacyStubBus(_) | Self::Native(_), SessionOutbound::Close(code)) => Err(code),
            (Self::Native(session), SessionOutbound::Message(message)) => {
                session.send_outbound_message(writer, message).await
            }
            (Self::Native(session), SessionOutbound::Request(request)) => {
                session.send_outbound_request(writer, *request).await
            }
        }
    }
}
