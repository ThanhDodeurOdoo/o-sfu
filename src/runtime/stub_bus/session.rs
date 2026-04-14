use std::sync::Arc;

use axum::extract::ws::Message;
use serde_json::Value;
use tracing::trace;

use super::{codec, session_controller::SessionController, signaling_edge::decode_frame};
use crate::runtime::{
    channel::{Channel, SessionOutbound},
    metrics::RuntimeMetrics,
    transport_adapter::RuntimeTransportAdapter,
    websocket_server::WsWriter,
};
use crate::signaling::{protocol::WebSocketCloseCode, shared::SessionId};

pub(crate) const STUB_SERVER_BUS_ID: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StubBusOutcome {
    Continue,
    Break,
    Close(WebSocketCloseCode),
}

#[derive(Debug)]
pub(crate) struct StubBusSession {
    controller: SessionController,
}

impl StubBusSession {
    #[must_use]
    pub(crate) fn new(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        metrics: Arc<RuntimeMetrics>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self {
            controller: SessionController::new(
                session_id,
                connection_id,
                channel,
                metrics,
                transport_adapter,
            ),
        }
    }

    pub(crate) async fn send_transport_bootstrap(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        self.controller.send_transport_bootstrap(writer).await
    }

    pub(crate) fn awaiting_ping_response(&self) -> bool {
        self.controller.awaiting_ping_response()
    }

    pub(crate) fn transport_close_code(&self) -> Option<WebSocketCloseCode> {
        self.controller.transport_close_code()
    }

    pub(crate) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        self.controller.send_ping(writer).await
    }

    pub(crate) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> StubBusOutcome {
        let commands = match decode_frame(message) {
            Ok(Some(commands)) => {
                self.controller.record_batch_received(commands.len());
                commands
            }
            Ok(None) => return StubBusOutcome::Break,
            Err(close_code) => {
                self.controller.record_parse_failure();
                return StubBusOutcome::Close(close_code);
            }
        };
        trace!(batch_len = commands.len(), "dispatching client bus batch");
        for command in commands {
            match self.controller.handle_command(writer, command).await {
                Ok(()) => {}
                Err(outcome) => return outcome,
            }
        }
        StubBusOutcome::Continue
    }

    pub(crate) async fn send_outbound(
        &self,
        writer: &mut WsWriter,
        outbound: SessionOutbound,
    ) -> Result<usize, WebSocketCloseCode> {
        match outbound {
            SessionOutbound::Message(message) => {
                let Some(legacy_message) = codec::legacy_server_message(message) else {
                    return Ok(0);
                };
                codec::send_server_message_batch(writer, &legacy_message).await?;
                Ok(1)
            }
            SessionOutbound::Request(request) => {
                let legacy_request = codec::legacy_server_request(*request);
                codec::send_server_request_batch(writer, &legacy_request).await?;
                Ok(1)
            }
            SessionOutbound::TrackBindingUpdate(_) => Ok(0),
            SessionOutbound::Close(code) => Err(code),
        }
    }
}

pub(crate) fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}
