use std::sync::Arc;

use axum::extract::ws::Message;
use serde_json::Value;
use tracing::trace;

use super::{
    codec, codec::WsWriter, session_controller::SessionController, signaling_edge::decode_envelope,
};
use crate::runtime::{
    channel::Channel, metrics::RuntimeMetrics, transport_adapter::RuntimeTransportAdapter,
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
        let batch = match codec::parse_batch(message) {
            Ok(Some(batch)) => {
                self.controller.record_batch_received(batch.len());
                batch
            }
            Ok(None) => return StubBusOutcome::Break,
            Err(close_code) => {
                self.controller.record_parse_failure();
                return StubBusOutcome::Close(close_code);
            }
        };
        trace!(batch_len = batch.len(), "dispatching client bus batch");
        for envelope in batch {
            let command = decode_envelope(envelope);
            match self.controller.handle_command(writer, command).await {
                Ok(()) => {}
                Err(outcome) => return outcome,
            }
        }
        StubBusOutcome::Continue
    }
}

pub(crate) fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}
