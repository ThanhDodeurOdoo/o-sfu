use std::sync::Arc;

use axum::extract::ws::Message;
use futures_util::SinkExt;

use crate::runtime::{
    channel::Channel,
    metrics::RuntimeMetrics,
    stub_bus::{StubBusOutcome, StubBusSession, WsWriter},
    transport_adapter::RuntimeTransportAdapter,
};
use crate::signaling::{
    protocol::{
        ClientEnvelope, ClientResponse, EnvelopeBatch, RequestId, ServerEnvelope, ServerRequest,
        WebSocketCloseCode,
    },
    shared::SessionId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionProtocolOutcome {
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
pub(super) enum SessionProtocol {
    LegacyStubBus(StubBusSession),
    #[allow(
        dead_code,
        reason = "the native post-auth session path is being introduced incrementally and is not wired into handshake selection yet"
    )]
    Native(NativeSessionProtocol),
}

impl SessionProtocol {
    pub(super) fn legacy_stub_bus(
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

    #[allow(
        dead_code,
        reason = "tests and the next protocol migration slice need a concrete native session implementation before runtime selection switches away from legacy stub-bus"
    )]
    pub(super) fn native() -> Self {
        Self::Native(NativeSessionProtocol::default())
    }

    pub(super) async fn initialize(&mut self, writer: &mut WsWriter) -> Result<(), ()> {
        match self {
            Self::LegacyStubBus(session) => session.send_transport_bootstrap(writer).await,
            Self::Native(_session) => {
                let _ = writer;
                Ok(())
            }
        }
    }

    pub(super) fn awaiting_ping_response(&self) -> bool {
        match self {
            Self::LegacyStubBus(session) => session.awaiting_ping_response(),
            Self::Native(session) => session.awaiting_ping_response(),
        }
    }

    pub(super) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        match self {
            Self::LegacyStubBus(session) => session.send_ping(writer).await,
            Self::Native(session) => session.send_ping(writer).await,
        }
    }

    pub(super) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        match self {
            Self::LegacyStubBus(session) => session.handle_frame(writer, message).await.into(),
            Self::Native(session) => session.handle_frame(writer, message),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct NativeSessionProtocol {
    next_request_counter: u64,
    pending_ping_request_id: Option<RequestId>,
}

impl NativeSessionProtocol {
    fn awaiting_ping_response(&self) -> bool {
        self.pending_ping_request_id.is_some()
    }

    fn build_ping_frame(&mut self) -> Result<(RequestId, String), WebSocketCloseCode> {
        let ping_request_id = self.next_request_id();
        let frame = serialize_native_batch(&vec![
            ServerEnvelope::Request {
                request_id: ping_request_id.clone(),
                request: ServerRequest::Ping,
            }
            .into_envelope()
            .map_err(|_error| WebSocketCloseCode::Error)?,
        ])?;
        Ok((ping_request_id, frame))
    }

    fn next_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }

    async fn send_ping(&mut self, writer: &mut WsWriter) -> Result<(), WebSocketCloseCode> {
        if self.pending_ping_request_id.is_some() {
            return Ok(());
        }
        let (ping_request_id, frame) = self.build_ping_frame()?;
        writer
            .send(Message::Text(frame.into()))
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        self.pending_ping_request_id = Some(ping_request_id);
        Ok(())
    }

    fn handle_frame(&mut self, _writer: &mut WsWriter, message: Message) -> SessionProtocolOutcome {
        match message {
            Message::Text(payload) => self.handle_text_payload(&payload),
            Message::Binary(payload) => match String::from_utf8(payload.to_vec()) {
                Ok(payload) => self.handle_text_payload(&payload),
                Err(_error) => SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError),
            },
            Message::Close(_) => SessionProtocolOutcome::Break,
            Message::Ping(_) | Message::Pong(_) => SessionProtocolOutcome::Continue,
        }
    }

    fn handle_text_payload(&mut self, payload: &str) -> SessionProtocolOutcome {
        let Ok(batch) = serde_json::from_str::<EnvelopeBatch>(payload) else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        for envelope in batch {
            let Ok(client_envelope) = ClientEnvelope::decode(envelope) else {
                return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
            };
            let outcome = self.handle_client_envelope(client_envelope);
            if !matches!(outcome, SessionProtocolOutcome::Continue) {
                return outcome;
            }
        }
        SessionProtocolOutcome::Continue
    }

    fn handle_client_envelope(&mut self, envelope: ClientEnvelope) -> SessionProtocolOutcome {
        match envelope {
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Ping,
            } if self
                .pending_ping_request_id
                .as_ref()
                .is_some_and(|request_id| request_id == &response_to) =>
            {
                self.pending_ping_request_id = None;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Response { .. }
            | ClientEnvelope::Message(_)
            | ClientEnvelope::Request { .. } => {
                SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }
}

fn serialize_native_batch(batch: &EnvelopeBatch) -> Result<String, WebSocketCloseCode> {
    serde_json::to_string(&batch).map_err(|_error| WebSocketCloseCode::Error)
}

#[cfg(test)]
mod tests {
    use super::{NativeSessionProtocol, serialize_native_batch};
    use crate::runtime::websocket_server::session_protocol::SessionProtocolOutcome;
    use crate::signaling::protocol::{
        ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId, ServerEnvelope,
        ServerRequest, StreamIntentPayload, WebSocketCloseCode,
    };
    use crate::signaling::shared::StreamType;

    #[test]
    fn native_session_protocol_encodes_typed_ping_requests_and_clears_matching_response() {
        let mut session = NativeSessionProtocol::default();
        let ping_frame = session.build_ping_frame();
        assert!(ping_frame.is_ok());
        let Ok((ping_request_id, frame)) = ping_frame else {
            return;
        };
        assert!(!session.awaiting_ping_response());

        session.pending_ping_request_id = Some(ping_request_id.clone());
        let response_batch = vec![
            ClientEnvelope::Response {
                response_to: ping_request_id,
                response: ClientResponse::Ping,
            }
            .into_envelope(),
        ];
        assert!(response_batch.iter().all(Result::is_ok));
        let response_envelopes = response_batch.into_iter().collect::<Result<Vec<_>, _>>();
        assert!(response_envelopes.is_ok());
        let Ok(response_envelopes) = response_envelopes else {
            return;
        };
        let response = serialize_native_batch(&response_envelopes);
        assert!(response.is_ok());
        let Ok(response) = response else {
            return;
        };

        let decoded_batch = serde_json::from_str::<EnvelopeBatch>(&frame);
        assert!(decoded_batch.is_ok());
        let Ok(decoded_batch) = decoded_batch else {
            return;
        };
        let decoded = decoded_batch.into_iter().next();
        assert!(decoded.is_some());
        let Some(decoded) = decoded else {
            return;
        };
        assert_eq!(
            ServerEnvelope::decode(decoded),
            Ok(ServerEnvelope::Request {
                request_id: RequestId::new("server-0"),
                request: ServerRequest::Ping,
            })
        );

        assert_eq!(
            session.handle_text_payload(&response),
            SessionProtocolOutcome::Continue
        );
        assert!(!session.awaiting_ping_response());
    }

    #[test]
    fn native_session_protocol_rejects_malformed_batches() {
        let mut session = NativeSessionProtocol::default();

        assert_eq!(
            session.handle_text_payload("{not-json"),
            SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
        );
    }

    #[test]
    fn native_session_protocol_rejects_unsupported_client_messages() {
        let mut session = NativeSessionProtocol::default();
        let publish_batch = vec![
            ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
                stream_type: StreamType::Camera,
            }))
            .into_envelope(),
        ];
        assert!(publish_batch.iter().all(Result::is_ok));
        let publish_envelopes = publish_batch.into_iter().collect::<Result<Vec<_>, _>>();
        assert!(publish_envelopes.is_ok());
        let Ok(publish_envelopes) = publish_envelopes else {
            return;
        };
        let publish = serialize_native_batch(&publish_envelopes);
        assert!(publish.is_ok());
        let Ok(publish) = publish else {
            return;
        };

        assert_eq!(
            session.handle_text_payload(&publish),
            SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
        );
    }
}
