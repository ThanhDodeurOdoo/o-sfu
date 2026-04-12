use std::sync::Arc;

use axum::extract::ws::Message;
use futures_util::SinkExt;

use crate::runtime::{
    channel::Channel,
    metrics::RuntimeMetrics,
    stub_bus::{StubBusOutcome, StubBusSession, WsWriter},
    transport_adapter::RuntimeTransportAdapter,
    transport_adapter::TransportConnectDirection,
};
use crate::signaling::{
    protocol::{
        ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
        EnvelopeBatch, RecordingActionResult, RequestId, ServerEnvelope, ServerRequest,
        ServerResponse, SessionDescriptionPayload, WebSocketCloseCode,
    },
    shared::SessionId,
    webrtc::RtpCapabilities as SignalingRtpCapabilities,
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

    pub(super) fn native(
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

    pub(super) async fn initialize(&mut self, writer: &mut WsWriter) -> Result<(), ()> {
        match self {
            Self::LegacyStubBus(session) => session.send_transport_bootstrap(writer).await,
            Self::Native(session) => session
                .send_initial_offer(writer)
                .await
                .map_err(|_error| ()),
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
            Self::Native(session) => session.handle_frame(writer, message).await,
        }
    }
}

const STUB_NEGOTIATION_OFFER_SDP: &str = "v=0\r\ns=o-sfu-stub-offer\r\n";

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingNegotiationRequest {
    request_id: RequestId,
    request: ServerRequest,
    client_rtp_capabilities: SignalingRtpCapabilities,
}

#[derive(Debug)]
pub(super) struct NativeSessionProtocol {
    session_id: SessionId,
    connection_id: u64,
    channel: Arc<Channel>,
    transport_adapter: RuntimeTransportAdapter,
    next_request_counter: u64,
    pending_ping_request_id: Option<RequestId>,
    pending_negotiation_request: Option<PendingNegotiationRequest>,
}

impl NativeSessionProtocol {
    fn new(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            channel,
            transport_adapter,
            next_request_counter: 0,
            pending_ping_request_id: None,
            pending_negotiation_request: None,
        }
    }

    fn awaiting_ping_response(&self) -> bool {
        self.pending_ping_request_id.is_some()
    }

    async fn send_initial_offer(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        let router_capabilities = self.channel.router_rtp_capabilities().await;
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        let bootstrap_payload = self
            .transport_adapter
            .transport_bootstrap_payload(&session_key, &router_capabilities)
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        let offer_request = ServerRequest::Offer(SessionDescriptionPayload {
            sdp: STUB_NEGOTIATION_OFFER_SDP.to_owned(),
        });
        let request_id = self
            .send_server_request(writer, offer_request.clone())
            .await?;
        self.pending_negotiation_request = Some(PendingNegotiationRequest {
            request_id,
            request: offer_request,
            client_rtp_capabilities: bootstrap_payload.router_capabilities,
        });
        Ok(())
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

    async fn send_server_request(
        &mut self,
        writer: &mut WsWriter,
        request: ServerRequest,
    ) -> Result<RequestId, WebSocketCloseCode> {
        let request_id = self.next_request_id();
        let frame = serialize_native_batch(&vec![
            ServerEnvelope::Request {
                request_id: request_id.clone(),
                request,
            }
            .into_envelope()
            .map_err(|_error| WebSocketCloseCode::Error)?,
        ])?;
        writer
            .send(Message::Text(frame.into()))
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        Ok(request_id)
    }

    async fn send_server_response(
        writer: &mut WsWriter,
        response_to: RequestId,
        response: ServerResponse,
    ) -> Result<(), WebSocketCloseCode> {
        let frame = serialize_native_batch(&vec![
            ServerEnvelope::Response {
                response_to,
                response,
            }
            .into_envelope()
            .map_err(|_error| WebSocketCloseCode::Error)?,
        ])?;
        writer
            .send(Message::Text(frame.into()))
            .await
            .map_err(|_error| WebSocketCloseCode::Error)
    }

    async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> SessionProtocolOutcome {
        match message {
            Message::Text(payload) => self.handle_text_payload(writer, &payload).await,
            Message::Binary(payload) => match String::from_utf8(payload.to_vec()) {
                Ok(payload) => self.handle_text_payload(writer, &payload).await,
                Err(_error) => SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError),
            },
            Message::Close(_) => SessionProtocolOutcome::Break,
            Message::Ping(_) | Message::Pong(_) => SessionProtocolOutcome::Continue,
        }
    }

    async fn handle_text_payload(
        &mut self,
        writer: &mut WsWriter,
        payload: &str,
    ) -> SessionProtocolOutcome {
        let Ok(batch) = serde_json::from_str::<EnvelopeBatch>(payload) else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        for envelope in batch {
            let Ok(client_envelope) = ClientEnvelope::decode(envelope) else {
                return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
            };
            let outcome = self.handle_client_envelope(writer, client_envelope).await;
            if !matches!(outcome, SessionProtocolOutcome::Continue) {
                return outcome;
            }
        }
        SessionProtocolOutcome::Continue
    }

    async fn handle_client_envelope(
        &mut self,
        writer: &mut WsWriter,
        envelope: ClientEnvelope,
    ) -> SessionProtocolOutcome {
        match envelope {
            ClientEnvelope::Message(ClientMessage::Info(info)) => {
                self.channel
                    .update_session_info(&self.session_id, info, false)
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message,
            })) => {
                self.channel.broadcast(&self.session_id, message).await;
                SessionProtocolOutcome::Continue
            }
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
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
            } => self.handle_negotiation_response(response_to, answer).await,
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StartRecording(_payload),
            } => match Self::send_server_response(
                writer,
                request_id,
                ServerResponse::StartRecording(RecordingActionResult { ok: false }),
            )
            .await
            {
                Ok(()) => SessionProtocolOutcome::Continue,
                Err(code) => SessionProtocolOutcome::Close(code),
            },
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StopRecording,
            } => match Self::send_server_response(
                writer,
                request_id,
                ServerResponse::StopRecording(RecordingActionResult { ok: false }),
            )
            .await
            {
                Ok(()) => SessionProtocolOutcome::Continue,
                Err(code) => SessionProtocolOutcome::Close(code),
            },
            ClientEnvelope::Response { .. }
            | ClientEnvelope::Message(
                ClientMessage::Auth(_)
                | ClientMessage::Publish(_)
                | ClientMessage::Unpublish(_)
                | ClientMessage::Subscribe(_),
            ) => SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError),
        }
    }

    async fn handle_negotiation_response(
        &mut self,
        response_to: RequestId,
        answer: SessionDescriptionPayload,
    ) -> SessionProtocolOutcome {
        if answer.sdp.is_empty() {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        let Some(pending_negotiation) = self.pending_negotiation_request.clone() else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        if pending_negotiation.request_id != response_to {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        self.pending_negotiation_request = None;
        if !self
            .channel
            .apply_client_rtp_capabilities(
                &self.session_id,
                pending_negotiation.client_rtp_capabilities,
                &self.transport_adapter,
            )
            .await
        {
            return SessionProtocolOutcome::Continue;
        }
        if !self
            .channel
            .apply_transport_connected(
                &self.session_id,
                TransportConnectDirection::Upload,
                &self.transport_adapter,
            )
            .await
        {
            return SessionProtocolOutcome::Continue;
        }
        if !self
            .channel
            .apply_transport_connected(
                &self.session_id,
                TransportConnectDirection::Download,
                &self.transport_adapter,
            )
            .await
        {
            return SessionProtocolOutcome::Continue;
        }
        match pending_negotiation.request {
            ServerRequest::Offer(_) | ServerRequest::Renegotiate(_) => {
                SessionProtocolOutcome::Continue
            }
            ServerRequest::Ping => SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError),
        }
    }
}

fn serialize_native_batch(batch: &EnvelopeBatch) -> Result<String, WebSocketCloseCode> {
    serde_json::to_string(&batch).map_err(|_error| WebSocketCloseCode::Error)
}
