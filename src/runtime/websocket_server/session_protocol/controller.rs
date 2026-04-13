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
use crate::signaling::{
    current_protocol::{CurrentServerMessage, CurrentServerRequest},
    protocol::{
        ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
        RecordingActionResult, RequestId, ServerMessage, ServerRequest, ServerResponse,
        SessionDescriptionPayload, WebSocketCloseCode,
    },
    shared::SessionId,
};

use super::{
    frame_codec::{
        decode_client_batch, send_server_messages, send_server_request, send_server_response,
    },
    negotiation::{
        NegotiationState, PendingNegotiationAction, PendingNegotiationRequest,
        RenegotiationDisposition,
    },
    request_state::NativeRequestState,
    track_projection::RemoteTrackProjection,
};

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
    #[allow(
        dead_code,
        reason = "the native post-auth session path is still gated to the stub transport while the real RTC backend finishes the remaining renegotiation migration"
    )]
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

#[derive(Debug)]
pub(in crate::runtime::websocket_server) struct NativeSessionProtocol {
    session_id: SessionId,
    connection_id: u64,
    channel: Arc<Channel>,
    transport_adapter: RuntimeTransportAdapter,
    request_state: NativeRequestState,
    negotiation: NegotiationState,
    track_projection: RemoteTrackProjection,
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
            request_state: NativeRequestState::default(),
            negotiation: NegotiationState::default(),
            track_projection: RemoteTrackProjection::default(),
        }
    }

    fn awaiting_ping_response(&self) -> bool {
        self.request_state.awaiting_ping_response()
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
        let offer = self
            .transport_adapter
            .create_initial_session_offer(&session_key)
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        let offer_request = ServerRequest::Offer(SessionDescriptionPayload {
            sdp: offer.into_sdp(),
        });
        self.issue_negotiation_request(
            writer,
            offer_request,
            PendingNegotiationAction::EstablishSession {
                client_rtp_capabilities: bootstrap_payload.router_capabilities,
            },
        )
        .await
    }

    async fn issue_negotiation_request(
        &mut self,
        writer: &mut WsWriter,
        request: ServerRequest,
        action: PendingNegotiationAction,
    ) -> Result<(), WebSocketCloseCode> {
        let request_id = self.request_state.next_request_id();
        send_server_request(writer, request_id.clone(), request.clone()).await?;
        self.negotiation.issue(request_id, request, action);
        Ok(())
    }

    async fn request_renegotiation(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<bool, WebSocketCloseCode> {
        match self.negotiation.request_renegotiation() {
            RenegotiationDisposition::Skip | RenegotiationDisposition::QueueOnly => Ok(false),
            RenegotiationDisposition::SendNow => {
                let session_key = self
                    .channel
                    .transport_session_key(&self.session_id, self.connection_id);
                let offer = self
                    .transport_adapter
                    .create_session_renegotiation_offer(&session_key)
                    .await
                    .map_err(|_error| WebSocketCloseCode::Error)?;
                self.issue_negotiation_request(
                    writer,
                    ServerRequest::Renegotiate(SessionDescriptionPayload {
                        sdp: offer.into_sdp(),
                    }),
                    PendingNegotiationAction::RefreshSession,
                )
                .await?;
                Ok(true)
            }
        }
    }

    async fn send_ping(&mut self, writer: &mut WsWriter) -> Result<(), WebSocketCloseCode> {
        let Some(request_id) = self.request_state.start_ping() else {
            return Ok(());
        };
        send_server_request(writer, request_id, ServerRequest::Ping).await
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
        let Ok(batch) = decode_client_batch(payload) else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        for envelope in batch {
            let outcome = self.handle_client_envelope(writer, envelope).await;
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
            ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
                self.channel
                    .update_download_state(
                        &self.session_id,
                        &payload.session_id,
                        &payload.states,
                        &self.transport_adapter,
                    )
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
                self.channel
                    .update_upload_state(
                        &self.session_id,
                        payload.stream_type,
                        false,
                        &self.transport_adapter,
                    )
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Ping,
            } if self.request_state.resolve_ping_response(&response_to) => {
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
            } => {
                self.handle_negotiation_response(writer, response_to, answer)
                    .await
            }
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StartRecording(_payload),
            } => match send_server_response(
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
            } => match send_server_response(
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
            | ClientEnvelope::Message(ClientMessage::Auth(_) | ClientMessage::Publish(_)) => {
                SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }

    async fn handle_negotiation_response(
        &mut self,
        writer: &mut WsWriter,
        response_to: RequestId,
        answer: SessionDescriptionPayload,
    ) -> SessionProtocolOutcome {
        if answer.sdp.is_empty() {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        let Some(resolved) = self.negotiation.resolve_answer(&response_to) else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        if self
            .transport_adapter
            .apply_session_answer(&session_key, &answer.sdp)
            .await
            .is_err()
        {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::Error);
        }
        if self
            .apply_negotiation_action(&resolved.pending)
            .await
            .is_err()
        {
            return SessionProtocolOutcome::Continue;
        }
        if matches!(resolved.pending.request, ServerRequest::Ping) {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        if resolved.queued_renegotiation {
            match self.request_renegotiation(writer).await {
                Ok(_sent) => {}
                Err(code) => return SessionProtocolOutcome::Close(code),
            }
        }
        SessionProtocolOutcome::Continue
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingNegotiationRequest,
    ) -> Result<(), ()> {
        match &pending.action {
            PendingNegotiationAction::EstablishSession {
                client_rtp_capabilities,
            } => {
                if !self
                    .channel
                    .apply_session_negotiated(
                        &self.session_id,
                        self.connection_id,
                        client_rtp_capabilities.clone(),
                        &self.transport_adapter,
                    )
                    .await
                {
                    return Err(());
                }
            }
            PendingNegotiationAction::RefreshSession => {}
        }
        Ok(())
    }

    async fn send_outbound_message(
        &mut self,
        writer: &mut WsWriter,
        message: CurrentServerMessage,
    ) -> Result<usize, WebSocketCloseCode> {
        let translated = self.track_projection.translate_server_message(message);
        let mut batch_len = send_server_messages(writer, translated.messages).await?;
        if translated.needs_renegotiation && self.request_renegotiation(writer).await? {
            batch_len += 1;
        }
        Ok(batch_len)
    }

    async fn send_outbound_request(
        &mut self,
        writer: &mut WsWriter,
        request: CurrentServerRequest,
    ) -> Result<usize, WebSocketCloseCode> {
        match request {
            CurrentServerRequest::BootstrapRemoteTrack(payload) => {
                self.track_projection
                    .apply_remote_track_bootstrap(payload)?;
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
            CurrentServerRequest::BootstrapTransports(_) | CurrentServerRequest::Ping => {
                Err(WebSocketCloseCode::Error)
            }
        }
    }
}
