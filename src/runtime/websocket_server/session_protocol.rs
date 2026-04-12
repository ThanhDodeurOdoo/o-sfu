use std::{collections::BTreeMap, sync::Arc};

use axum::extract::ws::Message;
use futures_util::SinkExt;

use crate::runtime::{
    channel::{Channel, SessionOutbound},
    metrics::RuntimeMetrics,
    stub_bus::{
        StubBusOutcome, StubBusSession, WsWriter, send_server_message_batch,
        send_server_request_batch,
    },
    transport_adapter::RuntimeTransportAdapter,
    transport_adapter::TransportConnectDirection,
};
use crate::signaling::{
    current_protocol::{
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentServerRequest,
        CurrentSessionInfoSnapshotById,
    },
    protocol::{
        ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
        EnvelopeBatch, PeerInfoPayload, PeerLeftPayload, RecordingActionResult, RequestId,
        ServerBroadcastPayload, ServerEnvelope, ServerMessage, ServerRequest, ServerResponse,
        SessionDescriptionPayload, TrackBinding, WebSocketCloseCode,
    },
    shared::{SessionId, SessionInfo, StreamType},
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

    pub(super) async fn send_outbound(
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
    remote_track_bindings: BTreeMap<String, TrackBinding>,
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
            remote_track_bindings: BTreeMap::new(),
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
            | ClientEnvelope::Message(ClientMessage::Auth(_) | ClientMessage::Publish(_)) => {
                SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
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

    async fn send_outbound_message(
        &mut self,
        writer: &mut WsWriter,
        message: CurrentServerMessage,
    ) -> Result<usize, WebSocketCloseCode> {
        let translated_messages = self.translate_server_message(message);
        self.send_server_messages(writer, translated_messages).await
    }

    async fn send_outbound_request(
        &mut self,
        writer: &mut WsWriter,
        request: CurrentServerRequest,
    ) -> Result<usize, WebSocketCloseCode> {
        match request {
            CurrentServerRequest::BootstrapRemoteTrack(payload) => {
                self.apply_remote_track_bootstrap(payload)?;
                self.send_server_messages(
                    writer,
                    vec![ServerMessage::Tracks(self.remote_track_snapshot())],
                )
                .await
            }
            CurrentServerRequest::BootstrapTransports(_) | CurrentServerRequest::Ping => {
                Err(WebSocketCloseCode::Error)
            }
        }
    }

    async fn send_server_messages(
        &self,
        writer: &mut WsWriter,
        messages: Vec<ServerMessage>,
    ) -> Result<usize, WebSocketCloseCode> {
        if messages.is_empty() {
            return Ok(0);
        }
        let mut batch = Vec::with_capacity(messages.len());
        for message in messages {
            batch.push(
                ServerEnvelope::Message(message)
                    .into_envelope()
                    .map_err(|_error| WebSocketCloseCode::Error)?,
            );
        }
        let frame = serialize_native_batch(&batch)?;
        writer
            .send(Message::Text(frame.into()))
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        Ok(batch.len())
    }

    fn translate_server_message(&mut self, message: CurrentServerMessage) -> Vec<ServerMessage> {
        match message {
            CurrentServerMessage::Broadcast(payload) => {
                vec![ServerMessage::Broadcast(ServerBroadcastPayload {
                    sender_id: payload.sender_id,
                    message: payload.message,
                })]
            }
            CurrentServerMessage::SessionDeparted(payload) => {
                self.remote_track_bindings
                    .retain(|_mid, binding| binding.session_id != payload.session_id);
                vec![ServerMessage::PeerLeft(PeerLeftPayload {
                    session_id: payload.session_id,
                })]
            }
            CurrentServerMessage::SessionInfoChanged(snapshot) => {
                self.translate_session_info_snapshot(snapshot)
            }
            CurrentServerMessage::ChannelStateChanged(state) => {
                vec![ServerMessage::RecordingChange(state)]
            }
        }
    }

    fn translate_session_info_snapshot(
        &mut self,
        snapshot: CurrentSessionInfoSnapshotById,
    ) -> Vec<ServerMessage> {
        let mut messages = Vec::with_capacity(snapshot.len().saturating_add(1));
        let mut track_snapshot_changed = false;
        for (bundle_key, info) in snapshot {
            let session_id = parse_bundle_session_info_key(&bundle_key);
            track_snapshot_changed |= self.apply_session_info_to_tracks(&session_id, &info);
            messages.push(ServerMessage::PeerInfo(PeerInfoPayload {
                session_id,
                info,
            }));
        }
        if track_snapshot_changed {
            messages.push(ServerMessage::Tracks(self.remote_track_snapshot()));
        }
        messages
    }

    fn apply_remote_track_bootstrap(
        &mut self,
        payload: CurrentRemoteTrackBootstrapPayload,
    ) -> Result<(), WebSocketCloseCode> {
        let Some(mid) = payload
            .rtp_parameters
            .0
            .get("mid")
            .and_then(serde_json::Value::as_str)
        else {
            return Err(WebSocketCloseCode::Error);
        };
        self.remote_track_bindings.insert(
            mid.to_owned(),
            TrackBinding {
                mid: mid.to_owned(),
                session_id: payload.session_id,
                stream_type: payload.stream_type,
                active: payload.active,
            },
        );
        Ok(())
    }

    fn apply_session_info_to_tracks(&mut self, session_id: &SessionId, info: &SessionInfo) -> bool {
        let mut changed = false;
        for binding in self.remote_track_bindings.values_mut() {
            if &binding.session_id != session_id {
                continue;
            }
            let next_active = match binding.stream_type {
                StreamType::Camera => info.is_camera_on,
                StreamType::Screen => info.is_screen_sharing_on,
                StreamType::Audio => None,
            };
            let Some(next_active) = next_active else {
                continue;
            };
            if binding.active != next_active {
                binding.active = next_active;
                changed = true;
            }
        }
        changed
    }

    fn remote_track_snapshot(&self) -> Vec<TrackBinding> {
        self.remote_track_bindings.values().cloned().collect()
    }
}

fn serialize_native_batch(batch: &EnvelopeBatch) -> Result<String, WebSocketCloseCode> {
    serde_json::to_string(&batch).map_err(|_error| WebSocketCloseCode::Error)
}

fn parse_bundle_session_info_key(key: &str) -> SessionId {
    match key.parse::<i64>() {
        Ok(value) => SessionId::Integer(value),
        Err(_error) => SessionId::String(key.to_owned()),
    }
}
