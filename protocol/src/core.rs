use std::{
    collections::{BTreeMap, BTreeSet},
    mem::take,
};

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate,
        BundleSessionInfoSnapshotById, BundleUpdate, bundle_session_info_key,
    },
    shared::{
        AvailableFeatures, DownloadStates, JsonPayload, RecordingState, SessionId, SessionInfo,
        StreamType,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest,
        ClientResponse, Envelope, EnvelopeBatch, PeerSnapshot, RecordingOptions, RequestId,
        ServerEnvelope, ServerMessage, ServerRequest, ServerResponse, SessionDescriptionPayload,
        StreamIntentPayload, SubscribePayload, TrackBinding, WebSocketCloseCode, WelcomePayload,
    },
};

pub use crate::bundle_api::BundleConnectionState as ConnectionState;

/// Timer id used by the recovery backoff scheduler.
pub const RECOVERY_TIMER_ID: u32 = 1;
const BATCH_FLUSH_TIMER_ID: u32 = 2;
const REQUEST_TIMEOUT_TIMER_ID_BASE: u32 = 10_000;
const INITIAL_RECOVERY_DELAY_MS: u32 = 1_000;
const MAX_RECOVERY_DELAY_MS: u32 = 30_000;
const BATCH_FLUSH_DELAY_MS: u32 = 100;
const REQUEST_TIMEOUT_MS: u32 = 5_000;
const MAX_OUTBOUND_BATCH_LEN: usize = 16;

/// Side-effect command returned by [`ProtocolCore`] methods.
///
/// The state machine itself is pure: it never touches I/O. Instead each
/// transition returns a `Vec<Command>` that the host (wasm glue, native
/// driver, test harness) must execute in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Serialize and send a JSON frame over the WebSocket.
    SendWebSocket(String),
    /// Apply a remote SDP offer to the local `RTCPeerConnection`.
    ApplyNegotiation {
        request_id: RequestId,
        kind: NegotiationKind,
        sdp: String,
    },
    /// Bind an incoming RTP track (identified by its SDP mid) to a stream type.
    AttachTrack {
        mid: String,
        stream_type: StreamType,
    },
    /// Remove the local track for the given stream type.
    DetachTrack {
        stream_type: StreamType,
    },
    CreatePeerConnection,
    ClosePeerConnection,
    CloseWebSocket {
        code: u16,
    },
    /// Notify listeners of a connection-state transition, with an optional
    /// human-readable cause (e.g. `"kicked"`, `"full"`).
    EmitStateChange {
        state: ConnectionState,
        cause: Option<String>,
    },
    /// Push a [`BundleUpdate`] event to the Odoo bundle compatibility layer.
    EmitUpdate {
        update: BundleUpdate,
    },
    RegisterPendingRequest {
        request_id: RequestId,
        kind: PendingRequestKind,
    },
    ResolvePendingRequest {
        request_id: RequestId,
        ok: bool,
    },
    /// Start a one-shot timer; the host must call [`ProtocolCore::on_timer`]
    /// when it fires.
    ScheduleTimer {
        id: u32,
        ms: u32,
    },
    CancelTimer {
        id: u32,
    },
    /// Open a new WebSocket to the given URL.
    Connect {
        url: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationKind {
    Offer,
    Renegotiate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingRequestKind {
    StartRecording,
    StopRecording,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectContext {
    url: String,
    jwt: String,
    channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRequest {
    kind: PendingRequestKind,
    timeout_timer_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingNegotiation {
    request_id: RequestId,
    kind: NegotiationKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolCore {
    state: ConnectionState,
    features: AvailableFeatures,
    recording_state: RecordingState,
    track_bindings: BTreeMap<String, TrackBinding>,
    active_uploads: BTreeSet<StreamType>,
    desired_downloads: BTreeMap<SessionId, DownloadStates>,
    desired_info: Option<SessionInfo>,
    connect_context: Option<ConnectContext>,
    recovery_delay_ms: u32,
    pending_batch: EnvelopeBatch,
    batch_flush_scheduled: bool,
    next_request_counter: u64,
    next_request_timeout_timer_id: u32,
    pending_requests: BTreeMap<RequestId, PendingRequest>,
    request_timeouts: BTreeMap<u32, RequestId>,
    pending_negotiation: Option<PendingNegotiation>,
}

impl Default for ProtocolCore {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolCore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: BundleConnectionState::Disconnected,
            features: empty_features(),
            recording_state: RecordingState::default(),
            track_bindings: BTreeMap::new(),
            active_uploads: BTreeSet::new(),
            desired_downloads: BTreeMap::new(),
            desired_info: None,
            connect_context: None,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
            pending_batch: Vec::new(),
            batch_flush_scheduled: false,
            next_request_counter: 0,
            next_request_timeout_timer_id: REQUEST_TIMEOUT_TIMER_ID_BASE,
            pending_requests: BTreeMap::new(),
            request_timeouts: BTreeMap::new(),
            pending_negotiation: None,
        }
    }

    #[must_use]
    pub const fn state(&self) -> ConnectionState {
        self.state
    }

    #[must_use]
    pub const fn features(&self) -> &AvailableFeatures {
        &self.features
    }

    #[must_use]
    pub const fn recording_state(&self) -> &RecordingState {
        &self.recording_state
    }

    #[must_use]
    pub fn track_binding(&self, mid: &str) -> Option<&TrackBinding> {
        self.track_bindings.get(mid)
    }

    pub fn connect(
        &mut self,
        url: impl Into<String>,
        jwt: impl Into<String>,
        channel: Option<String>,
    ) -> Vec<Command> {
        if !matches!(
            self.state,
            BundleConnectionState::Disconnected | BundleConnectionState::Closed
        ) {
            return Vec::new();
        }
        let url = url.into();
        self.connect_context = Some(ConnectContext {
            url: url.clone(),
            jwt: jwt.into(),
            channel,
        });
        self.features = empty_features();
        self.recording_state = RecordingState::default();
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.clear_sticky_state();
        self.clear_runtime_state();
        self.state = BundleConnectionState::Connecting;
        vec![
            Command::EmitStateChange {
                state: self.state,
                cause: None,
            },
            Command::Connect { url },
        ]
    }

    pub fn on_ws_open(&mut self) -> Vec<Command> {
        if !matches!(
            self.state,
            BundleConnectionState::Connecting | BundleConnectionState::Recovering
        ) {
            return Vec::new();
        }
        let Some(connect_context) = self.connect_context.as_ref() else {
            return Vec::new();
        };
        let Some(envelope) = ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
            jwt: connect_context.jwt.clone(),
            channel: connect_context.channel.clone(),
        }))
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Immediate)
    }

    pub fn on_ws_message(&mut self, frame: &str) -> Vec<Command> {
        let Ok(batch) = serde_json::from_str::<EnvelopeBatch>(frame) else {
            return protocol_error_commands();
        };
        let mut commands = Vec::new();
        for envelope in batch {
            let Ok(server_envelope) = ServerEnvelope::decode(envelope) else {
                return protocol_error_commands();
            };
            commands.extend(self.handle_server_envelope(server_envelope));
        }
        commands
    }

    pub fn on_welcome(&mut self, payload: WelcomePayload) -> Vec<Command> {
        if !matches!(
            self.state,
            BundleConnectionState::Connecting | BundleConnectionState::Recovering
        ) {
            return Vec::new();
        }
        self.features = payload.features;
        self.recording_state = payload.recording;
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.state = BundleConnectionState::Authenticated;

        let mut commands = vec![Command::EmitStateChange {
            state: self.state,
            cause: None,
        }];
        let snapshot = peer_snapshot_update(&payload.peers);
        if !snapshot.is_empty() {
            commands.push(Command::EmitUpdate {
                update: BundleUpdate::SessionInfoChange(snapshot),
            });
        }
        commands.extend(self.replay_sticky_state());
        commands
    }

    pub fn on_transport_ready(&mut self) -> Vec<Command> {
        if self.state != BundleConnectionState::Authenticated {
            return Vec::new();
        }
        self.state = BundleConnectionState::Connected;
        vec![Command::EmitStateChange {
            state: self.state,
            cause: None,
        }]
    }

    pub fn update_upload(&mut self, stream_type: StreamType, active: bool) -> Vec<Command> {
        if active {
            self.active_uploads.insert(stream_type);
        } else {
            self.active_uploads.remove(&stream_type);
        }
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(envelope) = ClientEnvelope::Message(if active {
            ClientMessage::Publish(StreamIntentPayload { stream_type })
        } else {
            ClientMessage::Unpublish(StreamIntentPayload { stream_type })
        })
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Batched)
    }

    pub fn update_download(
        &mut self,
        session_id: SessionId,
        states: DownloadStates,
    ) -> Vec<Command> {
        self.remember_download_states(&session_id, &states);
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(envelope) = ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
            session_id,
            states,
        }))
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Batched)
    }

    pub fn update_info(&mut self, info: SessionInfo) -> Vec<Command> {
        self.remember_info(&info);
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(envelope) = ClientEnvelope::Message(ClientMessage::Info(info))
            .into_envelope()
            .ok()
        else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Batched)
    }

    pub fn broadcast(&mut self, message: JsonPayload) -> Vec<Command> {
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(envelope) =
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload { message }))
                .into_envelope()
                .ok()
        else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Batched)
    }

    pub fn start_recording(&mut self, options: RecordingOptions) -> Vec<Command> {
        self.begin_request(
            ClientRequest::StartRecording(options),
            PendingRequestKind::StartRecording,
        )
    }

    pub fn stop_recording(&mut self) -> Vec<Command> {
        self.begin_request(
            ClientRequest::StopRecording,
            PendingRequestKind::StopRecording,
        )
    }

    pub fn submit_negotiation_answer(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
        sdp: impl Into<String>,
    ) -> Vec<Command> {
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(pending_negotiation) = self.pending_negotiation.as_ref() else {
            return Vec::new();
        };
        if pending_negotiation.request_id != *request_id || pending_negotiation.kind != kind {
            return Vec::new();
        }
        self.pending_negotiation = None;
        let response = match kind {
            NegotiationKind::Offer => {
                ClientResponse::Offer(SessionDescriptionPayload { sdp: sdp.into() })
            }
            NegotiationKind::Renegotiate => {
                ClientResponse::Renegotiate(SessionDescriptionPayload { sdp: sdp.into() })
            }
        };
        let Some(envelope) = ClientEnvelope::Response {
            response_to: request_id.clone(),
            response,
        }
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Immediate)
    }

    pub fn disconnect(&mut self) -> Vec<Command> {
        if matches!(
            self.state,
            BundleConnectionState::Disconnected | BundleConnectionState::Closed
        ) {
            return Vec::new();
        }
        self.state = BundleConnectionState::Disconnected;
        self.connect_context = None;
        self.features = empty_features();
        self.recording_state = RecordingState::default();
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.clear_sticky_state();

        let mut commands = vec![Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        }];
        commands.extend(self.clear_runtime_state_with_commands());
        commands.extend([
            Command::CloseWebSocket {
                code: u16::from(WebSocketCloseCode::Clean),
            },
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: self.state,
                cause: None,
            },
        ]);
        commands
    }

    pub fn on_ws_close(&mut self, code: u16) -> Vec<Command> {
        if matches!(
            self.state,
            BundleConnectionState::Disconnected | BundleConnectionState::Closed
        ) {
            return Vec::new();
        }

        let mut commands = Vec::new();
        commands.extend(self.clear_runtime_state_with_commands());

        if let Some(
            WebSocketCloseCode::AuthFailed
            | WebSocketCloseCode::Kicked
            | WebSocketCloseCode::ChannelFull,
        ) = web_socket_close_code(code)
        {
            self.state = BundleConnectionState::Closed;
            self.connect_context = None;
            self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
            commands.extend([
                Command::CancelTimer {
                    id: RECOVERY_TIMER_ID,
                },
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: self.state,
                    cause: close_cause(code).map(str::to_owned),
                },
            ]);
            return commands;
        }

        let Some(connect_context) = self.connect_context.as_ref() else {
            self.state = BundleConnectionState::Disconnected;
            commands.push(Command::EmitStateChange {
                state: self.state,
                cause: None,
            });
            return commands;
        };
        let _ = connect_context;
        let delay_ms = self.recovery_delay_ms;
        self.recovery_delay_ms = next_recovery_delay(delay_ms);
        self.state = BundleConnectionState::Recovering;
        commands.extend([
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: self.state,
                cause: None,
            },
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ms: delay_ms,
            },
        ]);
        commands
    }

    pub fn on_timer(&mut self, timer_id: u32) -> Vec<Command> {
        if timer_id == RECOVERY_TIMER_ID {
            return self.handle_recovery_timer();
        }
        if timer_id == BATCH_FLUSH_TIMER_ID {
            return self.flush_pending_batch(false);
        }
        if let Some(request_id) = self.request_timeouts.remove(&timer_id) {
            return self.handle_request_timeout(&request_id);
        }
        Vec::new()
    }

    fn handle_recovery_timer(&mut self) -> Vec<Command> {
        if self.state != BundleConnectionState::Recovering {
            return Vec::new();
        }
        let Some(connect_context) = self.connect_context.as_ref() else {
            return Vec::new();
        };
        self.state = BundleConnectionState::Connecting;
        vec![
            Command::EmitStateChange {
                state: self.state,
                cause: None,
            },
            Command::Connect {
                url: connect_context.url.clone(),
            },
        ]
    }

    fn handle_server_envelope(&mut self, envelope: ServerEnvelope) -> Vec<Command> {
        match envelope {
            ServerEnvelope::Message(message) => self.handle_server_message(message),
            ServerEnvelope::Request {
                request_id,
                request,
            } => self.handle_server_request(request_id, request),
            ServerEnvelope::Response {
                response_to,
                response,
            } => self.handle_server_response(&response_to, response),
        }
    }

    fn handle_server_message(&mut self, message: ServerMessage) -> Vec<Command> {
        match message {
            ServerMessage::Welcome(payload) => self.on_welcome(payload),
            ServerMessage::Tracks(bindings) => {
                self.replace_track_bindings(bindings);
                Vec::new()
            }
            ServerMessage::PeerInfo(payload) | ServerMessage::PeerJoined(payload) => {
                vec![Command::EmitUpdate {
                    update: BundleUpdate::SessionInfoChange(single_peer_update(
                        &payload.session_id,
                        payload.info,
                    )),
                }]
            }
            ServerMessage::PeerLeft(payload) => {
                self.remove_track_bindings_for_session(&payload.session_id);
                vec![Command::EmitUpdate {
                    update: BundleUpdate::Disconnect(BundleDisconnectUpdate {
                        session_id: payload.session_id,
                    }),
                }]
            }
            ServerMessage::Broadcast(payload) => vec![Command::EmitUpdate {
                update: BundleUpdate::Broadcast(BundleBroadcastUpdate {
                    sender_id: payload.sender_id,
                    message: payload.message,
                }),
            }],
            ServerMessage::RecordingChange(payload) => {
                self.recording_state = payload.state.clone();
                vec![Command::EmitUpdate {
                    update: BundleUpdate::ChannelInfoChange(payload),
                }]
            }
        }
    }

    fn handle_server_request(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
    ) -> Vec<Command> {
        match request {
            ServerRequest::Offer(payload) => {
                self.handle_negotiation_request(request_id, NegotiationKind::Offer, payload)
            }
            ServerRequest::Renegotiate(payload) => {
                self.handle_negotiation_request(request_id, NegotiationKind::Renegotiate, payload)
            }
            ServerRequest::Ping => {
                let Some(envelope) = ClientEnvelope::Response {
                    response_to: request_id,
                    response: ClientResponse::Ping,
                }
                .into_envelope()
                .ok() else {
                    return Vec::new();
                };
                self.enqueue_envelope(envelope, FlushMode::Immediate)
            }
        }
    }

    fn handle_server_response(
        &mut self,
        response_to: &RequestId,
        response: ServerResponse,
    ) -> Vec<Command> {
        match response {
            ServerResponse::StartRecording(payload) => {
                self.resolve_request(response_to, PendingRequestKind::StartRecording, payload.ok)
            }
            ServerResponse::StopRecording(payload) => {
                self.resolve_request(response_to, PendingRequestKind::StopRecording, payload.ok)
            }
        }
    }

    fn handle_negotiation_request(
        &mut self,
        request_id: RequestId,
        kind: NegotiationKind,
        payload: SessionDescriptionPayload,
    ) -> Vec<Command> {
        if !matches!(
            self.state,
            BundleConnectionState::Authenticated | BundleConnectionState::Connected
        ) {
            return Vec::new();
        }
        if self.pending_negotiation.is_some() {
            return protocol_error_commands();
        }
        let pending_request_id = request_id.clone();
        self.pending_negotiation = Some(PendingNegotiation { request_id, kind });
        let mut commands = Vec::new();
        if kind == NegotiationKind::Offer && self.state == BundleConnectionState::Authenticated {
            commands.push(Command::CreatePeerConnection);
        }
        commands.push(Command::ApplyNegotiation {
            request_id: pending_request_id,
            kind,
            sdp: payload.sdp,
        });
        commands
    }

    fn begin_request(&mut self, request: ClientRequest, kind: PendingRequestKind) -> Vec<Command> {
        if !self.can_send_client_messages() || self.has_pending_request(kind) {
            return Vec::new();
        }
        let request_id = self.next_request_id();
        let timeout_timer_id = self.next_request_timeout_timer_id();
        let Some(envelope) = ClientEnvelope::Request {
            request_id: request_id.clone(),
            request,
        }
        .into_envelope()
        .ok() else {
            return Vec::new();
        };

        self.pending_requests.insert(
            request_id.clone(),
            PendingRequest {
                kind,
                timeout_timer_id,
            },
        );
        self.request_timeouts
            .insert(timeout_timer_id, request_id.clone());

        let mut commands = vec![
            Command::RegisterPendingRequest { request_id, kind },
            Command::ScheduleTimer {
                id: timeout_timer_id,
                ms: REQUEST_TIMEOUT_MS,
            },
        ];
        commands.extend(self.enqueue_envelope(envelope, FlushMode::Batched));
        commands
    }

    fn resolve_request(
        &mut self,
        response_to: &RequestId,
        expected_kind: PendingRequestKind,
        ok: bool,
    ) -> Vec<Command> {
        let Some(pending_request) = self.pending_requests.remove(response_to) else {
            return Vec::new();
        };
        if pending_request.kind != expected_kind {
            self.pending_requests
                .insert(response_to.clone(), pending_request);
            return Vec::new();
        }
        self.request_timeouts
            .remove(&pending_request.timeout_timer_id);
        vec![
            Command::CancelTimer {
                id: pending_request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: response_to.clone(),
                ok,
            },
        ]
    }

    fn handle_request_timeout(&mut self, request_id: &RequestId) -> Vec<Command> {
        let Some(_pending_request) = self.pending_requests.remove(request_id) else {
            return Vec::new();
        };
        vec![Command::ResolvePendingRequest {
            request_id: request_id.clone(),
            ok: false,
        }]
    }

    fn enqueue_envelope(&mut self, envelope: Envelope, mode: FlushMode) -> Vec<Command> {
        match mode {
            FlushMode::Immediate => {
                self.pending_batch.push(envelope);
                self.flush_pending_batch(true)
            }
            FlushMode::Batched => {
                self.pending_batch.push(envelope);
                if self.pending_batch.len() >= MAX_OUTBOUND_BATCH_LEN {
                    self.flush_pending_batch(true)
                } else if self.batch_flush_scheduled {
                    Vec::new()
                } else {
                    self.batch_flush_scheduled = true;
                    vec![Command::ScheduleTimer {
                        id: BATCH_FLUSH_TIMER_ID,
                        ms: BATCH_FLUSH_DELAY_MS,
                    }]
                }
            }
        }
    }

    fn flush_pending_batch(&mut self, cancel_timer: bool) -> Vec<Command> {
        if self.pending_batch.is_empty() {
            self.batch_flush_scheduled = false;
            return Vec::new();
        }
        let batch = take(&mut self.pending_batch);
        let Ok(frame) = serde_json::to_string(&batch) else {
            self.batch_flush_scheduled = false;
            return Vec::new();
        };
        let had_timer = self.batch_flush_scheduled;
        self.batch_flush_scheduled = false;
        let mut commands = Vec::new();
        if cancel_timer && had_timer {
            commands.push(Command::CancelTimer {
                id: BATCH_FLUSH_TIMER_ID,
            });
        }
        commands.push(Command::SendWebSocket(frame));
        commands
    }

    fn clear_runtime_state(&mut self) {
        self.track_bindings.clear();
        self.pending_batch.clear();
        self.batch_flush_scheduled = false;
        self.pending_requests.clear();
        self.request_timeouts.clear();
        self.pending_negotiation = None;
    }

    fn clear_runtime_state_with_commands(&mut self) -> Vec<Command> {
        let mut commands = Vec::new();
        if self.batch_flush_scheduled {
            commands.push(Command::CancelTimer {
                id: BATCH_FLUSH_TIMER_ID,
            });
        }

        let pending_request_ids: Vec<RequestId> = self.pending_requests.keys().cloned().collect();
        for request_id in pending_request_ids {
            let Some(pending_request) = self.pending_requests.remove(&request_id) else {
                continue;
            };
            self.request_timeouts
                .remove(&pending_request.timeout_timer_id);
            commands.push(Command::CancelTimer {
                id: pending_request.timeout_timer_id,
            });
            commands.push(Command::ResolvePendingRequest {
                request_id,
                ok: false,
            });
        }

        self.pending_batch.clear();
        self.batch_flush_scheduled = false;
        self.track_bindings.clear();
        self.pending_negotiation = None;
        commands
    }

    fn clear_sticky_state(&mut self) {
        self.active_uploads.clear();
        self.desired_downloads.clear();
        self.desired_info = None;
    }

    fn replay_sticky_state(&mut self) -> Vec<Command> {
        if !self.can_send_client_messages() {
            return Vec::new();
        }

        let mut replay_batch = Vec::new();

        for &stream_type in &self.active_uploads {
            let Some(envelope) =
                ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
                    stream_type,
                }))
                .into_envelope()
                .ok()
            else {
                continue;
            };
            replay_batch.push(envelope);
        }

        for (session_id, states) in &self.desired_downloads {
            let Some(envelope) =
                ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                    session_id: session_id.clone(),
                    states: states.clone(),
                }))
                .into_envelope()
                .ok()
            else {
                continue;
            };
            replay_batch.push(envelope);
        }

        if let Some(info) = self.desired_info.clone() {
            let Some(envelope) = ClientEnvelope::Message(ClientMessage::Info(info))
                .into_envelope()
                .ok()
            else {
                return Vec::new();
            };
            replay_batch.push(envelope);
        }

        if replay_batch.is_empty() {
            return Vec::new();
        }

        self.pending_batch.extend(replay_batch);
        self.flush_pending_batch(true)
    }

    fn remember_download_states(&mut self, session_id: &SessionId, states: &DownloadStates) {
        let existing_states = self
            .desired_downloads
            .entry(session_id.clone())
            .or_default();
        merge_download_states(existing_states, states);
        if download_states_are_empty(existing_states) {
            self.desired_downloads.remove(session_id);
        }
    }

    fn remember_info(&mut self, info: &SessionInfo) {
        let existing_info = self.desired_info.get_or_insert_with(SessionInfo::default);
        merge_session_info(existing_info, info);
    }

    fn replace_track_bindings(&mut self, bindings: Vec<TrackBinding>) {
        self.track_bindings = bindings
            .into_iter()
            .map(|binding| (binding.mid.clone(), binding))
            .collect();
    }

    fn remove_track_bindings_for_session(&mut self, session_id: &SessionId) {
        self.track_bindings
            .retain(|_, binding| &binding.session_id != session_id);
    }

    fn has_pending_request(&self, kind: PendingRequestKind) -> bool {
        self.pending_requests
            .values()
            .any(|pending_request| pending_request.kind == kind)
    }

    fn can_send_client_messages(&self) -> bool {
        matches!(
            self.state,
            BundleConnectionState::Authenticated | BundleConnectionState::Connected
        )
    }

    fn next_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(self.next_request_counter.to_string());
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }

    fn next_request_timeout_timer_id(&mut self) -> u32 {
        let timer_id = self.next_request_timeout_timer_id;
        self.next_request_timeout_timer_id = self
            .next_request_timeout_timer_id
            .saturating_add(1)
            .max(REQUEST_TIMEOUT_TIMER_ID_BASE);
        timer_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushMode {
    Immediate,
    Batched,
}

fn empty_features() -> AvailableFeatures {
    AvailableFeatures {
        rtc: false,
        transcription: false,
        audio_recording: false,
        video_recording: false,
    }
}

fn merge_download_states(target: &mut DownloadStates, update: &DownloadStates) {
    if let Some(audio) = update.audio {
        target.audio = Some(audio);
    }
    if let Some(camera) = update.camera {
        target.camera = Some(camera);
    }
    if let Some(screen) = update.screen {
        target.screen = Some(screen);
    }
}

fn download_states_are_empty(states: &DownloadStates) -> bool {
    states.audio.is_none() && states.camera.is_none() && states.screen.is_none()
}

fn merge_session_info(target: &mut SessionInfo, update: &SessionInfo) {
    if let Some(is_talking) = update.is_talking {
        target.is_talking = Some(is_talking);
    }
    if let Some(is_camera_on) = update.is_camera_on {
        target.is_camera_on = Some(is_camera_on);
    }
    if let Some(is_screen_sharing_on) = update.is_screen_sharing_on {
        target.is_screen_sharing_on = Some(is_screen_sharing_on);
    }
    if let Some(is_self_muted) = update.is_self_muted {
        target.is_self_muted = Some(is_self_muted);
    }
    if let Some(is_deaf) = update.is_deaf {
        target.is_deaf = Some(is_deaf);
    }
    if let Some(is_raising_hand) = update.is_raising_hand {
        target.is_raising_hand = Some(is_raising_hand);
    }
}

fn peer_snapshot_update(peers: &[PeerSnapshot]) -> BundleSessionInfoSnapshotById {
    peers
        .iter()
        .map(|peer| (bundle_session_info_key(&peer.session_id), peer.info.clone()))
        .collect()
}

fn single_peer_update(session_id: &SessionId, info: SessionInfo) -> BundleSessionInfoSnapshotById {
    [(bundle_session_info_key(session_id), info)]
        .into_iter()
        .collect()
}

fn protocol_error_commands() -> Vec<Command> {
    vec![Command::CloseWebSocket {
        code: u16::from(WebSocketCloseCode::ProtocolError),
    }]
}

fn next_recovery_delay(current_delay_ms: u32) -> u32 {
    current_delay_ms
        .saturating_mul(3)
        .checked_div(2)
        .unwrap_or(MAX_RECOVERY_DELAY_MS)
        .min(MAX_RECOVERY_DELAY_MS)
}

fn web_socket_close_code(code: u16) -> Option<WebSocketCloseCode> {
    match code {
        1000 => Some(WebSocketCloseCode::Clean),
        1001 => Some(WebSocketCloseCode::Leaving),
        1002 => Some(WebSocketCloseCode::ProtocolError),
        1011 => Some(WebSocketCloseCode::Error),
        4001 => Some(WebSocketCloseCode::AuthFailed),
        4002 => Some(WebSocketCloseCode::AuthTimeout),
        4003 => Some(WebSocketCloseCode::Kicked),
        4004 => Some(WebSocketCloseCode::ChannelFull),
        _ => None,
    }
}

fn close_cause(code: u16) -> Option<&'static str> {
    match web_socket_close_code(code) {
        Some(WebSocketCloseCode::AuthFailed) => Some("auth_failed"),
        Some(WebSocketCloseCode::Kicked) => Some("kicked"),
        Some(WebSocketCloseCode::ChannelFull) => Some("full"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BATCH_FLUSH_TIMER_ID, Command, ConnectionState, NegotiationKind, PendingRequestKind,
        ProtocolCore, RECOVERY_TIMER_ID, REQUEST_TIMEOUT_MS,
    };
    use crate::{
        bundle_api::{BundleBroadcastUpdate, BundleDisconnectUpdate, BundleUpdate},
        shared::{
            AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, SessionInfo,
            StopCode, StreamType,
        },
        signaling::{
            AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest,
            ClientResponse, EnvelopeBatch, PeerInfoPayload, PeerLeftPayload, PeerSnapshot,
            RecordingActionResult, RecordingOptions, RequestId, ServerBroadcastPayload,
            ServerEnvelope, ServerMessage, ServerRequest, ServerResponse,
            SessionDescriptionPayload, StreamIntentPayload, SubscribePayload, TrackBinding,
            WebSocketCloseCode, WelcomePayload,
        },
    };

    fn sample_welcome_payload() -> WelcomePayload {
        WelcomePayload {
            features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: false,
                video_recording: true,
            },
            recording: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            peers: vec![PeerSnapshot {
                session_id: 7_i64.into(),
                info: SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
            }],
        }
    }

    fn decode_sent_batch(commands: &[Command]) -> EnvelopeBatch {
        let Some(Command::SendWebSocket(frame)) = commands
            .iter()
            .find(|command| matches!(command, Command::SendWebSocket(_)))
        else {
            return Vec::new();
        };
        serde_json::from_str(frame).unwrap_or_default()
    }

    fn decode_sent_client_envelopes(commands: &[Command]) -> Vec<ClientEnvelope> {
        decode_sent_batch(commands)
            .into_iter()
            .filter_map(|envelope| ClientEnvelope::decode(envelope).ok())
            .collect()
    }

    fn encode_server_batch(envelope: ServerEnvelope) -> String {
        let Ok(envelope) = envelope.into_envelope() else {
            return String::new();
        };
        serde_json::to_string(&vec![envelope]).unwrap_or_default()
    }

    #[test]
    fn protocol_core_connect_emits_connecting_state_and_socket_command() {
        let mut core = ProtocolCore::new();

        let commands = core.connect(
            "wss://sfu.example.com/socket",
            "signed-token",
            Some(String::from("channel-1")),
        );

        assert_eq!(core.state(), ConnectionState::Connecting);
        assert_eq!(
            commands,
            vec![
                Command::EmitStateChange {
                    state: ConnectionState::Connecting,
                    cause: None,
                },
                Command::Connect {
                    url: String::from("wss://sfu.example.com/socket"),
                },
            ]
        );
    }

    #[test]
    fn protocol_core_ignores_connect_while_session_is_active() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

        let commands = core.connect("wss://other.example.com/socket", "other-token", None);

        assert!(commands.is_empty());
        assert_eq!(core.state(), ConnectionState::Connecting);
    }

    #[test]
    fn protocol_core_ws_open_sends_auth_frame_immediately() {
        let mut core = ProtocolCore::new();
        let _ = core.connect(
            "wss://sfu.example.com/socket",
            "signed-token",
            Some(String::from("channel-1")),
        );

        let commands = core.on_ws_open();

        assert!(matches!(commands.as_slice(), [Command::SendWebSocket(_)]));
        let batch = decode_sent_batch(&commands);
        assert_eq!(batch.len(), 1);
        let Some(envelope) = batch.into_iter().next() else {
            return;
        };
        assert_eq!(
            ClientEnvelope::decode(envelope),
            Ok(ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
                jwt: String::from("signed-token"),
                channel: Some(String::from("channel-1")),
            })))
        );
    }

    #[test]
    fn protocol_core_welcome_transitions_to_authenticated_and_emits_peer_snapshot() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

        let commands = core.on_welcome(sample_welcome_payload());

        assert_eq!(core.state(), ConnectionState::Authenticated);
        assert!(core.features().video_recording);
        assert_eq!(core.recording_state().recording, Some(false));
        assert_eq!(
            commands,
            vec![
                Command::EmitStateChange {
                    state: ConnectionState::Authenticated,
                    cause: None,
                },
                Command::EmitUpdate {
                    update: BundleUpdate::SessionInfoChange(
                        [(
                            String::from("7"),
                            SessionInfo {
                                is_talking: Some(true),
                                ..SessionInfo::default()
                            }
                        )]
                        .into_iter()
                        .collect(),
                    ),
                },
            ]
        );
    }

    #[test]
    fn protocol_core_transport_ready_transitions_to_connected() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let commands = core.on_transport_ready();

        assert_eq!(core.state(), ConnectionState::Connected);
        assert_eq!(
            commands,
            vec![Command::EmitStateChange {
                state: ConnectionState::Connected,
                cause: None,
            }]
        );
    }

    #[test]
    fn protocol_core_batches_outbound_control_plane_messages_until_flush_timer() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let first_commands = core.update_info(SessionInfo {
            is_talking: Some(true),
            ..SessionInfo::default()
        });
        let second_commands = core.broadcast(json!({ "kind": "notice" }));

        assert_eq!(
            first_commands,
            vec![Command::ScheduleTimer {
                id: BATCH_FLUSH_TIMER_ID,
                ms: 100,
            }]
        );
        assert!(second_commands.is_empty());

        let flush_commands = core.on_timer(BATCH_FLUSH_TIMER_ID);
        let mut batch = decode_sent_batch(&flush_commands).into_iter();
        let Some(first_envelope) = batch.next() else {
            return;
        };
        let Some(second_envelope) = batch.next() else {
            return;
        };

        assert_eq!(
            ClientEnvelope::decode(first_envelope),
            Ok(ClientEnvelope::Message(ClientMessage::Info(SessionInfo {
                is_talking: Some(true),
                ..SessionInfo::default()
            })))
        );
        assert_eq!(
            ClientEnvelope::decode(second_envelope),
            Ok(ClientEnvelope::Message(ClientMessage::Broadcast(
                ClientBroadcastPayload {
                    message: json!({ "kind": "notice" }),
                }
            )))
        );
    }

    #[test]
    fn protocol_core_responds_to_server_ping_immediately() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let frame = encode_server_batch(ServerEnvelope::Request {
            request_id: RequestId::new("ping-1"),
            request: ServerRequest::Ping,
        });
        let commands = core.on_ws_message(&frame);
        let mut batch = decode_sent_batch(&commands).into_iter();
        let Some(envelope) = batch.next() else {
            return;
        };

        assert_eq!(
            ClientEnvelope::decode(envelope),
            Ok(ClientEnvelope::Response {
                response_to: RequestId::new("ping-1"),
                response: ClientResponse::Ping,
            })
        );
    }

    #[test]
    fn protocol_core_tracks_server_mid_bindings_and_clears_stale_snapshot_entries() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let first_tracks =
            encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
                TrackBinding {
                    mid: String::from("0"),
                    session_id: String::from("peer-1").into(),
                    stream_type: StreamType::Audio,
                    active: true,
                },
                TrackBinding {
                    mid: String::from("1"),
                    session_id: String::from("peer-2").into(),
                    stream_type: StreamType::Camera,
                    active: true,
                },
            ])));
        let second_tracks =
            encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
                TrackBinding {
                    mid: String::from("2"),
                    session_id: String::from("peer-2").into(),
                    stream_type: StreamType::Camera,
                    active: false,
                },
            ])));

        assert!(core.on_ws_message(&first_tracks).is_empty());
        assert_eq!(
            core.track_binding("0"),
            Some(&TrackBinding {
                mid: String::from("0"),
                session_id: String::from("peer-1").into(),
                stream_type: StreamType::Audio,
                active: true,
            })
        );
        assert_eq!(
            core.track_binding("1"),
            Some(&TrackBinding {
                mid: String::from("1"),
                session_id: String::from("peer-2").into(),
                stream_type: StreamType::Camera,
                active: true,
            })
        );

        assert!(core.on_ws_message(&second_tracks).is_empty());
        assert_eq!(core.track_binding("0"), None);
        assert_eq!(core.track_binding("1"), None);
        assert_eq!(
            core.track_binding("2"),
            Some(&TrackBinding {
                mid: String::from("2"),
                session_id: String::from("peer-2").into(),
                stream_type: StreamType::Camera,
                active: false,
            })
        );
    }

    #[test]
    fn protocol_core_peer_left_clears_track_bindings_for_that_session() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
            TrackBinding {
                mid: String::from("0"),
                session_id: String::from("peer-1").into(),
                stream_type: StreamType::Audio,
                active: true,
            },
            TrackBinding {
                mid: String::from("1"),
                session_id: String::from("peer-2").into(),
                stream_type: StreamType::Camera,
                active: true,
            },
        ])));
        let _ = core.on_ws_message(&tracks);

        let peer_left = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
            PeerLeftPayload {
                session_id: String::from("peer-1").into(),
            },
        )));

        assert_eq!(
            core.on_ws_message(&peer_left),
            vec![Command::EmitUpdate {
                update: BundleUpdate::Disconnect(BundleDisconnectUpdate {
                    session_id: String::from("peer-1").into(),
                }),
            }]
        );
        assert_eq!(core.track_binding("0"), None);
        assert!(core.track_binding("1").is_some());
    }

    #[test]
    fn protocol_core_emits_negotiation_command_and_accepts_matching_answer() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let offer_frame = encode_server_batch(ServerEnvelope::Request {
            request_id: RequestId::new("offer-1"),
            request: ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\ns=offer\r\n"),
            }),
        });
        let offer_commands = core.on_ws_message(&offer_frame);

        assert_eq!(
            offer_commands,
            vec![
                Command::CreatePeerConnection,
                Command::ApplyNegotiation {
                    request_id: RequestId::new("offer-1"),
                    kind: NegotiationKind::Offer,
                    sdp: String::from("v=0\r\ns=offer\r\n"),
                },
            ]
        );

        let answer_commands = core.submit_negotiation_answer(
            &RequestId::new("offer-1"),
            NegotiationKind::Offer,
            "v=0\r\ns=answer\r\n",
        );
        let mut batch = decode_sent_batch(&answer_commands).into_iter();
        let Some(envelope) = batch.next() else {
            return;
        };

        assert_eq!(
            ClientEnvelope::decode(envelope),
            Ok(ClientEnvelope::Response {
                response_to: RequestId::new("offer-1"),
                response: ClientResponse::Offer(SessionDescriptionPayload {
                    sdp: String::from("v=0\r\ns=answer\r\n"),
                }),
            })
        );
    }

    #[test]
    fn protocol_core_rejects_overlapping_negotiation_requests() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let first_offer = encode_server_batch(ServerEnvelope::Request {
            request_id: RequestId::new("offer-1"),
            request: ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\ns=offer-1\r\n"),
            }),
        });
        let second_offer = encode_server_batch(ServerEnvelope::Request {
            request_id: RequestId::new("offer-2"),
            request: ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\ns=offer-2\r\n"),
            }),
        });

        let _ = core.on_ws_message(&first_offer);
        let commands = core.on_ws_message(&second_offer);

        assert_eq!(
            commands,
            vec![Command::CloseWebSocket {
                code: u16::from(WebSocketCloseCode::ProtocolError),
            }]
        );
    }

    #[test]
    fn protocol_core_tracks_recording_request_until_matching_response() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let commands = core.start_recording(RecordingOptions {
            audio: Some(true),
            video: Some(true),
            transcription: None,
        });

        assert!(matches!(
            commands.as_slice(),
            [
                Command::RegisterPendingRequest {
                    request_id: _,
                    kind: PendingRequestKind::StartRecording,
                },
                Command::ScheduleTimer {
                    id: _,
                    ms: REQUEST_TIMEOUT_MS,
                },
                Command::ScheduleTimer {
                    id: BATCH_FLUSH_TIMER_ID,
                    ms: 100,
                },
            ]
        ));

        let Some(Command::RegisterPendingRequest { request_id, .. }) = commands.first() else {
            return;
        };
        let request_id = request_id.clone();
        let Some(Command::ScheduleTimer {
            id: timeout_timer_id,
            ..
        }) = commands.get(1)
        else {
            return;
        };

        let flush_commands = core.on_timer(BATCH_FLUSH_TIMER_ID);
        let mut batch = decode_sent_batch(&flush_commands).into_iter();
        let Some(envelope) = batch.next() else {
            return;
        };
        assert_eq!(
            ClientEnvelope::decode(envelope),
            Ok(ClientEnvelope::Request {
                request_id: request_id.clone(),
                request: ClientRequest::StartRecording(RecordingOptions {
                    audio: Some(true),
                    video: Some(true),
                    transcription: None,
                }),
            })
        );

        let response_frame = encode_server_batch(ServerEnvelope::Response {
            response_to: request_id.clone(),
            response: ServerResponse::StartRecording(RecordingActionResult { ok: true }),
        });
        let response_commands = core.on_ws_message(&response_frame);

        assert_eq!(
            response_commands,
            vec![
                Command::CancelTimer {
                    id: *timeout_timer_id,
                },
                Command::ResolvePendingRequest {
                    request_id,
                    ok: true,
                },
            ]
        );
    }

    #[test]
    fn protocol_core_request_timeout_resolves_pending_request_as_failed() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let commands = core.stop_recording();
        let Some(Command::RegisterPendingRequest { request_id, .. }) = commands.first() else {
            return;
        };
        let request_id = request_id.clone();
        let Some(Command::ScheduleTimer {
            id: timeout_timer_id,
            ..
        }) = commands.get(1)
        else {
            return;
        };

        let timeout_commands = core.on_timer(*timeout_timer_id);

        assert_eq!(
            timeout_commands,
            vec![Command::ResolvePendingRequest {
                request_id,
                ok: false,
            }]
        );
    }

    #[test]
    fn protocol_core_emits_peer_and_recording_updates_from_server_messages() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let peer_info_frame = encode_server_batch(ServerEnvelope::Message(
            ServerMessage::PeerInfo(PeerInfoPayload {
                session_id: String::from("peer-1").into(),
                info: SessionInfo {
                    is_camera_on: Some(true),
                    ..SessionInfo::default()
                },
            }),
        ));
        let peer_left_frame = encode_server_batch(ServerEnvelope::Message(
            ServerMessage::PeerLeft(PeerLeftPayload {
                session_id: String::from("peer-1").into(),
            }),
        ));
        let recording_frame = encode_server_batch(ServerEnvelope::Message(
            ServerMessage::RecordingChange(RecordingStateUpdate {
                state: RecordingState {
                    recording: Some(false),
                    audio: Some(false),
                    transcription: Some(false),
                    video: Some(false),
                },
                stop_code: Some(StopCode::UserRequest),
            }),
        ));
        let broadcast_frame = encode_server_batch(ServerEnvelope::Message(
            ServerMessage::Broadcast(ServerBroadcastPayload {
                sender_id: String::from("peer-2").into(),
                message: json!({ "body": "hello" }),
            }),
        ));

        assert_eq!(
            core.on_ws_message(&peer_info_frame),
            vec![Command::EmitUpdate {
                update: BundleUpdate::SessionInfoChange(
                    [(
                        String::from("peer-1"),
                        SessionInfo {
                            is_camera_on: Some(true),
                            ..SessionInfo::default()
                        }
                    )]
                    .into_iter()
                    .collect(),
                ),
            }]
        );
        assert_eq!(
            core.on_ws_message(&peer_left_frame),
            vec![Command::EmitUpdate {
                update: BundleUpdate::Disconnect(BundleDisconnectUpdate {
                    session_id: String::from("peer-1").into(),
                }),
            }]
        );
        assert_eq!(
            core.on_ws_message(&broadcast_frame),
            vec![Command::EmitUpdate {
                update: BundleUpdate::Broadcast(BundleBroadcastUpdate {
                    sender_id: String::from("peer-2").into(),
                    message: json!({ "body": "hello" }),
                }),
            }]
        );
        assert_eq!(
            core.on_ws_message(&recording_frame),
            vec![Command::EmitUpdate {
                update: BundleUpdate::ChannelInfoChange(RecordingStateUpdate {
                    state: RecordingState {
                        recording: Some(false),
                        audio: Some(false),
                        transcription: Some(false),
                        video: Some(false),
                    },
                    stop_code: Some(StopCode::UserRequest),
                }),
            }]
        );
    }

    #[test]
    fn protocol_core_disconnect_cleans_up_live_session() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.start_recording(RecordingOptions {
            audio: Some(true),
            video: None,
            transcription: None,
        });
        let _ = core.on_timer(BATCH_FLUSH_TIMER_ID);
        let _ = core.on_transport_ready();

        let commands = core.disconnect();

        assert_eq!(core.state(), ConnectionState::Disconnected);
        assert!(commands.contains(&Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        }));
        assert!(commands.contains(&Command::CloseWebSocket { code: 1000 }));
        assert!(commands.contains(&Command::ClosePeerConnection));
        assert!(commands.contains(&Command::EmitStateChange {
            state: ConnectionState::Disconnected,
            cause: None,
        }));
        assert_eq!(
            core.features(),
            &AvailableFeatures {
                rtc: false,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            }
        );
        let recording_state = serde_json::to_value(core.recording_state());
        assert_eq!(recording_state.unwrap_or_default(), json!({}));
    }

    #[test]
    fn protocol_core_non_terminal_close_enters_recovering() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.on_transport_ready();

        let commands = core.on_ws_close(1011);

        assert_eq!(core.state(), ConnectionState::Recovering);
        assert_eq!(
            commands,
            vec![
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: ConnectionState::Recovering,
                    cause: None,
                },
                Command::ScheduleTimer {
                    id: RECOVERY_TIMER_ID,
                    ms: 1_000,
                },
            ]
        );
    }

    #[test]
    fn protocol_core_replays_sticky_intents_after_recovery_authentication() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.on_transport_ready();
        let _ = core.update_upload(StreamType::Camera, true);
        let _ = core.update_download(
            String::from("peer-7").into(),
            DownloadStates {
                audio: Some(true),
                camera: Some(false),
                screen: None,
            },
        );
        let _ = core.update_info(SessionInfo {
            is_camera_on: Some(true),
            is_raising_hand: Some(true),
            ..SessionInfo::default()
        });
        let _ = core.on_ws_close(1011);
        let _ = core.on_timer(RECOVERY_TIMER_ID);

        let commands = core.on_welcome(sample_welcome_payload());
        let envelopes = decode_sent_client_envelopes(&commands);

        assert_eq!(core.state(), ConnectionState::Authenticated);
        assert_eq!(
            commands.first(),
            Some(&Command::EmitStateChange {
                state: ConnectionState::Authenticated,
                cause: None,
            })
        );
        assert_eq!(
            envelopes,
            vec![
                ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
                    stream_type: StreamType::Camera,
                })),
                ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                    session_id: String::from("peer-7").into(),
                    states: DownloadStates {
                        audio: Some(true),
                        camera: Some(false),
                        screen: None,
                    },
                })),
                ClientEnvelope::Message(ClientMessage::Info(SessionInfo {
                    is_camera_on: Some(true),
                    is_raising_hand: Some(true),
                    ..SessionInfo::default()
                })),
            ]
        );
    }

    #[test]
    fn protocol_core_updates_sticky_intents_while_recovering_before_replay() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.on_transport_ready();
        let _ = core.update_upload(StreamType::Camera, true);
        let _ = core.update_download(
            String::from("peer-7").into(),
            DownloadStates {
                audio: Some(true),
                camera: None,
                screen: None,
            },
        );
        let _ = core.on_ws_close(1011);
        let _ = core.update_upload(StreamType::Camera, false);
        let _ = core.update_download(
            String::from("peer-7").into(),
            DownloadStates {
                audio: Some(false),
                camera: Some(true),
                screen: None,
            },
        );
        let _ = core.update_info(SessionInfo {
            is_self_muted: Some(true),
            ..SessionInfo::default()
        });
        let _ = core.on_timer(RECOVERY_TIMER_ID);

        let commands = core.on_welcome(sample_welcome_payload());
        let envelopes = decode_sent_client_envelopes(&commands);

        assert_eq!(
            envelopes,
            vec![
                ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                    session_id: String::from("peer-7").into(),
                    states: DownloadStates {
                        audio: Some(false),
                        camera: Some(true),
                        screen: None,
                    },
                })),
                ClientEnvelope::Message(ClientMessage::Info(SessionInfo {
                    is_self_muted: Some(true),
                    ..SessionInfo::default()
                })),
            ]
        );
    }

    #[test]
    fn protocol_core_recovery_timer_retries_the_saved_url() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.on_transport_ready();
        let _ = core.on_ws_close(1011);

        let commands = core.on_timer(RECOVERY_TIMER_ID);

        assert_eq!(core.state(), ConnectionState::Connecting);
        assert_eq!(
            commands,
            vec![
                Command::EmitStateChange {
                    state: ConnectionState::Connecting,
                    cause: None,
                },
                Command::Connect {
                    url: String::from("wss://sfu.example.com/socket"),
                },
            ]
        );
    }

    #[test]
    fn protocol_core_successful_recovery_resets_backoff_delay() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.on_transport_ready();
        let _ = core.on_ws_close(1011);
        let _ = core.on_timer(RECOVERY_TIMER_ID);
        let _ = core.on_welcome(sample_welcome_payload());

        let commands = core.on_ws_close(1011);

        assert_eq!(
            commands,
            vec![
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: ConnectionState::Recovering,
                    cause: None,
                },
                Command::ScheduleTimer {
                    id: RECOVERY_TIMER_ID,
                    ms: 1_000,
                },
            ]
        );
    }

    #[test]
    fn protocol_core_terminal_close_enters_closed_with_cause() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());

        let commands = core.on_ws_close(4004);

        assert_eq!(core.state(), ConnectionState::Closed);
        assert_eq!(
            commands,
            vec![
                Command::CancelTimer {
                    id: RECOVERY_TIMER_ID,
                },
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: ConnectionState::Closed,
                    cause: Some(String::from("full")),
                },
            ]
        );
    }

    #[test]
    fn protocol_core_rejects_illegal_authenticated_transition() {
        let mut core = ProtocolCore::new();

        let commands = core.on_welcome(sample_welcome_payload());

        assert!(commands.is_empty());
        assert_eq!(core.state(), ConnectionState::Disconnected);
    }

    #[test]
    fn protocol_core_closes_on_invalid_server_batch() {
        let mut core = ProtocolCore::new();

        let commands = core.on_ws_message("{not json");

        assert_eq!(
            commands,
            vec![Command::CloseWebSocket {
                code: u16::from(WebSocketCloseCode::ProtocolError),
            }]
        );
    }

    #[test]
    fn protocol_core_ignores_unknown_or_stale_timers() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

        let commands = core.on_timer(99);

        assert!(commands.is_empty());
        assert_eq!(core.state(), ConnectionState::Connecting);
    }
}
