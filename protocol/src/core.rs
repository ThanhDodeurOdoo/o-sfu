use std::collections::BTreeMap;

mod connection_lifecycle;
mod outbound_batch;
mod request_flow;
mod request_tracker;
mod server_events;
mod sticky_replay;

use crate::{
    bundle_api::BundleConnectionState,
    shared::{
        AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
        SessionId, SessionInfo, StreamType,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, Envelope,
        EnvelopeBatch, PeerSnapshot, RecordingOptions, RequestId, ServerEnvelope, ServerMessage,
        ServerRequest, ServerResponse, StreamIntentPayload, SubscribePayload, TrackBinding,
        WebSocketCloseCode, WelcomePayload,
    },
};
use outbound_batch::{FlushMode, OutboundBatcher};
use request_tracker::RequestTracker;
use sticky_replay::StickyReplayState;

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
    /// Emit a protocol-domain event for the host projection layer.
    EmitEvent {
        event: ProtocolEvent,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    PeerSnapshot {
        peers: Vec<PeerSnapshot>,
    },
    PeerInfo {
        session_id: SessionId,
        info: SessionInfo,
    },
    PeerLeft {
        session_id: SessionId,
    },
    Broadcast {
        sender_id: SessionId,
        message: JsonPayload,
    },
    RecordingStateChanged {
        state: RecordingStateUpdate,
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
    sticky_replay: StickyReplayState,
    connect_context: Option<ConnectContext>,
    recovery_delay_ms: u32,
    outbound_batch: OutboundBatcher,
    request_tracker: RequestTracker,
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
            sticky_replay: StickyReplayState::new(),
            connect_context: None,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
            outbound_batch: OutboundBatcher::new(),
            request_tracker: RequestTracker::new(),
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
        connection_lifecycle::connect(self, url.into(), jwt.into(), channel)
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
        if !payload.peers.is_empty() {
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::PeerSnapshot {
                    peers: payload.peers,
                },
            });
        }
        commands.extend(self.replay_sticky_state());
        commands
    }

    pub fn on_transport_ready(&mut self) -> Vec<Command> {
        connection_lifecycle::on_transport_ready(self)
    }

    pub fn update_upload(&mut self, stream_type: StreamType, active: bool) -> Vec<Command> {
        self.sticky_replay.set_upload_active(stream_type, active);
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
        self.sticky_replay
            .remember_download_states(&session_id, &states);
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
        self.sticky_replay.remember_info(&info);
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
        request_flow::start_recording(self, options)
    }

    pub fn stop_recording(&mut self) -> Vec<Command> {
        request_flow::stop_recording(self)
    }

    pub fn submit_negotiation_answer(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
        sdp: impl Into<String>,
    ) -> Vec<Command> {
        request_flow::submit_negotiation_answer(self, request_id, kind, sdp.into())
    }

    pub fn disconnect(&mut self) -> Vec<Command> {
        connection_lifecycle::disconnect(self)
    }

    pub fn on_ws_close(&mut self, code: u16) -> Vec<Command> {
        connection_lifecycle::on_ws_close(self, code)
    }

    pub fn on_timer(&mut self, timer_id: u32) -> Vec<Command> {
        if timer_id == RECOVERY_TIMER_ID {
            return self.handle_recovery_timer();
        }
        if timer_id == BATCH_FLUSH_TIMER_ID {
            return self.flush_pending_batch(false);
        }
        if let Some(commands) = self.request_tracker.resolve_timeout(timer_id) {
            return commands;
        }
        Vec::new()
    }

    fn handle_recovery_timer(&mut self) -> Vec<Command> {
        connection_lifecycle::handle_recovery_timer(self)
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
        server_events::handle_server_message(self, message)
    }

    fn handle_server_request(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
    ) -> Vec<Command> {
        request_flow::handle_server_request(self, request_id, request)
    }

    fn handle_server_response(
        &mut self,
        response_to: &RequestId,
        response: ServerResponse,
    ) -> Vec<Command> {
        request_flow::handle_server_response(self, response_to, response)
    }

    fn enqueue_envelope(&mut self, envelope: Envelope, mode: FlushMode) -> Vec<Command> {
        self.outbound_batch.enqueue(envelope, mode)
    }

    fn flush_pending_batch(&mut self, cancel_timer: bool) -> Vec<Command> {
        self.outbound_batch.flush(cancel_timer)
    }

    fn clear_runtime_state(&mut self) {
        self.track_bindings.clear();
        self.outbound_batch.clear();
        self.request_tracker.clear();
        self.pending_negotiation = None;
    }

    fn clear_runtime_state_with_commands(&mut self) -> Vec<Command> {
        let mut commands = self.outbound_batch.clear_with_commands();
        commands.extend(self.request_tracker.clear_with_commands());
        self.track_bindings.clear();
        self.pending_negotiation = None;
        commands
    }

    fn clear_sticky_state(&mut self) {
        self.sticky_replay.clear();
    }

    fn replay_sticky_state(&mut self) -> Vec<Command> {
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(replay_batch) = self.sticky_replay.replay_batch() else {
            return Vec::new();
        };

        self.outbound_batch.extend(replay_batch);
        self.flush_pending_batch(true)
    }

    fn can_send_client_messages(&self) -> bool {
        matches!(
            self.state,
            BundleConnectionState::Authenticated | BundleConnectionState::Connected
        )
    }
}

fn empty_features() -> AvailableFeatures {
    AvailableFeatures {
        rtc: false,
        transcription: false,
        audio_recording: false,
        video_recording: false,
    }
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
mod tests;
