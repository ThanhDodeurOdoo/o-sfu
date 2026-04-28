// TODO: needs documentation:
//! Pure client-side signaling state machine for the `o-sfu` protocol.
//!
//! `ProtocolCore` never perform I/O directly. Every public transition returns
//! [`Commands`], an ordered batch of side effects for the host to execute in
//! sequence. The host owns the actual WebSocket, peer-connection, and timer
//! integration, then feeds the resulting events back into the core.
//!
//! we have to coordinate transport lifecycle, negotiation, timers,
//! and host-visible state changes without hiding side effects inside the state machine itself.
//! Returning commands allow three things that are hard to keep at the same time otherwise:
//!
//! 1. Deterministic tests: transitions can be asserted as pure input/output
//!    steps without a live socket, browser API, or async runtime.
//! 2. Runtime independence: the same core can drive wasm, native, and test
//!    hosts because it describes *what* must happen, not *how* that host does it.
//! 3. Explicit ordering and cleanup: reconnects, request timeouts, and teardown
//!    paths expose every required side effect, which avoids hidden partial work
//!    and makes it obvious when the host still owes a timer cancel or close.
//!
//! The command system is more cumbersome than inlining I/O calls, but that
//! cost is what keeps the protocol verifiable and portable. Browser hosts
//! should consume the commands through the TypeScript runtime contract wrapper,
//! which validates the highest-risk batch ordering before the runtime executes
//! them:
//!
//! - an initial offer must create the peer connection immediately before
//!   applying the remote description;
//! - a renegotiation offer must not recreate the peer connection;
//! - explicit disconnect cleanup closes the websocket before the peer
//!   connection when both effects are emitted together;
//! - recovery scheduling happens only after the peer connection has been
//!   closed for that socket loss.

use std::collections::BTreeMap;

mod connection_lifecycle;
mod outbound_batch;
mod request_flow;
mod request_tracker;
mod server_events;
mod sticky_replay;
#[cfg(feature = "verification-models")]
pub mod verification;

use outbound_batch::{FlushMode, OutboundBatcher};
use request_tracker::RequestTracker;
use sticky_replay::StickyReplayState;

pub use crate::bundle_api::BundleConnectionState as ConnectionState;
use crate::{
    bundle_api::BundleConnectionState,
    shared::{
        AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
        StreamType, UserId, UserInfo,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, Envelope,
        EnvelopeBatch, NegotiationUploadSlot, PeerSnapshot, RecordingOptions, RequestId,
        ServerEnvelope, ServerMessage, ServerRequest, ServerResponse, SourceDescriptor,
        StreamIntentPayload, SubscribePayload, TrackBinding, WebSocketCloseCode, WelcomePayload,
    },
};

/// Timer id used by the recovery backoff scheduler.
pub const RECOVERY_TIMER_ID: u32 = 1;
const BATCH_FLUSH_TIMER_ID: u32 = 2;
const REQUEST_TIMEOUT_TIMER_ID_BASE: u32 = 10_000;
const INITIAL_RECOVERY_DELAY_MS: u32 = 1_000;
const MAX_RECOVERY_DELAY_MS: u32 = 30_000;
const BATCH_FLUSH_DELAY_MS: u32 = 100;
const REQUEST_TIMEOUT_MS: u32 = 5_000;
const MAX_OUTBOUND_BATCH_LEN: usize = 16;

/// One side-effect intent emitted by [`ProtocolCore`].
///
/// The state machine itself is pure: it never touches I/O. Instead each
/// transition returns [`Commands`] that the host (wasm glue, native driver,
/// test harness) must execute in order. That keeps transport work, timers, and
/// projection updates visible at the protocol boundary instead of being buried
/// in host-specific control flow.
// TODO: needs documentation:
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Serialize and send a JSON frame over the WebSocket.
    SendWebSocket(String),
    /// Apply a remote SDP offer to the local `RTCPeerConnection`.
    ApplyNegotiation {
        request_id: RequestId,
        kind: NegotiationKind,
        sdp: String,
        upload_slots: Vec<NegotiationUploadSlot>,
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

/// Ordered side effects emitted by one protocol-core transition.
///
/// A command batch is the full host work produced by applying one input to the
/// protocol state machine. The host must execute the commands in order before
/// feeding any resulting transport or timer events back into [`ProtocolCore`].
pub type Commands = Vec<Command>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    PeerSnapshot {
        peers: Vec<PeerSnapshot>,
    },
    TrackSnapshot {
        bindings: Vec<TrackBinding>,
    },
    SourceSnapshot {
        sources: Vec<SourceDescriptor>,
    },
    PeerInfo {
        user_id: UserId,
        info: UserInfo,
    },
    PeerLeft {
        user_id: UserId,
    },
    Broadcast {
        sender_id: UserId,
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
    room: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingNegotiation {
    request_id: RequestId,
    kind: NegotiationKind,
}

// TODO: needs documentation:
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
    /// Builds a fresh protocol state machine with no remembered user intent.
    ///
    /// Reconnect replay is opt-in through the mutating APIs below, so a new
    /// core starts from a fully fresh state instead of assuming any previous room,
    /// publication, or subscription state.
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

    /// Starts a fresh connection attempt and replaces any earlier user context.
    ///
    /// This is intentionally stricter than a reconnect path: it clears sticky
    /// replay and runtime state so a caller switching rooms or credentials cannot
    /// accidentally leak the previous user intent into the new connection.
    pub fn connect(
        &mut self,
        url: impl Into<String>,
        jwt: impl Into<String>,
        room: Option<String>,
    ) -> Commands {
        connection_lifecycle::connect(self, url.into(), jwt.into(), room)
    }

    /// Authenticates a newly opened socket with the stored connect context.
    ///
    /// Recovery reuses the same JWT and optional room that `connect` captured,
    /// which keeps every socket attempt tied to one explicit admission context.
    pub fn on_ws_open(&mut self) -> Commands {
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
            channel: connect_context.room.clone(),
        }))
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Immediate)
    }

    /// handle ws message
    ///
    /// Malformed batches or envelopes are treated as protocol violations and
    /// close the socket immediately so partially-applied server state cannot
    /// survive atfer a framing error.
    pub fn on_ws_message(&mut self, frame: &str) -> Commands {
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

    /// Commits the authenticated server snapshot and "replays" client wanted state
    ///
    /// The welcome payload is the point where feature flags and recording state
    /// become authoritative again after a reconnect. Only after that snapshot is
    /// accepted do we replay remembered uploads, downloads, and user info.
    pub fn on_welcome(&mut self, payload: WelcomePayload) -> Commands {
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

    /// Marks the local transport layer as ready after the initial negotiation.
    ///
    /// The host should call this only once the peer connection is usable for
    /// media, because it is what upgrades the core from authenticated signaling
    /// state to a fully connected user.
    pub fn on_transport_ready(&mut self) -> Commands {
        connection_lifecycle::on_transport_ready(self)
    }

    /// Stores the desired publication state and sends it when signaling is ready.
    ///
    /// Publish intent is sticky across reconnects, which lets UI toggles be issued
    /// before authentication completes without losing the latest desired state.
    pub fn publish(&mut self, stream_type: StreamType, active: bool) -> Commands {
        self.sticky_replay.set_publish_active(stream_type, active);
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

    /// Remembers the latest per-peer subscription intent for reconnect replay.
    ///
    /// Repeated updates merge at the sticky layer, so callers can send partial
    /// audio/camera/screen adjustments without rebuilding the full preference set
    /// on every change or after recovery.
    pub fn subscribe(&mut self, user_id: UserId, states: DownloadStates) -> Commands {
        self.sticky_replay
            .remember_subscription_states(&user_id, &states);
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(envelope) = ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
            user_id,
            states,
        }))
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, FlushMode::Batched)
    }

    /// Persists the laetst local user metadata patch for the current room.
    ///
    /// User info is replayed after reconnect so transient transport failures do
    /// not silently reset presence indicators such as mute, hand raise, or camera
    /// state back to server defaults.
    pub fn update_info(&mut self, info: UserInfo) -> Commands {
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

    /// Sends a best-effort broadcast to the current room.
    ///
    /// Broadcast payloads are intentionally not sticky: if the client is not yet
    /// authenticated, the message is dropped instead of being replayed later out
    /// of its original conversational contexte.
    pub fn broadcast(&mut self, message: JsonPayload) -> Commands {
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

    pub fn start_recording(&mut self, options: RecordingOptions) -> Commands {
        request_flow::start_recording(self, options)
    }
    pub fn stop_recording(&mut self) -> Commands {
        request_flow::stop_recording(self)
    }

    /// Replies to the currently pending negotiation request.
    ///
    /// The host must echo the exact `request_id` and `kind` from
    /// [`Command::ApplyNegotiation`]; mismatches are ignored so a stale or
    /// reordered SDP answer cannot accidentally resolve the wrong negotiation.
    pub fn submit_negotiation_answer(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
        sdp: impl Into<String>,
    ) -> Commands {
        request_flow::submit_negotiation_answer(self, request_id, kind, sdp.into())
    }

    pub fn disconnect(&mut self) -> Commands {
        connection_lifecycle::disconnect(self)
    }

    pub fn on_ws_close(&mut self, code: u16) -> Commands {
        connection_lifecycle::on_ws_close(self, code)
    }

    /// Dispatches all timer callbacks through one entry point.
    ///
    /// Timer ids are part of the protocol-core contract: recovery, outbound batch
    /// flushing, and request timeouts each reserve their own namespace and must be
    /// routed back here by the host in the order they fire.
    pub fn on_timer(&mut self, timer_id: u32) -> Commands {
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

    fn handle_recovery_timer(&mut self) -> Commands {
        connection_lifecycle::handle_recovery_timer(self)
    }

    fn handle_server_envelope(&mut self, envelope: ServerEnvelope) -> Commands {
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

    fn handle_server_message(&mut self, message: ServerMessage) -> Commands {
        server_events::handle_server_message(self, message)
    }

    fn handle_server_request(&mut self, request_id: RequestId, request: ServerRequest) -> Commands {
        request_flow::handle_server_request(self, request_id, request)
    }

    fn handle_server_response(
        &mut self,
        response_to: &RequestId,
        response: ServerResponse,
    ) -> Commands {
        request_flow::handle_server_response(self, response_to, response)
    }

    fn enqueue_envelope(&mut self, envelope: Envelope, mode: FlushMode) -> Commands {
        self.outbound_batch.enqueue(envelope, mode)
    }

    fn flush_pending_batch(&mut self, cancel_timer: bool) -> Commands {
        self.outbound_batch.flush(cancel_timer)
    }

    fn clear_runtime_state(&mut self) {
        self.track_bindings.clear();
        self.outbound_batch.clear();
        self.request_tracker.clear();
        self.pending_negotiation = None;
    }

    /// Tears down runtime state while emitting the cleanup commands the host still owes.
    ///
    /// This is used on disconnect and terminal close paths where queued batches,
    /// timeout timers, and pending requests must be cancelled explicitly instead of
    /// being forgotten inside the pure state machine.
    fn clear_runtime_state_with_commands(&mut self) -> Commands {
        let mut commands = self.outbound_batch.clear_with_commands();
        commands.extend(self.request_tracker.clear_with_commands());
        let had_source_descriptors = self
            .track_bindings
            .values()
            .any(|binding| binding.source.is_some());
        if !self.track_bindings.is_empty() {
            self.track_bindings.clear();
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: Vec::new(),
                },
            });
        }
        if had_source_descriptors {
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::SourceSnapshot {
                    sources: Vec::new(),
                },
            });
        }
        self.pending_negotiation = None;
        commands
    }

    fn clear_sticky_state(&mut self) {
        self.sticky_replay.clear();
    }

    /// Flushes remembered client intent immediately after the server snapshot is known.
    ///
    /// Replayed envelopes bypass the normal batching delay so recovery converges on
    /// the last desired publish/subscribe/info state before new incremental updates
    /// start to accumulate again.
    fn replay_sticky_state(&mut self) -> Commands {
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

fn protocol_error_commands() -> Commands {
    vec![Command::CloseWebSocket {
        code: u16::from(WebSocketCloseCode::ProtocolError),
    }]
}

/// Grows reconnect delay by 1.5x while keeping the backoff bounded.
///
/// The sequence is intentionally modest so short-lived outages recover quickly,
/// but repeated failures still spread out retries and avoid hot-loop reconnects.
fn next_recovery_delay(current_delay_ms: u32) -> u32 {
    current_delay_ms
        .saturating_mul(3)
        .checked_div(2)
        .unwrap_or(MAX_RECOVERY_DELAY_MS)
        .min(MAX_RECOVERY_DELAY_MS)
}

#[cfg(test)]
mod tests;
