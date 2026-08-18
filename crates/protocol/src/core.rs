//! Pure client-side signaling state machine for the `o-sfu` protocol.
//!
//! [`ProtocolCore`] never performs I/O directly. Every public transition returns
//! [`CommandBatch`], an ordered batch of side effects for the host to execute in
//! sequence. The host keeps the actual WebSocket, peer connection and timer
//! integration, then feeds the resulting events back into the core.
//!
//! we have to coordinate transport lifecycle, negotiation, timers and
//! host-visible state changes without hiding side effects inside the state
//! machine itself.
//! Returning commands allow three things that are hard to keep at the same time otherwise:
//!
//! 1. Deterministic tests: transitions can be asserted as pure input/output
//!    steps without a live socket, browser API or async runtime.
//! 2. Runtime independence: the same core can drive wasm, native and test
//!    hosts because it describes *what* must happen, not *how* that host does it.
//! 3. Explicit ordering and cleanup: reconnects, request timeouts and teardown
//!    paths expose every required side effect, which avoids hidden partial work
//!    and makes it obvious when the host still owes a timer cancel or close.
//!
//! The command system is more cumbersome than inlining I/O calls, but that
//! cost is what keeps the protocol verifiable and portable. Browser hosts
//! should consume the commands as [`CommandBatch`] values.

use std::{collections::BTreeMap, mem::replace};

use serde::{Deserialize, Serialize};

mod command_batch;
mod connection_lifecycle;
mod outbound_batch;
mod request_flow;
mod request_tracker;
mod server_events;
mod sticky_replay;
mod timers;

pub use command_batch::CommandBatch;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    pub use super::command_batch::test_support::command_batch;
}
use outbound_batch::{FlushMode, OutboundBatcher};
use request_tracker::RequestTracker;
use sticky_replay::StickyReplayState;
use timers::RequestTimeoutId;

pub use crate::bundle_api::BundleConnectionState as ConnectionState;
use crate::{
    bundle_api::BundleConnectionState,
    shared::{
        AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
        StreamType, UserId, UserInfo,
    },
    signaling::{
        AuthPayload, ClientBroadcastPayload, ClientEnvelope, ClientMessage, Envelope,
        MAX_ENVELOPE_BATCH_LEN, NegotiationUploadSlot, PeerSnapshot, RecordingOptions, RequestId,
        ServerEnvelope, StreamIntentPayload, SubscribePayload, TrackBinding, WelcomePayload,
        decode_envelope_batch,
    },
    wire::ServerMessage,
};

/// host-facing timer id used by the recovery backoff scheduler
pub const RECOVERY_TIMER_ID: u32 = 1;
const BATCH_FLUSH_TIMER_ID: u32 = 2;
const INITIAL_RECOVERY_DELAY_MS: u32 = 1_000;
const MAX_RECOVERY_DELAY_MS: u32 = 30_000;
const BATCH_FLUSH_DELAY_MS: u32 = 100;
const REQUEST_TIMEOUT_MS: u32 = 5_000;
const MAX_OUTBOUND_BATCH_LEN: usize = 16;

/// One side-effect intent emitted by [`ProtocolCore`].
///
/// The state machine itself is pure: it never touches I/O. Instead each
/// transition returns [`CommandBatch`] that the host (wasm glue, native driver,
/// test harness) must execute in order. That keeps transport work, timers, and
/// projection updates visible at the protocol boundary instead of being buried
/// in host-specific control flow.
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
    BeginPendingRequest {
        request: PendingRequest,
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

pub(crate) type Commands = Vec<Command>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    PeerSnapshot {
        peers: Vec<PeerSnapshot>,
    },
    TrackSnapshot {
        bindings: Vec<TrackBinding>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NegotiationKind {
    Offer,
    Renegotiate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PendingRequestKind {
    StartRecording,
    StopRecording,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingRequest {
    pub request_id: RequestId,
    pub kind: PendingRequestKind,
    pub timeout_timer_id: u32,
    pub timeout_ms: u32,
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProtocolPhase {
    Disconnected,
    Connecting,
    Authenticated(NegotiationSlot),
    Connected(NegotiationSlot),
    Recovering,
    Closed,
}

impl ProtocolPhase {
    const fn connection_state(&self) -> ConnectionState {
        match self {
            Self::Disconnected => BundleConnectionState::Disconnected,
            Self::Connecting => BundleConnectionState::Connecting,
            Self::Authenticated(_) => BundleConnectionState::Authenticated,
            Self::Connected(_) => BundleConnectionState::Connected,
            Self::Recovering => BundleConnectionState::Recovering,
            Self::Closed => BundleConnectionState::Closed,
        }
    }

    const fn is_awaiting_welcome(&self) -> bool {
        matches!(self, Self::Connecting | Self::Recovering)
    }

    fn apply_lifecycle_state(&mut self, state: ConnectionState) {
        if self.connection_state() == state {
            return;
        }
        let current = replace(self, Self::Disconnected);
        *self = match (current, state) {
            (Self::Authenticated(slot), BundleConnectionState::Connected) => Self::Connected(slot),
            (_, BundleConnectionState::Disconnected) => Self::Disconnected,
            (_, BundleConnectionState::Connecting) => Self::Connecting,
            (_, BundleConnectionState::Authenticated) => Self::Authenticated(NegotiationSlot::Idle),
            (_, BundleConnectionState::Connected) => Self::Connected(NegotiationSlot::Idle),
            (_, BundleConnectionState::Recovering) => Self::Recovering,
            (_, BundleConnectionState::Closed) => Self::Closed,
        };
    }

    const fn can_send_client_messages(&self) -> bool {
        matches!(self, Self::Authenticated(_) | Self::Connected(_))
    }

    const fn can_enter_connected(&self) -> bool {
        matches!(self, Self::Authenticated(NegotiationSlot::Idle))
    }

    fn accept_negotiation(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
    ) -> Result<(), NegotiationRejection> {
        match (self, kind) {
            (Self::Authenticated(slot), NegotiationKind::Offer)
            | (Self::Connected(slot), NegotiationKind::Renegotiate) => {
                slot.accept(request_id, kind)
            }
            (Self::Authenticated(_), NegotiationKind::Renegotiate)
            | (Self::Connected(_), NegotiationKind::Offer) => {
                Err(NegotiationRejection::ProtocolError)
            }
            (
                Self::Disconnected | Self::Connecting | Self::Recovering | Self::Closed,
                NegotiationKind::Offer | NegotiationKind::Renegotiate,
            ) => Err(NegotiationRejection::Ignored),
        }
    }

    fn resolve_negotiation(&mut self, request_id: &RequestId, kind: NegotiationKind) -> bool {
        match self {
            Self::Authenticated(slot) | Self::Connected(slot) => slot.resolve(request_id, kind),
            Self::Disconnected | Self::Connecting | Self::Recovering | Self::Closed => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NegotiationSlot {
    Idle,
    WaitingForAnswer(PendingNegotiation),
}

impl NegotiationSlot {
    fn accept(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
    ) -> Result<(), NegotiationRejection> {
        match self {
            Self::Idle => {
                *self = Self::WaitingForAnswer(PendingNegotiation {
                    request_id: request_id.clone(),
                    kind,
                });
                Ok(())
            }
            Self::WaitingForAnswer(_) => Err(NegotiationRejection::ProtocolError),
        }
    }

    fn resolve(&mut self, request_id: &RequestId, kind: NegotiationKind) -> bool {
        let Self::WaitingForAnswer(pending) = self else {
            return false;
        };
        if pending.request_id != *request_id || pending.kind != kind {
            return false;
        }
        *self = Self::Idle;
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NegotiationRejection {
    Ignored,
    ProtocolError,
}

/// The stored state falls into three groups:
///   - session snapshots accepted from the server
///   - remembered client intent that should survive reconnects
///   - in-flight host work that must be cancelled or resolved during cleanup
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolCore {
    /// Lifecycle and server-driven negotiation state.
    phase: ProtocolPhase,
    /// Feature snapshot from the last accepted welcome payload.
    ///
    /// Until a welcome is accepted this remains empty. It is reset on fresh
    /// connects and terminal cleanup so callers never read capabilities from a
    /// previous room or credential context.
    features: AvailableFeatures,
    /// Recording snapshot from the server.
    ///
    /// Welcome payloads make this authoritative after authentication. Later
    /// recording-change messages update it incrementally, while fresh connects
    /// and terminal cleanup reset it back to the neutral default.
    recording_state: RecordingState,
    /// Current server-maintained mapping from SDP mid to stream binding metadata.
    ///
    /// The map is replaced by track snapshots and trimmed when peers leave. It
    /// is runtime state only and is cleared on disconnect or socket loss.
    track_bindings: BTreeMap<String, TrackBinding>,
    /// Latest client intent that must be replayed after a recovered socket is
    /// authenticated.
    ///
    /// Publication, subscription and local user-info updates are kept here
    /// because they describe what the user still wants. One-off broadcasts and
    /// request-response operations are not sticky because replaying them later
    /// would change their meaning.
    sticky_replay: StickyReplayState,
    /// Saved admission context for the active connection attempt.
    ///
    /// Recovery reuses this URL, JWT and optional room to open the next socket.
    /// Explicit disconnects, terminal close codes and fresh connects clear or
    /// replace it so old credentials cannot revive a stopped session.
    connect_context: Option<ConnectContext>,
    /// Delay that will be used for the next recovery retry.
    ///
    /// The value is reset after a successful welcome or intentional lifecycle
    /// reset. Transient websocket loss consumes the current value when
    /// scheduling recovery, then increases it for the following retry.
    recovery_delay_ms: u32,
    /// Buffered outbound envelopes waiting for an immediate flush, size limit
    /// or batch timer.
    ///
    /// The batcher owns only serializable protocol envelopes and the knowledge
    /// that a flush timer is pending. The host still owns the actual timer and
    /// websocket write side effects emitted as commands.
    outbound_batch: OutboundBatcher,
    /// Tracks request-response operations that must resolve exactly once.
    ///
    /// Each live request is paired with one timeout timer. Responses and timer
    /// callbacks both flow through this tracker so stale, mismatched or racing
    /// events cannot resolve the wrong host promise.
    request_tracker: RequestTracker,
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
            phase: ProtocolPhase::Disconnected,
            features: empty_features(),
            recording_state: RecordingState::default(),
            track_bindings: BTreeMap::new(),
            sticky_replay: StickyReplayState::new(),
            connect_context: None,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
            outbound_batch: OutboundBatcher::new(),
            request_tracker: RequestTracker::new(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> ConnectionState {
        self.phase.connection_state()
    }

    #[must_use]
    pub const fn features(&self) -> &AvailableFeatures {
        &self.features
    }

    #[must_use]
    pub const fn recording_state(&self) -> &RecordingState {
        &self.recording_state
    }

    /// Starts a fresh connection attempt when the current state permits one.
    ///
    /// Accepts [`ConnectionState::Disconnected`], [`ConnectionState::Closed`] and
    /// [`ConnectionState::Recovering`]. Calls from [`ConnectionState::Connecting`],
    /// [`ConnectionState::Authenticated`] and [`ConnectionState::Connected`] return
    /// an empty [`CommandBatch`] without replacing the saved admission context.
    ///
    /// This is stricter than a reconnect path: it clears sticky
    /// replay and runtime state so a caller switching rooms or credentials cannot
    /// accidentally leak the previous user intent into the new connection.
    pub fn connect(
        &mut self,
        url: impl Into<String>,
        jwt: impl Into<String>,
        room: Option<String>,
    ) -> CommandBatch {
        command_batch(connection_lifecycle::connect(
            self,
            url.into(),
            jwt.into(),
            room,
        ))
    }

    /// Authenticates a newly opened socket with the stored connect context.
    ///
    /// Recovery reuses the same JWT and optional room that [`ProtocolCore::connect`] captured,
    /// which keeps every socket attempt tied to one explicit admission context.
    pub fn on_ws_open(&mut self) -> CommandBatch {
        if !matches!(
            self.phase.connection_state(),
            BundleConnectionState::Connecting | BundleConnectionState::Recovering
        ) {
            return CommandBatch::default();
        }
        let Some(connect_context) = self.connect_context.as_ref() else {
            return CommandBatch::default();
        };
        command_batch(self.enqueue_client_message(
            ClientMessage::Auth(AuthPayload {
                jwt: connect_context.jwt.clone(),
                channel: connect_context.room.clone(),
            }),
            FlushMode::Immediate,
        ))
    }

    /// handle ws message
    ///
    /// Malformed batches or envelopes are treated as protocol violations.
    /// The whole batch is decoded before any envelope is applied so partially
    /// applied server state cannot survive after a later decode error.
    pub fn on_ws_message(&mut self, frame: &str) -> CommandBatch {
        let Ok(batch) = decode_envelope_batch(frame, MAX_ENVELOPE_BATCH_LEN) else {
            return CommandBatch::close_for_protocol_error();
        };
        let Ok(envelopes) = batch
            .into_iter()
            .map(ServerEnvelope::decode)
            .collect::<Result<Vec<_>, _>>()
        else {
            return CommandBatch::close_for_protocol_error();
        };
        let mut commands = Vec::new();
        for envelope in envelopes {
            match envelope {
                ServerEnvelope::Message(message) => {
                    if self.phase.is_awaiting_welcome()
                        && !matches!(message, ServerMessage::Welcome(_))
                    {
                        return CommandBatch::close_for_protocol_error();
                    }
                    commands.extend(server_events::handle_server_message(self, message));
                }
                ServerEnvelope::Request {
                    request_id,
                    request,
                } => {
                    commands.extend(request_flow::handle_server_request(
                        self, request_id, request,
                    ));
                }
                ServerEnvelope::Response {
                    response_to,
                    response,
                } => {
                    commands.extend(request_flow::handle_server_response(
                        self,
                        &response_to,
                        response,
                    ));
                }
            }
        }
        command_batch(commands)
    }

    fn accept_welcome(&mut self, payload: WelcomePayload) -> Commands {
        if !matches!(
            self.phase.connection_state(),
            BundleConnectionState::Connecting | BundleConnectionState::Recovering
        ) {
            return Vec::new();
        }
        self.features = payload.features;
        self.recording_state = payload.recording;
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.phase
            .apply_lifecycle_state(BundleConnectionState::Authenticated);

        let mut commands = vec![Command::EmitStateChange {
            state: self.phase.connection_state(),
            cause: None,
        }];
        if !payload.peers.is_empty() {
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::PeerSnapshot {
                    peers: payload.peers,
                },
            });
        }
        commands.extend(self.replay_session_state());
        commands
    }

    /// Marks the local transport layer as ready after the initial negotiation.
    ///
    /// The host should call this only once the peer connection is usable for
    /// media, because it is what upgrades the core from authenticated signaling
    /// state to a fully connected user.
    pub fn on_transport_ready(&mut self) -> CommandBatch {
        if !self.phase.can_enter_connected() {
            return CommandBatch::default();
        }
        self.phase
            .apply_lifecycle_state(BundleConnectionState::Connected);
        let mut commands = vec![Command::EmitStateChange {
            state: self.state(),
            cause: None,
        }];
        commands.extend(self.replay_publication_state());
        command_batch(commands)
    }

    /// Stores the desired publication state and sends it when the media transport is ready.
    ///
    /// Publish intent is sticky across reconnects, which lets UI toggles be issued
    /// before authentication completes without losing the latest desired state.
    pub fn publish(&mut self, stream_type: StreamType, active: bool) -> CommandBatch {
        self.sticky_replay.set_publish_active(stream_type, active);
        if !matches!(&self.phase, ProtocolPhase::Connected(_)) {
            return CommandBatch::default();
        }
        let message = if active {
            ClientMessage::Publish(StreamIntentPayload { stream_type })
        } else {
            ClientMessage::Unpublish(StreamIntentPayload { stream_type })
        };
        command_batch(self.enqueue_client_message(message, FlushMode::Batched))
    }

    /// Remembers the latest per-peer subscription intent for reconnect replay.
    ///
    /// Repeated updates merge at the sticky layer, so callers can send partial
    /// audio/camera/screen adjustments without rebuilding the full preference set
    /// on every change or after recovery.
    pub fn subscribe(&mut self, user_id: UserId, states: DownloadStates) -> CommandBatch {
        self.sticky_replay
            .remember_subscription_states(&user_id, &states);
        if !self.can_send_client_messages() {
            return CommandBatch::default();
        }
        command_batch(self.enqueue_client_message(
            ClientMessage::Subscribe(SubscribePayload { user_id, states }),
            FlushMode::Batched,
        ))
    }

    /// Persists the laetst local user metadata patch for the current room.
    ///
    /// User info is replayed after reconnect so transient transport failures do
    /// not silently reset presence indicators such as mute, hand raise, or camera
    /// state back to server defaults.
    pub fn update_info(&mut self, info: UserInfo) -> CommandBatch {
        self.sticky_replay.remember_info(&info);
        if !self.can_send_client_messages() {
            return CommandBatch::default();
        }
        command_batch(self.enqueue_client_message(ClientMessage::Info(info), FlushMode::Batched))
    }

    /// Sends a best-effort broadcast to the current room.
    ///
    /// Broadcast payloads are not sticky: if the client is not yet
    /// authenticated, the message is dropped instead of being replayed later out
    /// of its original conversational contexte.
    pub fn broadcast(&mut self, message: JsonPayload) -> CommandBatch {
        if !self.can_send_client_messages() {
            return CommandBatch::default();
        }
        command_batch(self.enqueue_client_message(
            ClientMessage::Broadcast(ClientBroadcastPayload { message }),
            FlushMode::Batched,
        ))
    }

    pub fn start_recording(&mut self, options: RecordingOptions) -> CommandBatch {
        request_flow::start_recording(self, options)
    }
    pub fn stop_recording(&mut self) -> CommandBatch {
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
    ) -> CommandBatch {
        command_batch(request_flow::submit_negotiation_answer(
            self, request_id, kind, sdp,
        ))
    }

    pub fn disconnect(&mut self) -> CommandBatch {
        command_batch(connection_lifecycle::disconnect(self))
    }

    pub fn on_ws_close(&mut self, code: u16) -> CommandBatch {
        command_batch(connection_lifecycle::on_ws_close(self, code))
    }

    /// Dispatches all timer callbacks through one entry point.
    ///
    /// Timer ids are part of the protocol-core contract: recovery, outbound batch
    /// flushing, and request timeouts each reserve their own namespace and must be
    /// routed back here by the host in the order they fire.
    pub fn on_timer(&mut self, timer_id: u32) -> CommandBatch {
        if timer_id == RECOVERY_TIMER_ID {
            return command_batch(connection_lifecycle::handle_recovery_timer(self));
        }
        if timer_id == BATCH_FLUSH_TIMER_ID {
            return command_batch(self.flush_pending_batch(false));
        }
        if let Some(commands) = RequestTimeoutId::try_from_raw(timer_id)
            .and_then(|timeout_id| self.request_tracker.resolve_timeout(timeout_id))
        {
            return command_batch(commands);
        }
        CommandBatch::default()
    }

    fn enqueue_envelope(&mut self, envelope: Envelope, mode: FlushMode) -> Commands {
        self.outbound_batch.enqueue(envelope, mode)
    }

    fn enqueue_client_message(&mut self, message: ClientMessage, mode: FlushMode) -> Commands {
        let Some(envelope) = ClientEnvelope::Message(message).into_envelope().ok() else {
            return Vec::new();
        };
        self.enqueue_envelope(envelope, mode)
    }

    fn flush_pending_batch(&mut self, cancel_timer: bool) -> Commands {
        self.outbound_batch.flush(cancel_timer)
    }

    fn clear_runtime_state(&mut self) {
        self.track_bindings.clear();
        self.outbound_batch.clear();
        self.request_tracker.clear();
    }

    /// Tears down runtime state while emitting the cleanup commands the host still owes.
    ///
    /// This is used on disconnect and terminal close paths where queued batches,
    /// timeout timers, and pending requests must be cancelled explicitly instead of
    /// being forgotten inside the pure state machine.
    fn clear_runtime_state_with_commands(&mut self) -> Commands {
        let mut commands = self.outbound_batch.clear_with_commands();
        commands.extend(self.request_tracker.clear_with_commands());
        if !self.track_bindings.is_empty() {
            self.track_bindings.clear();
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: Vec::new(),
                },
            });
        }
        commands
    }

    fn clear_sticky_state(&mut self) {
        self.sticky_replay.clear();
    }

    /// Flushes room-level intent immediately after the server snapshot is known.
    fn replay_session_state(&mut self) -> Commands {
        if !self.can_send_client_messages() {
            return Vec::new();
        }
        let Some(replay_batch) = self.sticky_replay.replay_session_batch() else {
            return Vec::new();
        };

        self.outbound_batch.extend(replay_batch);
        self.flush_pending_batch(true)
    }

    /// Flushes publish intent after the recovered media transport is ready.
    fn replay_publication_state(&mut self) -> Commands {
        if !self.can_send_client_messages() {
            return Vec::new();
        }

        let mut replay_batch = Vec::new();
        for stream_type in self.sticky_replay.active_publications() {
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
        if replay_batch.is_empty() {
            return Vec::new();
        }

        self.outbound_batch.extend(replay_batch);
        self.flush_pending_batch(true)
    }

    fn can_send_client_messages(&self) -> bool {
        self.phase.can_send_client_messages()
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

fn command_batch(commands: Commands) -> CommandBatch {
    CommandBatch::from_core_commands(commands)
}

/// Grows reconnect delay by 1.5x while keeping the backoff bounded.
///
/// The sequence is modest so short-lived outages recover quickly,
/// but repeated failures still spread out retries and avoid hot-loop reconnects.
fn next_recovery_delay(current_delay_ms: u32) -> u32 {
    current_delay_ms
        .saturating_mul(3)
        .checked_div(2)
        .unwrap_or(MAX_RECOVERY_DELAY_MS)
        .min(MAX_RECOVERY_DELAY_MS)
}

#[cfg(test)]
#[path = "core/TESTS/mod.rs"]
mod tests;
