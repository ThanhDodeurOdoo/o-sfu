#![no_main]

//! Fuzz target for `ProtocolCore`'s stateful sequencing surface.
//!
//! directly interact with the core so the fuzzer can reach authenticated,
//! connected, recovery, negotiation, and pending-request paths
//!
//! this target does not fuzz arbitrary JSON framing. That boundary is
//! already covered by `protocol_decode.rs`.
//!
//!  guarantees:
//!
//! 1. every run starts with a valid-enough connect/open/welcome handshake;
//! 2. server frames are encoded through the real protocol envelope types; and
//! 3. emitted commands may be followed up with negotiation answers or request
//!    responses so multi-step flows keep progressing.

use libfuzzer_sys::arbitrary;
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Error, Unstructured},
    fuzz_target,
};
use o_sfu_protocol::{
    host::{Command, ConnectionState, PendingRequestKind, ProtocolCore, RECOVERY_TIMER_ID},
    wire::{
        AvailableFeatures, RecordingState, RecordingStateUpdate, UserId, UserInfo, StopCode,
        StreamType,
    },
    wire::{
        PeerInfoPayload, PeerLeftPayload, PeerSnapshot, RecordingActionResult, RecordingOptions,
        RequestId, ServerBroadcastPayload, ServerEnvelope, ServerMessage, ServerRequest,
        ServerResponse, SessionDescriptionPayload, TrackBinding, WebSocketCloseCode,
        WelcomePayload,
    },
};
use serde_json::Value;

const MAX_STEPS: usize = 24;
const MAX_BATCH_EVENTS: usize = 4;
const MAX_HANDSHAKE_EVENTS: usize = 2;
const MAX_PEERS: usize = 3;
const MAX_TRACK_BINDINGS: usize = 3;
const MAX_LABEL_LEN: usize = 16;
const TIMER_ID_BATCH_FLUSH: u32 = 2;
const TIMER_ID_REQUEST_TIMEOUT_BASE: u32 = 10_000;
const INVALID_TIMER_ID: u32 = 65_535;
const CHANNEL_PATH: &str = "/socket";
const ANSWER_SDP: &str = "v=0\r\ns=answer-seq\r\n";
const REQUEST_ID_SEED: &str = "req-seq";
const ALPHANUMERIC: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

#[derive(Debug)]
struct Scenario {
    connect: ConnectInput,
    handshake: HandshakeInput,
    steps: Vec<Step>,
}

impl<'a> Arbitrary<'a> for Scenario {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, Error> {
        Ok(Self {
            connect: u.arbitrary()?,
            handshake: u.arbitrary()?,
            steps: arbitrary_vec(u, MAX_STEPS)?,
        })
    }
}

#[derive(Debug, Arbitrary)]
struct ConnectInput {
    url_room: Label,
    jwt: Label,
    auth_room: Option<Label>,
}

#[derive(Debug)]
struct HandshakeInput {
    welcome: WelcomeInput,
    extra_events: Vec<ServerEventInput>,
    followups: FollowupInput,
}

impl<'a> Arbitrary<'a> for HandshakeInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, Error> {
        Ok(Self {
            welcome: u.arbitrary()?,
            extra_events: arbitrary_vec(u, MAX_HANDSHAKE_EVENTS)?,
            followups: u.arbitrary()?,
        })
    }
}

#[derive(Debug, Arbitrary)]
enum Step {
    ServerBatch {
        batch: BatchInput,
        followups: FollowupInput,
    },
    TransportReady,
    Timer(TimerInput),
    WsClose(CloseInput),
    Disconnect,
    StartRecording {
        options: RecordingOptionsInput,
        followup: PendingRequestFollowup,
    },
    StopRecording {
        followup: PendingRequestFollowup,
    },
    Connect(ConnectInput),
}

#[derive(Debug)]
struct BatchInput {
    events: Vec<ServerEventInput>,
}

impl<'a> Arbitrary<'a> for BatchInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, Error> {
        let len = u.int_in_range(1..=MAX_BATCH_EVENTS)?;
        let mut events = Vec::with_capacity(len);
        for _ in 0..len {
            events.push(u.arbitrary()?);
        }
        Ok(Self { events })
    }
}

#[derive(Debug, Clone, Copy, Arbitrary)]
struct FollowupInput {
    negotiation: NegotiationFollowup,
    pending_request: PendingRequestFollowup,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
enum NegotiationFollowup {
    Ignore,
    Answer,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
enum PendingRequestFollowup {
    Ignore,
    ResolveFailure,
    ResolveSuccess,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
enum TimerInput {
    Recovery,
    BatchFlush,
    RequestTimeout,
    Invalid,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
enum CloseInput {
    Clean,
    Leaving,
    ProtocolError,
    Error,
    AuthFailed,
    AuthTimeout,
    Kicked,
    RoomFull,
}

#[derive(Debug, Clone, Arbitrary)]
enum ServerEventInput {
    Tracks(TrackBindingsInput),
    PeerInfo(PeerInfoInput),
    PeerJoined(PeerInfoInput),
    PeerLeft(SessionIdInput),
    Broadcast(BroadcastInput),
    RecordingChange(RecordingStateUpdateInput),
    Request(ServerRequestInput),
    Response(ServerResponseInput),
}

#[derive(Debug, Clone)]
struct WelcomeInput {
    features: FeatureFlagsInput,
    recording: RecordingStateInput,
    peers: Vec<PeerSnapshotInput>,
}

impl<'a> Arbitrary<'a> for WelcomeInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, Error> {
        Ok(Self {
            features: u.arbitrary()?,
            recording: u.arbitrary()?,
            peers: arbitrary_vec(u, MAX_PEERS)?,
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct FeatureFlagsInput {
    rtc: bool,
    transcription: bool,
    audio_recording: bool,
    video_recording: bool,
}

#[derive(Debug, Clone, Arbitrary)]
struct PeerSnapshotInput {
    user_id: SessionIdInput,
    info: SessionInfoInput,
}

#[derive(Debug, Clone)]
struct TrackBindingsInput {
    bindings: Vec<TrackBindingInput>,
}

impl<'a> Arbitrary<'a> for TrackBindingsInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, Error> {
        Ok(Self {
            bindings: arbitrary_vec(u, MAX_TRACK_BINDINGS)?,
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct TrackBindingInput {
    mid: Label,
    user_id: SessionIdInput,
    stream_type: StreamTypeInput,
    active: bool,
}

#[derive(Debug, Clone, Arbitrary)]
struct PeerInfoInput {
    user_id: SessionIdInput,
    info: SessionInfoInput,
}

#[derive(Debug, Clone, Arbitrary)]
struct BroadcastInput {
    sender_id: SessionIdInput,
    message: JsonPayloadInput,
}

#[derive(Debug, Clone, Arbitrary)]
struct RecordingStateUpdateInput {
    state: RecordingStateInput,
    stop_code: Option<StopCodeInput>,
}

#[derive(Debug, Clone, Arbitrary)]
enum ServerRequestInput {
    Offer(Label),
    Renegotiate(Label),
}

#[derive(Debug, Clone, Arbitrary)]
enum ServerResponseInput {
    StartRecording { ok: bool },
    StopRecording { ok: bool },
}

#[derive(Debug, Clone, Arbitrary)]
enum SessionIdInput {
    Integer(i16),
    String(Label),
}

#[derive(Debug, Clone, Arbitrary)]
enum StreamTypeInput {
    Audio,
    Camera,
    Screen,
}

#[derive(Debug, Clone, Arbitrary)]
struct SessionInfoInput {
    is_talking: Option<bool>,
    is_featured: Option<bool>,
    is_camera_on: Option<bool>,
    is_screen_sharing_on: Option<bool>,
    is_self_muted: Option<bool>,
    is_deaf: Option<bool>,
    is_raising_hand: Option<bool>,
}

#[derive(Debug, Clone, Arbitrary)]
struct RecordingStateInput {
    recording: Option<bool>,
    audio: Option<bool>,
    transcription: Option<bool>,
    video: Option<bool>,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
enum StopCodeInput {
    UserRequest,
    ChannelClosed,
    RecordingTimeout,
    RecordingFailed,
    DiskSpaceExhausted,
}

#[derive(Debug, Clone, Arbitrary)]
enum JsonPayloadInput {
    Bool(bool),
    Number(u8),
    Text(Label),
    Tagged { kind: Label, value: u8 },
}

#[derive(Debug, Clone, Arbitrary)]
struct RecordingOptionsInput {
    audio: Option<bool>,
    video: Option<bool>,
    transcription: Option<bool>,
}

#[derive(Debug, Clone)]
struct Label(String);

impl<'a> Arbitrary<'a> for Label {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, Error> {
        let len = u.int_in_range(0..=MAX_LABEL_LEN)?;
        let mut value = String::with_capacity(len);
        for _ in 0..len {
            let byte = *u.choose(ALPHANUMERIC)?;
            value.push(char::from(byte));
        }
        Ok(Self(value))
    }
}

fuzz_target!(|scenario: Scenario| {
    let mut core = ProtocolCore::new();

    connect_core(&mut core, &scenario.connect);

    if let Some(frame) = initial_handshake_frame(&scenario.handshake) {
        let commands = core.on_ws_message(&frame);
        process_followups(&mut core, &commands, scenario.handshake.followups);
    }

    for step in scenario.steps {
        match step {
            Step::ServerBatch { batch, followups } => {
                if let Some(frame) = batch.frame_for_state(core.state()) {
                    let commands = core.on_ws_message(&frame);
                    process_followups(&mut core, &commands, followups);
                }
            }
            Step::TransportReady => {
                let _ = core.on_transport_ready();
            }
            Step::Timer(timer) => {
                let _ = core.on_timer(timer.id());
            }
            Step::WsClose(close_code) => {
                let _ = core.on_ws_close(close_code.code());
            }
            Step::Disconnect => {
                let _ = core.disconnect();
            }
            Step::StartRecording { options, followup } => {
                let commands = core.start_recording(options.into_protocol());
                process_followups(
                    &mut core,
                    &commands,
                    FollowupInput {
                        negotiation: NegotiationFollowup::Ignore,
                        pending_request: followup,
                    },
                );
            }
            Step::StopRecording { followup } => {
                let commands = core.stop_recording();
                process_followups(
                    &mut core,
                    &commands,
                    FollowupInput {
                        negotiation: NegotiationFollowup::Ignore,
                        pending_request: followup,
                    },
                );
            }
            Step::Connect(connect) => {
                connect_core(&mut core, &connect);
            }
        }
    }
});

fn arbitrary_vec<'a, T: Arbitrary<'a>>(
    u: &mut Unstructured<'a>,
    max_len: usize,
) -> Result<Vec<T>, Error> {
    let len = u.int_in_range(0..=max_len)?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(u.arbitrary()?);
    }
    Ok(values)
}

fn connect_core(core: &mut ProtocolCore, connect: &ConnectInput) {
    let _ = core.connect(
        format!(
            "wss://{}.example.test{CHANNEL_PATH}",
            connect.url_room.prefixed("room-")
        ),
        connect.jwt.prefixed("jwt-"),
        connect
            .auth_room
            .as_ref()
            .map(|room| room.prefixed("room-")),
    );
    let _ = core.on_ws_open();
}

fn initial_handshake_frame(handshake: &HandshakeInput) -> Option<String> {
    let mut batch = vec![
        ServerMessage::Welcome(handshake.welcome.clone().into_protocol())
            .into_envelope()
            .ok()?,
    ];
    for event in &handshake.extra_events {
        batch.push(
            event
                .clone()
                .into_envelope(ConnectionState::Authenticated)?
                .into_envelope()
                .ok()?,
        );
    }
    serde_json::to_string(&batch).ok()
}

impl BatchInput {
    fn frame_for_state(&self, state: ConnectionState) -> Option<String> {
        let mut batch = Vec::new();
        for event in &self.events {
            batch.push(event.clone().into_envelope(state)?.into_envelope().ok()?);
        }
        if batch.is_empty() {
            return None;
        }
        serde_json::to_string(&batch).ok()
    }
}

impl ServerEventInput {
    fn into_envelope(self, state: ConnectionState) -> Option<ServerEnvelope> {
        match self {
            Self::Tracks(bindings) => Some(ServerEnvelope::Message(ServerMessage::Tracks(
                bindings.into_protocol(),
            ))),
            Self::PeerInfo(info)
                if matches!(
                    state,
                    ConnectionState::Connecting
                        | ConnectionState::Recovering
                        | ConnectionState::Authenticated
                        | ConnectionState::Connected
                ) =>
            {
                Some(ServerEnvelope::Message(ServerMessage::PeerInfo(
                    info.into_protocol(),
                )))
            }
            Self::PeerJoined(info)
                if matches!(
                    state,
                    ConnectionState::Authenticated | ConnectionState::Connected
                ) =>
            {
                Some(ServerEnvelope::Message(ServerMessage::PeerJoined(
                    info.into_protocol(),
                )))
            }
            Self::PeerLeft(user_id)
                if matches!(
                    state,
                    ConnectionState::Connecting
                        | ConnectionState::Recovering
                        | ConnectionState::Authenticated
                        | ConnectionState::Connected
                ) =>
            {
                Some(ServerEnvelope::Message(ServerMessage::PeerLeft(
                    PeerLeftPayload {
                        user_id: user_id.into_protocol("peer-"),
                    },
                )))
            }
            Self::Broadcast(broadcast) => Some(ServerEnvelope::Message(ServerMessage::Broadcast(
                broadcast.into_protocol(),
            ))),
            Self::RecordingChange(update) => Some(ServerEnvelope::Message(
                ServerMessage::RecordingChange(update.into_protocol()),
            )),
            Self::Request(request)
                if matches!(
                    state,
                    ConnectionState::Authenticated | ConnectionState::Connected
                ) =>
            {
                Some(ServerEnvelope::Request {
                    request_id: RequestId::new(String::from(REQUEST_ID_SEED)),
                    request: request.into_protocol(),
                })
            }
            Self::Response(response)
                if matches!(
                    state,
                    ConnectionState::Authenticated | ConnectionState::Connected
                ) =>
            {
                Some(ServerEnvelope::Response {
                    response_to: RequestId::new(String::from(REQUEST_ID_SEED)),
                    response: response.into_protocol(),
                })
            }
            _ => None,
        }
    }
}

impl WelcomeInput {
    fn into_protocol(self) -> WelcomePayload {
        WelcomePayload {
            features: self.features.into_protocol(),
            recording: self.recording.into_protocol(),
            peers: self
                .peers
                .into_iter()
                .map(PeerSnapshotInput::into_protocol)
                .collect(),
        }
    }
}

impl FeatureFlagsInput {
    fn into_protocol(self) -> AvailableFeatures {
        AvailableFeatures {
            rtc: self.rtc,
            transcription: self.transcription,
            audio_recording: self.audio_recording,
            video_recording: self.video_recording,
        }
    }
}

impl PeerSnapshotInput {
    fn into_protocol(self) -> PeerSnapshot {
        PeerSnapshot {
            user_id: self.user_id.into_protocol("peer-"),
            info: self.info.into_protocol(),
        }
    }
}

impl TrackBindingsInput {
    fn into_protocol(self) -> Vec<TrackBinding> {
        self.bindings
            .into_iter()
            .map(TrackBindingInput::into_protocol)
            .collect()
    }
}

impl TrackBindingInput {
    fn into_protocol(self) -> TrackBinding {
        TrackBinding {
            mid: self.mid.prefixed("mid-"),
            user_id: self.user_id.into_protocol("peer-"),
            stream_type: self.stream_type.into_protocol(),
            active: self.active,
            source: None,
        }
    }
}

impl PeerInfoInput {
    fn into_protocol(self) -> PeerInfoPayload {
        PeerInfoPayload {
            user_id: self.user_id.into_protocol("peer-"),
            info: self.info.into_protocol(),
        }
    }
}

impl BroadcastInput {
    fn into_protocol(self) -> ServerBroadcastPayload {
        ServerBroadcastPayload {
            sender_id: self.sender_id.into_protocol("peer-"),
            message: self.message.into_protocol(),
        }
    }
}

impl RecordingStateUpdateInput {
    fn into_protocol(self) -> RecordingStateUpdate {
        RecordingStateUpdate {
            state: self.state.into_protocol(),
            stop_code: self.stop_code.map(StopCodeInput::into_protocol),
        }
    }
}

impl ServerRequestInput {
    fn into_protocol(self) -> ServerRequest {
        match self {
            Self::Offer(label) => ServerRequest::Offer(SessionDescriptionPayload {
                sdp: label.into_sdp("offer-"),
                upload_slots: Vec::new(),
            }),
            Self::Renegotiate(label) => ServerRequest::Renegotiate(SessionDescriptionPayload {
                sdp: label.into_sdp("renegotiate-"),
                upload_slots: Vec::new(),
            }),
        }
    }
}

impl ServerResponseInput {
    fn into_protocol(self) -> ServerResponse {
        match self {
            Self::StartRecording { ok } => {
                ServerResponse::StartRecording(RecordingActionResult { ok })
            }
            Self::StopRecording { ok } => {
                ServerResponse::StopRecording(RecordingActionResult { ok })
            }
        }
    }
}

impl SessionIdInput {
    fn into_protocol(self, prefix: &str) -> UserId {
        match self {
            Self::Integer(value) => UserId::Integer(i64::from(value)),
            Self::String(value) => UserId::String(value.prefixed(prefix)),
        }
    }
}

impl StreamTypeInput {
    fn into_protocol(self) -> StreamType {
        match self {
            Self::Audio => StreamType::Audio,
            Self::Camera => StreamType::Camera,
            Self::Screen => StreamType::Screen,
        }
    }
}

impl SessionInfoInput {
    fn into_protocol(self) -> UserInfo {
        UserInfo {
            is_talking: self.is_talking,
            is_featured: self.is_featured,
            is_camera_on: self.is_camera_on,
            is_screen_sharing_on: self.is_screen_sharing_on,
            is_self_muted: self.is_self_muted,
            is_deaf: self.is_deaf,
            is_raising_hand: self.is_raising_hand,
        }
    }
}

impl RecordingStateInput {
    fn into_protocol(self) -> RecordingState {
        RecordingState {
            recording: self.recording,
            audio: self.audio,
            transcription: self.transcription,
            video: self.video,
        }
    }
}

impl StopCodeInput {
    fn into_protocol(self) -> StopCode {
        match self {
            Self::UserRequest => StopCode::UserRequest,
            Self::ChannelClosed => StopCode::ChannelClosed,
            Self::RecordingTimeout => StopCode::RecordingTimeout,
            Self::RecordingFailed => StopCode::RecordingFailed,
            Self::DiskSpaceExhausted => StopCode::DiskSpaceExhausted,
        }
    }
}

impl JsonPayloadInput {
    fn into_protocol(self) -> Value {
        match self {
            Self::Bool(value) => Value::Bool(value),
            Self::Number(value) => Value::Number(serde_json::Number::from(u64::from(value))),
            Self::Text(value) => Value::String(value.prefixed("msg-")),
            Self::Tagged { kind, value } => serde_json::json!({
                "kind": kind.prefixed("kind-"),
                "value": value,
            }),
        }
    }
}

impl RecordingOptionsInput {
    fn into_protocol(self) -> RecordingOptions {
        RecordingOptions {
            audio: self.audio,
            video: self.video,
            transcription: self.transcription,
        }
    }
}

impl Label {
    fn prefixed(&self, prefix: &str) -> String {
        format!("{prefix}{}", self.0)
    }

    fn into_sdp(self, prefix: &str) -> String {
        format!("v=0\r\ns={}\r\n", self.prefixed(prefix))
    }
}

impl TimerInput {
    fn id(self) -> u32 {
        match self {
            Self::Recovery => RECOVERY_TIMER_ID,
            Self::BatchFlush => TIMER_ID_BATCH_FLUSH,
            Self::RequestTimeout => TIMER_ID_REQUEST_TIMEOUT_BASE,
            Self::Invalid => INVALID_TIMER_ID,
        }
    }
}

impl CloseInput {
    fn code(self) -> u16 {
        match self {
            Self::Clean => u16::from(WebSocketCloseCode::Clean),
            Self::Leaving => u16::from(WebSocketCloseCode::Leaving),
            Self::ProtocolError => u16::from(WebSocketCloseCode::ProtocolError),
            Self::Error => u16::from(WebSocketCloseCode::Error),
            Self::AuthFailed => u16::from(WebSocketCloseCode::AuthFailed),
            Self::AuthTimeout => u16::from(WebSocketCloseCode::AuthTimeout),
            Self::Kicked => u16::from(WebSocketCloseCode::Kicked),
            Self::RoomFull => u16::from(WebSocketCloseCode::RoomFull),
        }
    }
}

fn process_followups(core: &mut ProtocolCore, commands: &[Command], followups: FollowupInput) {
    for command in commands {
        match command {
            Command::ApplyNegotiation {
                request_id, kind, ..
            } if matches!(followups.negotiation, NegotiationFollowup::Answer) => {
                let _ = core.submit_negotiation_answer(request_id, *kind, ANSWER_SDP);
            }
            Command::BeginPendingRequest {
                request_id, kind, ..
            }
                if !matches!(followups.pending_request, PendingRequestFollowup::Ignore) =>
            {
                if let Some(frame) =
                    pending_request_response_frame(request_id, *kind, followups.pending_request)
                {
                    let _ = core.on_ws_message(&frame);
                }
            }
            _ => {}
        }
    }
}

fn pending_request_response_frame(
    request_id: &RequestId,
    kind: PendingRequestKind,
    followup: PendingRequestFollowup,
) -> Option<String> {
    let ok = match followup {
        PendingRequestFollowup::Ignore => return None,
        PendingRequestFollowup::ResolveFailure => false,
        PendingRequestFollowup::ResolveSuccess => true,
    };

    let response = match kind {
        PendingRequestKind::StartRecording => {
            ServerResponse::StartRecording(RecordingActionResult { ok })
        }
        PendingRequestKind::StopRecording => {
            ServerResponse::StopRecording(RecordingActionResult { ok })
        }
    };

    let envelope = ServerEnvelope::Response {
        response_to: request_id.clone(),
        response,
    }
    .into_envelope()
    .ok()?;

    serde_json::to_string(&vec![envelope]).ok()
}
