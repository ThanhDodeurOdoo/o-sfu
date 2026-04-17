#![no_main]

use libfuzzer_sys::fuzz_target;
use o_sfu_protocol::{
    core::{Command, ConnectionState, PendingRequestKind, ProtocolCore},
    shared::{
        AvailableFeatures, RecordingState, RecordingStateUpdate, SessionId, SessionInfo,
        StopCode, StreamType,
    },
    signaling::{
        PeerInfoPayload, PeerLeftPayload, PeerSnapshot, RecordingActionResult, RecordingOptions,
        RequestId, ServerBroadcastPayload, ServerEnvelope, ServerMessage, ServerRequest,
        ServerResponse, SessionDescriptionPayload, TrackBinding, WelcomePayload,
    },
};

use serde_json::Value;

const MAX_STEPS: usize = 24;

#[derive(Debug)]
struct Cursor<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }

    fn read_u8(&mut self) -> Option<u8> {
        let byte = self.bytes.get(self.index).copied()?;
        self.index += 1;
        Some(byte)
    }

    fn read_bool(&mut self) -> bool {
        self.read_u8().is_some_and(|byte| byte & 1 == 0)
    }

    fn read_usize(&mut self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        usize::from(self.read_u8().unwrap_or(0)) % (limit + 1)
    }

    fn read_label(&mut self, prefix: &str, max_len: usize) -> String {
        let len = self.read_usize(max_len);
        let mut value = String::with_capacity(prefix.len() + len);
        value.push_str(prefix);
        for _ in 0..len {
            let byte = self.read_u8().unwrap_or(0);
            let ch = match byte % 36 {
                0..=9 => char::from(b'0' + (byte % 10)),
                value => char::from(b'a' + (value - 10)),
            };
            value.push(ch);
        }
        value
    }

    fn read_session_id(&mut self) -> SessionId {
        if self.read_bool() {
            SessionId::Integer(i64::from(self.read_u8().unwrap_or(0)))
        } else {
            SessionId::String(self.read_label("peer-", 8))
        }
    }

    fn read_stream_type(&mut self) -> StreamType {
        match self.read_u8().unwrap_or(0) % 3 {
            0 => StreamType::Audio,
            1 => StreamType::Camera,
            _ => StreamType::Screen,
        }
    }

    fn read_option_bool(&mut self) -> Option<bool> {
        match self.read_u8().unwrap_or(0) % 3 {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut cursor = Cursor::new(data);
    let mut core = ProtocolCore::new();

    let _ = core.connect(
        format!(
            "wss://{}.example.test/socket",
            cursor.read_label("room-", 12)
        ),
        cursor.read_label("jwt-", 16),
        cursor.read_bool().then(|| cursor.read_label("channel-", 8)),
    );
    let _ = core.on_ws_open();

    if let Some(frame) = initial_handshake_frame(&mut cursor) {
        let commands = core.on_ws_message(&frame);
        process_followups(&mut core, &commands, &mut cursor);
    }

    for _ in 0..MAX_STEPS {
        let Some(opcode) = cursor.read_u8() else {
            break;
        };

        let commands = match opcode % 7 {
            0 => {
                if let Some(frame) = server_batch_frame(&mut cursor, core.state()) {
                    let commands = core.on_ws_message(&frame);
                    process_followups(&mut core, &commands, &mut cursor);
                    commands
                } else {
                    Vec::new()
                }
            }
            1 => core.on_transport_ready(),
            2 => core.on_timer(match cursor.read_u8().unwrap_or(0) % 3 {
                0 => 1,
                1 => 99,
                _ => 4_095,
            }),
            3 => core.on_ws_close(match cursor.read_u8().unwrap_or(0) % 8 {
                0 => 1_000,
                1 => 1_001,
                2 => 1_002,
                3 => 1_011,
                4 => 4_001,
                5 => 4_002,
                6 => 4_003,
                _ => 4_004,
            }),
            4 => core.disconnect(),
            5 => start_recording(&mut core, &mut cursor),
            _ => stop_recording(&mut core, &mut cursor),
        };

        process_followups(&mut core, &commands, &mut cursor);

        if matches!(core.state(), ConnectionState::Disconnected | ConnectionState::Closed)
            && cursor.read_bool()
        {
            let _ = core.connect(
                format!(
                    "wss://{}.example.test/reconnect",
                    cursor.read_label("room-", 12)
                ),
                cursor.read_label("jwt-", 16),
                cursor.read_bool().then(|| cursor.read_label("channel-", 8)),
            );
            let _ = core.on_ws_open();
        }
    }
});

fn initial_handshake_frame(cursor: &mut Cursor<'_>) -> Option<String> {
    let mut batch = vec![ServerMessage::Welcome(sample_welcome_payload(cursor))
        .into_envelope()
        .ok()?];

    let extra_count = cursor.read_usize(2);
    for _ in 0..extra_count {
        batch.push(sample_server_envelope(cursor, ConnectionState::Authenticated).into_envelope().ok()?);
    }

    serde_json::to_string(&batch).ok()
}

fn server_batch_frame(cursor: &mut Cursor<'_>, state: ConnectionState) -> Option<String> {
    let mut batch = Vec::new();
    let count = cursor.read_usize(3) + 1;
    for _ in 0..count {
        batch.push(sample_server_envelope(cursor, state).into_envelope().ok()?);
    }

    serde_json::to_string(&batch).ok()
}

fn sample_server_envelope(cursor: &mut Cursor<'_>, state: ConnectionState) -> ServerEnvelope {
    match state {
        ConnectionState::Authenticated | ConnectionState::Connected => {
            sample_ready_server_envelope(cursor)
        }
        ConnectionState::Connecting | ConnectionState::Recovering => {
            sample_lifecycle_server_envelope(cursor)
        }
        ConnectionState::Disconnected | ConnectionState::Closed => {
            sample_idle_server_envelope(cursor)
        }
    }
}

fn sample_ready_server_envelope(cursor: &mut Cursor<'_>) -> ServerEnvelope {
    match cursor.read_u8().unwrap_or(0) % 8 {
        0 => ServerEnvelope::Message(ServerMessage::Tracks(sample_track_bindings(cursor))),
        1 => ServerEnvelope::Message(ServerMessage::PeerInfo(sample_peer_info(cursor))),
        2 => ServerEnvelope::Message(ServerMessage::PeerJoined(sample_peer_info(cursor))),
        3 => ServerEnvelope::Message(ServerMessage::PeerLeft(PeerLeftPayload {
            session_id: cursor.read_session_id(),
        })),
        4 => ServerEnvelope::Message(ServerMessage::Broadcast(ServerBroadcastPayload {
            sender_id: cursor.read_session_id(),
            message: sample_json_payload(cursor),
        })),
        5 => ServerEnvelope::Message(ServerMessage::RecordingChange(
            RecordingStateUpdate {
                state: sample_recording_state(cursor),
                stop_code: sample_stop_code(cursor),
            },
        )),
        6 => sample_server_request(cursor),
        _ => sample_server_response(cursor),
    }
}

fn sample_lifecycle_server_envelope(cursor: &mut Cursor<'_>) -> ServerEnvelope {
    match cursor.read_u8().unwrap_or(0) % 5 {
        0 => ServerEnvelope::Message(ServerMessage::Tracks(sample_track_bindings(cursor))),
        1 => ServerEnvelope::Message(ServerMessage::PeerInfo(sample_peer_info(cursor))),
        2 => ServerEnvelope::Message(ServerMessage::PeerLeft(PeerLeftPayload {
            session_id: cursor.read_session_id(),
        })),
        3 => ServerEnvelope::Message(ServerMessage::Broadcast(ServerBroadcastPayload {
            sender_id: cursor.read_session_id(),
            message: sample_json_payload(cursor),
        })),
        _ => ServerEnvelope::Message(ServerMessage::RecordingChange(
            RecordingStateUpdate {
                state: sample_recording_state(cursor),
                stop_code: sample_stop_code(cursor),
            },
        )),
    }
}

fn sample_idle_server_envelope(cursor: &mut Cursor<'_>) -> ServerEnvelope {
    match cursor.read_u8().unwrap_or(0) % 3 {
        0 => ServerEnvelope::Message(ServerMessage::Tracks(sample_track_bindings(cursor))),
        1 => ServerEnvelope::Message(ServerMessage::Broadcast(ServerBroadcastPayload {
            sender_id: cursor.read_session_id(),
            message: sample_json_payload(cursor),
        })),
        _ => ServerEnvelope::Message(ServerMessage::RecordingChange(
            RecordingStateUpdate {
                state: sample_recording_state(cursor),
                stop_code: sample_stop_code(cursor),
            },
        )),
    }
}

fn sample_server_request(cursor: &mut Cursor<'_>) -> ServerEnvelope {
    ServerEnvelope::Request {
        request_id: RequestId::new(cursor.read_label("req-", 8)),
        request: match cursor.read_u8().unwrap_or(0) % 3 {
            0 => ServerRequest::Offer(SessionDescriptionPayload {
                sdp: sample_sdp(cursor),
            }),
            1 => ServerRequest::Renegotiate(SessionDescriptionPayload {
                sdp: sample_sdp(cursor),
            }),
            _ => ServerRequest::Ping,
        },
    }
}

fn sample_server_response(cursor: &mut Cursor<'_>) -> ServerEnvelope {
    ServerEnvelope::Response {
        response_to: RequestId::new(cursor.read_label("req-", 8)),
        response: match cursor.read_u8().unwrap_or(0) % 2 {
            0 => ServerResponse::StartRecording(RecordingActionResult {
                ok: cursor.read_bool(),
            }),
            _ => ServerResponse::StopRecording(RecordingActionResult {
                ok: cursor.read_bool(),
            }),
        },
    }
}

fn sample_welcome_payload(cursor: &mut Cursor<'_>) -> WelcomePayload {
    let peers = (0..cursor.read_usize(3))
        .map(|_| PeerSnapshot {
            session_id: cursor.read_session_id(),
            info: sample_session_info(cursor),
        })
        .collect();

    WelcomePayload {
        features: AvailableFeatures {
            rtc: cursor.read_bool(),
            transcription: cursor.read_bool(),
            audio_recording: cursor.read_bool(),
            video_recording: cursor.read_bool(),
        },
        recording: sample_recording_state(cursor),
        peers,
    }
}

fn sample_track_bindings(cursor: &mut Cursor<'_>) -> Vec<TrackBinding> {
    let mut bindings = Vec::new();
    for _ in 0..cursor.read_usize(3) {
        bindings.push(TrackBinding {
            mid: cursor.read_label("mid-", 8),
            session_id: cursor.read_session_id(),
            stream_type: cursor.read_stream_type(),
            active: cursor.read_bool(),
        });
    }
    bindings
}

fn sample_peer_info(cursor: &mut Cursor<'_>) -> PeerInfoPayload {
    PeerInfoPayload {
        session_id: cursor.read_session_id(),
        info: sample_session_info(cursor),
    }
}

fn sample_session_info(cursor: &mut Cursor<'_>) -> SessionInfo {
    SessionInfo {
        is_talking: cursor.read_option_bool(),
        is_featured: cursor.read_option_bool(),
        is_camera_on: cursor.read_option_bool(),
        is_screen_sharing_on: cursor.read_option_bool(),
        is_self_muted: cursor.read_option_bool(),
        is_deaf: cursor.read_option_bool(),
        is_raising_hand: cursor.read_option_bool(),
    }
}

fn sample_recording_state(cursor: &mut Cursor<'_>) -> RecordingState {
    RecordingState {
        recording: cursor.read_option_bool(),
        audio: cursor.read_option_bool(),
        transcription: cursor.read_option_bool(),
        video: cursor.read_option_bool(),
    }
}

fn sample_stop_code(cursor: &mut Cursor<'_>) -> Option<StopCode> {
    match cursor.read_u8().unwrap_or(0) % 6 {
        0 => Some(StopCode::UserRequest),
        1 => Some(StopCode::ChannelClosed),
        2 => Some(StopCode::RecordingTimeout),
        3 => Some(StopCode::RecordingFailed),
        4 => Some(StopCode::DiskSpaceExhausted),
        _ => None,
    }
}

fn sample_sdp(cursor: &mut Cursor<'_>) -> String {
    format!("v=0\r\ns={}\r\n", cursor.read_label("sdp-", 16))
}

fn sample_json_payload(cursor: &mut Cursor<'_>) -> Value {
    match cursor.read_u8().unwrap_or(0) % 4 {
        0 => Value::Bool(cursor.read_bool()),
        1 => Value::Number(serde_json::Number::from(u64::from(cursor.read_u8().unwrap_or(0)))),
        2 => Value::String(cursor.read_label("msg-", 16)),
        _ => serde_json::json!({
            "kind": cursor.read_label("kind-", 8),
            "value": cursor.read_u8().unwrap_or(0),
        }),
    }
}

fn start_recording(core: &mut ProtocolCore, cursor: &mut Cursor<'_>) -> Vec<Command> {
    core.start_recording(RecordingOptions {
        audio: cursor.read_option_bool(),
        video: cursor.read_option_bool(),
        transcription: cursor.read_option_bool(),
    })
}

fn stop_recording(core: &mut ProtocolCore, _cursor: &mut Cursor<'_>) -> Vec<Command> {
    core.stop_recording()
}

fn process_followups(core: &mut ProtocolCore, commands: &[Command], cursor: &mut Cursor<'_>) {
    for command in commands {
        match command {
            Command::ApplyNegotiation {
                request_id,
                kind,
                ..
            } if cursor.read_bool() => {
                let answer = sample_sdp(cursor);
                let _ = core.submit_negotiation_answer(request_id, *kind, answer);
            }
            Command::RegisterPendingRequest { request_id, kind } if cursor.read_bool() => {
                if let Some(frame) = pending_request_response_frame(request_id, *kind, cursor) {
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
    cursor: &mut Cursor<'_>,
) -> Option<String> {
    let response = match kind {
        PendingRequestKind::StartRecording => ServerResponse::StartRecording(
            RecordingActionResult {
                ok: cursor.read_bool(),
            },
        ),
        PendingRequestKind::StopRecording => ServerResponse::StopRecording(
            RecordingActionResult {
                ok: cursor.read_bool(),
            },
        ),
    };

    let envelope = ServerEnvelope::Response {
        response_to: request_id.clone(),
        response,
    }
    .into_envelope()
    .ok()?;

    serde_json::to_string(&vec![envelope]).ok()
}
