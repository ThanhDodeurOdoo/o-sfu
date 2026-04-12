use crate::{
    bundle_api::{BundleConnectionState, BundleUpdate},
    shared::{AvailableFeatures, RecordingState, StreamType},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, EnvelopeBatch, WebSocketCloseCode,
        WelcomePayload,
    },
};

pub use crate::bundle_api::BundleConnectionState as ConnectionState;

/// Timer id used by the recovery backoff scheduler.
pub const RECOVERY_TIMER_ID: u32 = 1;
const INITIAL_RECOVERY_DELAY_MS: u32 = 1_000;
const MAX_RECOVERY_DELAY_MS: u32 = 30_000;

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
    ApplyOffer(String),
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
struct ConnectContext {
    url: String,
    jwt: String,
    channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolCore {
    state: ConnectionState,
    features: AvailableFeatures,
    recording_state: RecordingState,
    connect_context: Option<ConnectContext>,
    recovery_delay_ms: u32,
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
            connect_context: None,
            recovery_delay_ms: INITIAL_RECOVERY_DELAY_MS,
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
        let Some(auth_frame) = encode_auth_frame(connect_context) else {
            return Vec::new();
        };
        vec![Command::SendWebSocket(auth_frame)]
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
        self.state = BundleConnectionState::Authenticated;
        vec![Command::EmitStateChange {
            state: self.state,
            cause: None,
        }]
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
        vec![
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::CloseWebSocket {
                code: u16::from(WebSocketCloseCode::Clean),
            },
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: self.state,
                cause: None,
            },
        ]
    }

    pub fn on_ws_close(&mut self, code: u16) -> Vec<Command> {
        if matches!(
            self.state,
            BundleConnectionState::Disconnected | BundleConnectionState::Closed
        ) {
            return Vec::new();
        }
        if let Some(
            WebSocketCloseCode::AuthFailed
            | WebSocketCloseCode::Kicked
            | WebSocketCloseCode::ChannelFull,
        ) = web_socket_close_code(code)
        {
            self.state = BundleConnectionState::Closed;
            self.connect_context = None;
            self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
            vec![
                Command::CancelTimer {
                    id: RECOVERY_TIMER_ID,
                },
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: self.state,
                    cause: close_cause(code).map(str::to_owned),
                },
            ]
        } else {
            let Some(connect_context) = self.connect_context.as_ref() else {
                self.state = BundleConnectionState::Disconnected;
                return vec![Command::EmitStateChange {
                    state: self.state,
                    cause: None,
                }];
            };
            let _ = connect_context;
            let delay_ms = self.recovery_delay_ms;
            self.recovery_delay_ms = next_recovery_delay(delay_ms);
            self.state = BundleConnectionState::Recovering;
            vec![
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: self.state,
                    cause: None,
                },
                Command::ScheduleTimer {
                    id: RECOVERY_TIMER_ID,
                    ms: delay_ms,
                },
            ]
        }
    }

    pub fn on_timer(&mut self, timer_id: u32) -> Vec<Command> {
        if timer_id != RECOVERY_TIMER_ID || self.state != BundleConnectionState::Recovering {
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
}

fn empty_features() -> AvailableFeatures {
    AvailableFeatures {
        rtc: false,
        transcription: false,
        audio_recording: false,
        video_recording: false,
    }
}

fn encode_auth_frame(connect_context: &ConnectContext) -> Option<String> {
    let envelope = ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
        jwt: connect_context.jwt.clone(),
        channel: connect_context.channel.clone(),
    }))
    .into_envelope()
    .ok()?;
    let batch: EnvelopeBatch = vec![envelope];
    serde_json::to_string(&batch).ok()
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

    use super::{Command, ConnectionState, ProtocolCore, RECOVERY_TIMER_ID};
    use crate::{
        shared::{AvailableFeatures, RecordingState, SessionInfo},
        signaling::{
            AuthPayload, ClientEnvelope, ClientMessage, EnvelopeBatch, PeerSnapshot, WelcomePayload,
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
    fn protocol_core_ws_open_sends_auth_frame() {
        let mut core = ProtocolCore::new();
        let _ = core.connect(
            "wss://sfu.example.com/socket",
            "signed-token",
            Some(String::from("channel-1")),
        );

        let commands = core.on_ws_open();

        assert!(matches!(commands.as_slice(), [Command::SendWebSocket(_)]));
        let [Command::SendWebSocket(frame)] = commands.as_slice() else {
            return;
        };
        let batch = serde_json::from_str::<EnvelopeBatch>(frame);
        assert!(batch.is_ok());
        let batch = batch.unwrap_or_default();
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
    fn protocol_core_welcome_transitions_to_authenticated() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

        let commands = core.on_welcome(sample_welcome_payload());

        assert_eq!(core.state(), ConnectionState::Authenticated);
        assert!(core.features().video_recording);
        assert_eq!(core.recording_state().recording, Some(false));
        assert_eq!(
            commands,
            vec![Command::EmitStateChange {
                state: ConnectionState::Authenticated,
                cause: None,
            }]
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
    fn protocol_core_disconnect_cleans_up_live_session() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
        let _ = core.on_welcome(sample_welcome_payload());
        let _ = core.on_transport_ready();

        let commands = core.disconnect();

        assert_eq!(core.state(), ConnectionState::Disconnected);
        assert_eq!(
            commands,
            vec![
                Command::CancelTimer {
                    id: RECOVERY_TIMER_ID,
                },
                Command::CloseWebSocket { code: 1000 },
                Command::ClosePeerConnection,
                Command::EmitStateChange {
                    state: ConnectionState::Disconnected,
                    cause: None,
                },
            ]
        );
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
        assert!(recording_state.is_ok());
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
    fn protocol_core_ignores_unknown_or_stale_timers() {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

        let commands = core.on_timer(99);

        assert!(commands.is_empty());
        assert_eq!(core.state(), ConnectionState::Connecting);
    }
}
