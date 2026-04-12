use std::collections::{BTreeMap, VecDeque};

use o_sfu_protocol::{
    bundle_api::{BundleConnectionState, BundleStateChange, BundleUpdate, bundle_session_info_key},
    core::{Command, ProtocolCore},
    host_bridge::HostCommand,
    shared::{
        AvailableFeatures, RecordingState, SessionId as ProtocolSessionId,
        SessionInfo as ProtocolSessionInfo,
    },
};

use super::fixtures::*;

#[derive(Default)]
struct ProtocolHarnessPeer {
    core: ProtocolCore,
    state_changes: Vec<BundleStateChange>,
    timers: BTreeMap<u32, u32>,
    updates: Vec<BundleUpdate>,
    websocket: Option<TestWebSocket>,
}

impl ProtocolHarnessPeer {
    async fn connect(&mut self, url: &str, jwt: &str, channel: Option<String>) -> Option<()> {
        let commands = self.core.connect(url.to_owned(), jwt.to_owned(), channel);
        self.run_commands(commands).await
    }

    async fn read_server_frame(&mut self) -> Option<()> {
        let payload = read_text_message(self.websocket.as_mut()?).await?;
        let commands = self.core.on_ws_message(&payload);
        self.run_commands(commands).await
    }

    async fn observe_close(&mut self, code: u16) -> Option<()> {
        let commands = self.core.on_ws_close(code);
        self.run_commands(commands).await
    }

    async fn run_commands(&mut self, commands: Vec<Command>) -> Option<()> {
        let mut pending: VecDeque<_> = commands.into();
        while let Some(command) = pending.pop_front() {
            let follow_up = match command {
                Command::Connect { url } => {
                    let websocket = connect_async(url).await.ok()?;
                    self.websocket = Some(websocket.0);
                    self.core.on_ws_open()
                }
                Command::SendWebSocket(frame) => {
                    self.websocket
                        .as_mut()?
                        .send(tungstenite::Message::Text(frame.into()))
                        .await
                        .ok()?;
                    Vec::new()
                }
                Command::EmitStateChange { state, cause } => {
                    self.state_changes.push(BundleStateChange { state, cause });
                    Vec::new()
                }
                command @ Command::EmitEvent { .. } => match HostCommand::from(command) {
                    HostCommand::EmitUpdate { update } => {
                        self.updates.push(update);
                        Vec::new()
                    }
                    _ => return None,
                },
                Command::CreatePeerConnection | Command::ClosePeerConnection => Vec::new(),
                Command::ApplyNegotiation {
                    request_id,
                    kind,
                    sdp: _sdp,
                } => {
                    let mut follow_up = self.core.submit_negotiation_answer(
                        &request_id,
                        kind,
                        "v=0\r\ns=protocol-core-answer\r\n",
                    );
                    follow_up.extend(self.core.on_transport_ready());
                    follow_up
                }
                Command::ScheduleTimer { id, ms } => {
                    self.timers.insert(id, ms);
                    Vec::new()
                }
                Command::CancelTimer { id } => {
                    self.timers.remove(&id);
                    Vec::new()
                }
                Command::CloseWebSocket { .. } => {
                    self.websocket.as_mut()?.close(None).await.ok()?;
                    Vec::new()
                }
                Command::RegisterPendingRequest { .. }
                | Command::ResolvePendingRequest { .. }
                | Command::AttachTrack { .. }
                | Command::DetachTrack { .. } => return None,
            };
            pending.extend(follow_up);
        }
        Some(())
    }
}

#[tokio::test]
async fn protocol_core_replays_real_server_welcome_peer_snapshot() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let existing_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(31));
    let joining_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(32));
    assert!(existing_token.is_some());
    assert!(joining_token.is_some());
    let Some(existing_token) = existing_token else {
        return;
    };
    let Some(joining_token) = joining_token else {
        return;
    };

    let existing_socket = authenticate_with_jwt(&server, &existing_token).await;
    assert!(existing_socket.is_some());
    let Some(mut existing_socket) = existing_socket else {
        return;
    };
    let existing_welcome = read_welcome(&mut existing_socket).await;
    assert!(
        existing_welcome.is_some(),
        "existing peer should complete handshake"
    );

    let mut peer = ProtocolHarnessPeer::default();
    let connected = peer
        .connect(&format!("ws://{}/", server.addr), &joining_token, None)
        .await;
    assert!(
        connected.is_some(),
        "protocol core should connect to test server"
    );
    let read_frame = peer.read_server_frame().await;
    assert!(
        read_frame.is_some(),
        "protocol core should receive the welcome frame"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Authenticated);
    assert_eq!(
        peer.core.features(),
        &AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );
    assert_eq!(
        peer.core.recording_state(),
        &RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert_eq!(
        peer.state_changes,
        vec![
            BundleStateChange {
                state: BundleConnectionState::Connecting,
                cause: None,
            },
            BundleStateChange {
                state: BundleConnectionState::Authenticated,
                cause: None,
            },
        ]
    );
    assert_eq!(
        peer.updates,
        vec![BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(31)),
            ProtocolSessionInfo::default(),
        )]))]
    );
}

#[tokio::test]
async fn protocol_core_maps_real_server_auth_failure_to_closed_state() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

    let mut peer = ProtocolHarnessPeer::default();
    let connected = peer
        .connect(
            &format!("ws://{}/", server.addr),
            "invalid.jwt.payload",
            Some(channel.uuid().to_owned()),
        )
        .await;
    assert!(connected.is_some(), "protocol core should open websocket");

    let close_code = read_close_code(match peer.websocket.as_mut() {
        Some(websocket) => websocket,
        None => return,
    })
    .await;
    assert_eq!(close_code, Some(CloseCode::Library(4001)));

    let observed = peer.observe_close(4001).await;
    assert!(
        observed.is_some(),
        "protocol core should consume the auth failure close code"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Closed);
    assert_eq!(
        peer.state_changes,
        vec![
            BundleStateChange {
                state: BundleConnectionState::Connecting,
                cause: None,
            },
            BundleStateChange {
                state: BundleConnectionState::Closed,
                cause: Some(String::from("auth_failed")),
            },
        ]
    );
    assert!(peer.timers.is_empty());
}

#[tokio::test]
async fn protocol_core_answers_real_server_native_offer_when_enabled() {
    let server = spawn_native_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-native",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(33));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let mut peer = ProtocolHarnessPeer::default();
    let connected = peer
        .connect(&format!("ws://{}/", server.addr), &token, None)
        .await;
    assert!(
        connected.is_some(),
        "protocol core should connect to native test server"
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "protocol core should consume the welcome frame"
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "protocol core should consume and answer the native offer"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Connected);
    assert!(
        peer.state_changes.iter().any(|change| {
            change.state == BundleConnectionState::Connected && change.cause.is_none()
        }),
        "native offer handling should drive the protocol core into the connected state"
    );
}
