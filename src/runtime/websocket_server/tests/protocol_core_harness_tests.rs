use std::collections::{BTreeMap, VecDeque};

use o_sfu_protocol::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate, BundleStateChange,
        BundleUpdate, bundle_session_info_key,
    },
    core::{Command, ProtocolCore},
    host_bridge::HostCommand,
    shared::{
        AvailableFeatures, DownloadStates as ProtocolDownloadStates, RecordingState,
        SessionId as ProtocolSessionId, SessionInfo as ProtocolSessionInfo,
        StreamType as ProtocolStreamType,
    },
    signaling::TrackBinding,
};
use serde_json::json;

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
        let payload = timeout(
            Duration::from_secs(1),
            read_text_message(self.websocket.as_mut()?),
        )
        .await
        .ok()??;
        let commands = self.core.on_ws_message(&payload);
        self.run_commands(commands).await
    }

    async fn observe_close(&mut self, code: u16) -> Option<()> {
        let commands = self.core.on_ws_close(code);
        self.run_commands(commands).await
    }

    async fn connect_and_finish_handshake(
        &mut self,
        url: &str,
        jwt: &str,
        channel: Option<String>,
    ) -> Option<()> {
        self.connect(url, jwt, channel).await?;
        self.read_server_frame().await?;
        self.read_server_frame().await?;
        Some(())
    }

    async fn broadcast(&mut self, message: serde_json::Value) -> Option<()> {
        let commands = self.core.broadcast(message);
        self.run_commands(commands).await?;
        self.flush_scheduled_timers().await
    }

    async fn update_info(&mut self, info: ProtocolSessionInfo) -> Option<()> {
        let commands = self.core.update_info(info);
        self.run_commands(commands).await?;
        self.flush_scheduled_timers().await
    }

    async fn update_upload(&mut self, stream_type: ProtocolStreamType, active: bool) -> Option<()> {
        let commands = self.core.update_upload(stream_type, active);
        self.run_commands(commands).await?;
        self.flush_scheduled_timers().await
    }

    async fn update_download(
        &mut self,
        session_id: ProtocolSessionId,
        states: ProtocolDownloadStates,
    ) -> Option<()> {
        let commands = self.core.update_download(session_id, states);
        self.run_commands(commands).await?;
        self.flush_scheduled_timers().await
    }

    async fn flush_scheduled_timers(&mut self) -> Option<()> {
        let timer_ids = self.timers.keys().copied().collect::<Vec<_>>();
        for timer_id in timer_ids {
            let commands = self.core.on_timer(timer_id);
            self.run_commands(commands).await?;
        }
        Some(())
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

#[tokio::test]
async fn protocol_core_receives_native_broadcast_and_peer_updates() {
    let server = spawn_native_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-native-events",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(41));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(42));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    assert!(
        alice
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
            .await
            .is_some()
    );
    assert!(
        bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
            .await
            .is_some()
    );
    bob.updates.clear();

    assert!(alice.broadcast(json!({ "text": "hello" })).await.is_some());
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated broadcast"
    );
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::Broadcast(BundleBroadcastUpdate {
            sender_id: ProtocolSessionId::Integer(41),
            message: json!({ "text": "hello" }),
        }))
    );

    assert!(
        alice
            .update_info(ProtocolSessionInfo {
                is_talking: Some(true),
                ..ProtocolSessionInfo::default()
            })
            .await
            .is_some()
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated peer info"
    );
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(41)),
            ProtocolSessionInfo {
                is_talking: Some(true),
                ..ProtocolSessionInfo::default()
            },
        )])))
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "alice should consume its own translated peer info frame before disconnect assertions"
    );

    let close_result = match bob.websocket.as_mut() {
        Some(websocket) => websocket.close(None).await,
        None => return,
    };
    assert!(close_result.is_ok());
    bob.websocket = None;
    sleep(Duration::from_millis(50)).await;

    assert!(
        alice.read_server_frame().await.is_some(),
        "alice should consume translated peer disconnect"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::Disconnect(BundleDisconnectUpdate {
            session_id: ProtocolSessionId::Integer(42),
        }))
    );
}

#[tokio::test]
async fn protocol_core_receives_translated_track_snapshot_and_unpublish_update() {
    let server = spawn_native_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-native-tracks",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(51));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(52));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    assert!(
        alice
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
            .await
            .is_some()
    );
    assert!(
        bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
            .await
            .is_some()
    );

    let producer_id = channel
        .publish_track(
            &SessionId::Integer(51),
            StreamType::Camera,
            MediaKind::Video,
            sample_video_rtp_parameters("cam-0"),
            &server.state.transport_adapter,
        )
        .await;
    assert!(producer_id.is_some(), "native publisher should be ready");

    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated tracks snapshot"
    );
    assert_eq!(
        bob.core.track_binding("cam-0"),
        Some(&TrackBinding {
            mid: String::from("cam-0"),
            session_id: ProtocolSessionId::Integer(51),
            stream_type: ProtocolStreamType::Camera,
            active: true,
        })
    );

    assert!(
        alice
            .update_upload(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated unpublish state"
    );
    assert_eq!(
        bob.core.track_binding("cam-0"),
        Some(&TrackBinding {
            mid: String::from("cam-0"),
            session_id: ProtocolSessionId::Integer(51),
            stream_type: ProtocolStreamType::Camera,
            active: false,
        })
    );
}

#[tokio::test]
async fn protocol_core_native_subscribe_updates_consumer_activity() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let server = spawn_test_server_with_timeouts_and_protocol(
        1_000,
        10_000,
        60_000,
        100,
        RuntimeTransportAdapter::from_stub_adapter(Arc::clone(&adapter)),
        true,
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-native-subscribe",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(61));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(62));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    assert!(
        alice
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
            .await
            .is_some()
    );
    assert!(
        bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
            .await
            .is_some()
    );

    let producer_id = channel
        .publish_track(
            &SessionId::Integer(61),
            StreamType::Camera,
            MediaKind::Video,
            sample_video_rtp_parameters("cam-1"),
            &server.state.transport_adapter,
        )
        .await;
    assert!(producer_id.is_some(), "native publisher should be ready");
    assert!(bob.read_server_frame().await.is_some());

    assert!(
        bob.update_download(
            ProtocolSessionId::Integer(61),
            ProtocolDownloadStates {
                camera: Some(false),
                ..ProtocolDownloadStates::default()
            },
        )
        .await
        .is_some()
    );

    let observed = timeout(Duration::from_secs(1), async {
        loop {
            if adapter.snapshot_events().iter().any(|event| {
                matches!(
                    event,
                    StubWebRtcEvent::ConsumerActivityUpdated {
                        consumer_session_id,
                        source_session_id,
                        active: false,
                    } if *consumer_session_id == SessionId::Integer(62)
                        && *source_session_id == SessionId::Integer(61)
                )
            }) {
                return true;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
    .unwrap_or(false);
    assert!(observed, "stub adapter should record subscribe activity");
}

fn sample_video_rtp_parameters(mid: &str) -> RtpParameters {
    RtpParameters(json!({
        "mid": mid,
        "codecs": [
            {
                "mimeType": "video/VP8",
                "payloadType": 96,
                "clockRate": 90000,
                "parameters": {},
                "rtcpFeedback": [
                    { "type": "nack" },
                    { "type": "nack", "parameter": "pli" },
                    { "type": "ccm", "parameter": "fir" },
                    { "type": "goog-remb" },
                    { "type": "transport-cc" }
                ]
            },
            {
                "mimeType": "video/rtx",
                "payloadType": 97,
                "clockRate": 90000,
                "parameters": { "apt": "96" },
                "rtcpFeedback": []
            }
        ],
        "headerExtensions": [
            { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
            { "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", "id": 4, "encrypt": false },
            { "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01", "id": 5, "encrypt": false }
        ],
        "encodings": [{ "ssrc": 22222 }]
    }))
}
