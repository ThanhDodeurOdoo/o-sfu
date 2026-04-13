use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    time::Instant,
};

use o_sfu_protocol::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate, BundleStateChange,
        BundleUpdate, bundle_session_info_key,
    },
    core::{Command, ProtocolCore},
    host_bridge::{HostCommand, HostPendingRequestKind},
    shared::{
        AvailableFeatures, DownloadStates as ProtocolDownloadStates, RecordingState,
        SessionId as ProtocolSessionId, SessionInfo as ProtocolSessionInfo,
        StreamType as ProtocolStreamType,
    },
    signaling::{RecordingOptions, TrackBinding},
};
use serde_json::json;
use str0m::{Candidate, Rtc, change::SdpOffer};

use super::fixtures::*;

const BATCH_FLUSH_DELAY_MS: u32 = 100;
const RECOVERY_DELAY_MS: u32 = 1_000;

struct ProtocolHarnessRtcPeer {
    rtc: Rtc,
}

impl ProtocolHarnessRtcPeer {
    fn new(port: u16) -> Option<Self> {
        let mut rtc = Rtc::new(Instant::now());
        rtc.add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp").ok()?,
        )?;
        Some(Self { rtc })
    }

    fn answer_offer(&mut self, offer_sdp: &str) -> Option<String> {
        let answer = self
            .rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
            .ok()?;
        Some(answer.to_sdp_string())
    }
}

#[derive(Default)]
struct ProtocolHarnessPeer {
    core: ProtocolCore,
    pending_request_commands: Vec<HostCommand>,
    rtc_peer: Option<ProtocolHarnessRtcPeer>,
    state_changes: Vec<BundleStateChange>,
    timers: BTreeMap<u32, u32>,
    updates: Vec<BundleUpdate>,
    websocket: Option<TestWebSocket>,
}

impl ProtocolHarnessPeer {
    fn with_real_rtc_negotiation(port: u16) -> Option<Self> {
        Some(Self {
            rtc_peer: Some(ProtocolHarnessRtcPeer::new(port)?),
            ..Self::default()
        })
    }

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
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn update_info(&mut self, info: ProtocolSessionInfo) -> Option<()> {
        let commands = self.core.update_info(info);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn update_upload(&mut self, stream_type: ProtocolStreamType, active: bool) -> Option<()> {
        let commands = self.core.update_upload(stream_type, active);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn update_download(
        &mut self,
        session_id: ProtocolSessionId,
        states: ProtocolDownloadStates,
    ) -> Option<()> {
        let commands = self.core.update_download(session_id, states);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn start_recording(
        &mut self,
        audio: Option<bool>,
        video: Option<bool>,
        transcription: Option<bool>,
    ) -> Option<()> {
        let commands = self.core.start_recording(RecordingOptions {
            audio,
            video,
            transcription,
        });
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn stop_recording(&mut self) -> Option<()> {
        let commands = self.core.stop_recording();
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn flush_timers_with_delay(&mut self, delay_ms: u32) -> Option<()> {
        let timer_ids = self
            .timers
            .iter()
            .filter_map(|(id, ms)| (*ms == delay_ms).then_some(*id))
            .collect::<Vec<_>>();
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
                Command::CreatePeerConnection
                | Command::ClosePeerConnection
                | Command::AttachTrack { .. }
                | Command::DetachTrack { .. } => Vec::new(),
                Command::ApplyNegotiation {
                    request_id,
                    kind,
                    sdp,
                } => {
                    let answer_sdp = match self.rtc_peer.as_mut() {
                        Some(rtc_peer) => rtc_peer.answer_offer(&sdp)?,
                        None => String::from("v=0\r\ns=protocol-core-answer\r\n"),
                    };
                    let mut follow_up =
                        self.core
                            .submit_negotiation_answer(&request_id, kind, &answer_sdp);
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
                command @ (Command::RegisterPendingRequest { .. }
                | Command::ResolvePendingRequest { .. }) => {
                    self.pending_request_commands
                        .push(HostCommand::from(command));
                    Vec::new()
                }
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
async fn protocol_core_receives_translated_track_snapshot_and_explicit_unpublish_removal() {
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
        bob.read_server_frame().await.is_some(),
        "bob should consume the serialized renegotiation request after track bootstrap"
    );

    assert!(
        alice
            .update_upload(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the removal renegotiation request"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume the translated track-removal snapshot"
    );
    assert_eq!(bob.core.track_binding("cam-0"), None);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the removal renegotiation request"
    );
}

#[tokio::test]
async fn protocol_core_native_publish_round_trips_through_real_server_session_protocol() {
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
        "issuer-native-publish",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(53));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(54));
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

    assert!(
        alice
            .update_upload(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should consume the renegotiation request and answer it"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the translated track snapshot after publish commit"
    );
    assert_eq!(
        bob.core.track_binding("stub-mid-0"),
        Some(&TrackBinding {
            mid: String::from("stub-mid-0"),
            session_id: ProtocolSessionId::Integer(53),
            stream_type: ProtocolStreamType::Camera,
            active: true,
        })
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the follow-up renegotiation request for the new remote track"
    );
    assert!(
        adapter.snapshot_events().iter().any(|event| matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
                session_id,
                media_kind,
            } if *session_id == SessionId::Integer(53) && *media_kind == MediaKind::Video
        )),
        "native publish should declare producer media through the transport adapter"
    );
}

#[tokio::test]
async fn protocol_core_native_publish_round_trips_through_real_rtc_server_session_protocol() {
    let server = spawn_native_protocol_rtc_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-native-rtc-publish",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(71));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(72));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let Some(mut alice) = ProtocolHarnessPeer::with_real_rtc_negotiation(56_301) else {
        return;
    };
    let Some(mut bob) = ProtocolHarnessPeer::with_real_rtc_negotiation(56_302) else {
        return;
    };
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

    assert!(
        alice
            .update_upload(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should consume the rtc-backed renegotiation request and answer it"
    );

    let Some(bob_websocket) = bob.websocket.as_mut() else {
        return;
    };
    let Some(track_snapshot_payload) =
        timeout(Duration::from_secs(1), read_text_message(bob_websocket))
            .await
            .ok()
            .flatten()
    else {
        return;
    };
    let track_batch = serde_json::from_str::<EnvelopeBatch>(&track_snapshot_payload).ok();
    assert!(track_batch.is_some());
    let Some(track_batch) = track_batch else {
        return;
    };
    let track_messages = native_server_messages(&track_batch);
    assert!(track_messages.is_some());
    let Some(track_messages) = track_messages else {
        return;
    };
    let Some(first_track_message) = track_messages.first() else {
        return;
    };
    assert_eq!(track_messages.len(), 1);
    assert!(matches!(first_track_message, ServerMessage::Tracks(_)));
    let Some(ServerMessage::Tracks(track_bindings)) = track_messages.into_iter().next() else {
        return;
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(published_track) = track_bindings.first() else {
        return;
    };
    assert_eq!(published_track.session_id, ProtocolSessionId::Integer(71));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    let track_commands = bob.core.on_ws_message(&track_snapshot_payload);
    assert!(bob.run_commands(track_commands).await.is_some());
    assert_eq!(
        bob.core.track_binding(&published_track.mid),
        Some(published_track)
    );

    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the rtc-backed follow-up renegotiation request"
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

#[tokio::test]
async fn protocol_core_native_recording_requests_resolve_against_real_server_responses() {
    let server = spawn_native_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-native-recording",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(63));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let mut peer = ProtocolHarnessPeer::default();
    assert!(
        peer.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &token, None)
            .await
            .is_some()
    );

    assert!(
        peer.start_recording(Some(true), Some(false), None)
            .await
            .is_some()
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "real server should answer start recording"
    );
    let start_request_id = peer
        .pending_request_commands
        .iter()
        .find_map(|command| match command {
            HostCommand::RegisterPendingRequest {
                request_id,
                request_kind: HostPendingRequestKind::StartRecording,
            } => Some(request_id.clone()),
            _ => None,
        });
    assert!(start_request_id.is_some());
    let Some(start_request_id) = start_request_id else {
        return;
    };
    assert!(
        peer.pending_request_commands
            .contains(&HostCommand::ResolvePendingRequest {
                request_id: start_request_id,
                ok: false,
            }),
        "real server should resolve start recording with its current stubbed response"
    );

    peer.pending_request_commands.clear();

    assert!(peer.stop_recording().await.is_some());
    assert!(
        peer.read_server_frame().await.is_some(),
        "real server should answer stop recording"
    );
    let stop_request_id = peer
        .pending_request_commands
        .iter()
        .find_map(|command| match command {
            HostCommand::RegisterPendingRequest {
                request_id,
                request_kind: HostPendingRequestKind::StopRecording,
            } => Some(request_id.clone()),
            _ => None,
        });
    assert!(stop_request_id.is_some());
    let Some(stop_request_id) = stop_request_id else {
        return;
    };
    assert!(
        peer.pending_request_commands
            .contains(&HostCommand::ResolvePendingRequest {
                request_id: stop_request_id,
                ok: false,
            }),
        "real server should resolve stop recording with its current stubbed response"
    );
}

#[tokio::test]
async fn protocol_core_replays_latest_info_after_real_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_native_recovery_peers(
        SessionId::Integer(71),
        SessionId::Integer(72),
    ))
    .await
    else {
        return;
    };

    assert!(
        bob_update_info_and_deliver(
            &mut bob,
            &mut alice,
            ProtocolSessionInfo {
                is_self_muted: Some(true),
                ..ProtocolSessionInfo::default()
            },
        )
        .await
        .is_some()
    );
    alice.updates.clear();

    assert!(
        close_peer_and_observe_recovery(&mut bob, &mut alice)
            .await
            .is_some()
    );
    alice.updates.clear();

    let latest_info = ProtocolSessionInfo {
        is_self_muted: Some(false),
        is_raising_hand: Some(true),
        ..ProtocolSessionInfo::default()
    };
    assert!(
        recover_peer_with_latest_info(&mut bob, latest_info.clone())
            .await
            .is_some()
    );

    assert!(
        alice.read_server_frame().await.is_some(),
        "alice should receive bob's replayed latest session info after recovery"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(72)),
            latest_info,
        )])))
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

async fn setup_native_recovery_peers(
    alice_session_id: SessionId,
    bob_session_id: SessionId,
) -> Option<(
    TestServer,
    Arc<Channel>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_native_protocol_test_server(1_000, 100).await?;
    let channel = create_channel(
        &server,
        "issuer-native-recovery",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), alice_session_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), bob_session_id)?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    Some((server, channel, alice, bob))
}

async fn bob_update_info_and_deliver(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    alice.read_server_frame().await?;
    Some(())
}

async fn close_peer_and_observe_recovery(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
) -> Option<()> {
    bob.websocket.as_mut()?.close(None).await.ok()?;
    bob.websocket = None;
    bob.observe_close(1011).await?;
    alice.read_server_frame().await?;
    Some(())
}

async fn recover_peer_with_latest_info(
    bob: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    bob.flush_timers_with_delay(RECOVERY_DELAY_MS).await?;
    bob.read_server_frame().await?;
    bob.read_server_frame().await?;
    assert!(bob.websocket.is_some());
    Some(())
}

fn peer_reached_state(peer: &ProtocolHarnessPeer, state: BundleConnectionState) -> bool {
    peer.state_changes
        .iter()
        .any(|change| change.state == state && change.cause.is_none())
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
