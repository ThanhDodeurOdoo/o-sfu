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
    core::{Command, NegotiationKind, ProtocolCore},
    host_bridge::{HostCommand, HostPendingRequestKind, host_commands},
    shared::{
        AvailableFeatures, DownloadStates as ProtocolDownloadStates, RecordingState,
        RecordingStateUpdate, SessionId as ProtocolSessionId, SessionInfo as ProtocolSessionInfo,
        StopCode as ProtocolStopCode, StreamType as ProtocolStreamType,
    },
    signaling::{RecordingOptions, TrackBinding},
};
use serde_json::json;
use str0m::media::Mid;
use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
    format::{Codec, FormatParams},
    media::Frequency,
};

use super::fixtures::*;
use crate::runtime::test_rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters;
use crate::runtime::{rtc_adapter::DebugRouteEntry, transport_adapter::TransportSessionKey};
use crate::signaling::shared::SessionPermissions;
use o_sfu_router::MediaKind;

const BATCH_FLUSH_DELAY_MS: u32 = 100;
const RECOVERY_DELAY_MS: u32 = 1_000;

struct ProtocolHarnessRtcPeer {
    rtc: Rtc,
}

impl ProtocolHarnessRtcPeer {
    fn new_with_rtc(port: u16, mut rtc: Rtc) -> Option<Self> {
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

#[derive(Clone, Copy)]
struct ProtocolHarnessRtcPeerFactory {
    port: u16,
    build_rtc: fn() -> Rtc,
}

impl ProtocolHarnessRtcPeerFactory {
    fn new(port: u16, build_rtc: fn() -> Rtc) -> Self {
        Self { port, build_rtc }
    }

    fn build_peer(self) -> Option<ProtocolHarnessRtcPeer> {
        ProtocolHarnessRtcPeer::new_with_rtc(self.port, (self.build_rtc)())
    }
}

fn default_protocol_harness_rtc() -> Rtc {
    Rtc::new(Instant::now())
}

#[derive(Debug, Clone)]
struct PendingHarnessNegotiation {
    request_id: RequestId,
    kind: NegotiationKind,
    sdp: String,
}

struct ProtocolHarnessPeer {
    core: ProtocolCore,
    pending_request_commands: Vec<HostCommand>,
    pending_negotiations: VecDeque<PendingHarnessNegotiation>,
    rtc_peer_factory: Option<ProtocolHarnessRtcPeerFactory>,
    rtc_peer: Option<ProtocolHarnessRtcPeer>,
    state_changes: Vec<BundleStateChange>,
    timers: BTreeMap<u32, u32>,
    updates: Vec<BundleUpdate>,
    websocket: Option<TestWebSocket>,
    auto_answer_negotiation: bool,
}

impl Default for ProtocolHarnessPeer {
    fn default() -> Self {
        Self {
            core: ProtocolCore::default(),
            pending_request_commands: Vec::new(),
            pending_negotiations: VecDeque::new(),
            rtc_peer_factory: None,
            rtc_peer: None,
            state_changes: Vec::new(),
            timers: BTreeMap::new(),
            updates: Vec::new(),
            websocket: None,
            auto_answer_negotiation: true,
        }
    }
}

impl ProtocolHarnessPeer {
    fn with_real_rtc_negotiation(port: u16) -> Option<Self> {
        let rtc_peer_factory =
            ProtocolHarnessRtcPeerFactory::new(port, default_protocol_harness_rtc);
        Some(Self {
            rtc_peer_factory: Some(rtc_peer_factory),
            rtc_peer: Some(rtc_peer_factory.build_peer()?),
            ..Self::default()
        })
    }

    fn with_custom_rtc_negotiation(port: u16, build_rtc: fn() -> Rtc) -> Option<Self> {
        let rtc_peer_factory = ProtocolHarnessRtcPeerFactory::new(port, build_rtc);
        Some(Self {
            rtc_peer_factory: Some(rtc_peer_factory),
            rtc_peer: Some(rtc_peer_factory.build_peer()?),
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

    async fn publish(&mut self, stream_type: ProtocolStreamType, active: bool) -> Option<()> {
        let commands = self.core.publish(stream_type, active);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    async fn subscribe(
        &mut self,
        session_id: ProtocolSessionId,
        states: ProtocolDownloadStates,
    ) -> Option<()> {
        let commands = self.core.subscribe(session_id, states);
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

    async fn answer_next_negotiation(&mut self) -> Option<()> {
        let pending = self.pending_negotiations.pop_front()?;
        let answer_sdp = match self.rtc_peer.as_mut() {
            Some(rtc_peer) => rtc_peer.answer_offer(&pending.sdp)?,
            None => String::from("v=0\r\ns=protocol-core-answer\r\n"),
        };
        let mut commands =
            self.core
                .submit_negotiation_answer(&pending.request_id, pending.kind, &answer_sdp);
        commands.extend(self.core.on_transport_ready());
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
                command @ Command::EmitEvent { .. } => {
                    for host_command in host_commands(vec![command]) {
                        if let HostCommand::EmitUpdate { update } = host_command {
                            self.updates.push(update);
                        }
                    }
                    Vec::new()
                }
                Command::CreatePeerConnection => {
                    if let Some(factory) = self.rtc_peer_factory {
                        self.rtc_peer = factory.build_peer();
                        let _ = self.rtc_peer.as_ref()?;
                    }
                    Vec::new()
                }
                Command::ClosePeerConnection => {
                    self.rtc_peer = None;
                    Vec::new()
                }
                Command::AttachTrack { .. } | Command::DetachTrack { .. } => Vec::new(),
                Command::ApplyNegotiation {
                    request_id,
                    kind,
                    sdp,
                } => {
                    if !self.auto_answer_negotiation {
                        self.pending_negotiations
                            .push_back(PendingHarnessNegotiation {
                                request_id,
                                kind,
                                sdp,
                            });
                        continue;
                    }
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
                        .extend(host_commands(vec![command]));
                    Vec::new()
                }
            };
            pending.extend(follow_up);
        }
        Some(())
    }
}

async fn read_next_server_payload(websocket: &mut TestWebSocket) -> Option<String> {
    timeout(Duration::from_secs(1), read_text_message(websocket))
        .await
        .ok()
        .flatten()
}

async fn no_server_frame(peer: &mut ProtocolHarnessPeer, wait: Duration) -> bool {
    let Some(websocket) = peer.websocket.as_mut() else {
        return false;
    };
    timeout(wait, read_text_message(websocket)).await.is_err()
}

async fn read_track_snapshot(peer: &mut ProtocolHarnessPeer) -> Option<Vec<TrackBinding>> {
    let websocket = peer.websocket.as_mut()?;
    let payload = read_next_server_payload(websocket).await?;
    let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
    let messages = protocol_server_messages(&batch)?;
    let ServerMessage::Tracks(track_bindings) = messages.into_iter().next()? else {
        return None;
    };
    let commands = peer.core.on_ws_message(&payload);
    peer.run_commands(commands).await?;
    Some(track_bindings)
}

fn route_has_consumer_activity(
    route_entry: &DebugRouteEntry,
    consumer_session_key: &TransportSessionKey,
    active: bool,
) -> bool {
    route_entry.destinations.iter().any(|destination| {
        destination.dest_session == *consumer_session_key && destination.active == active
    })
}

async fn real_rtc_route_entry(
    server: &TestServer,
    channel: &Arc<Channel>,
    source_session_id: SessionId,
    consumer_session_id: SessionId,
    mid: &str,
) -> Option<(DebugRouteEntry, TransportSessionKey)> {
    let _source_connection_id = channel.session_connection_id(&source_session_id).await?;
    let consumer_connection_id = channel.session_connection_id(&consumer_session_id).await?;
    let consumer_session_key =
        channel.transport_session_key(&consumer_session_id, consumer_connection_id);
    let route_entry = server
        .state
        .transport_adapter
        .debug_route_entry_by_consumer_mid(&consumer_session_key, Mid::from(mid))
        .await?;
    Some((route_entry, consumer_session_key))
}

async fn assert_real_rtc_subscribe_activity(
    bob: &mut ProtocolHarnessPeer,
    server: &TestServer,
    channel: &Arc<Channel>,
    published_track: &TrackBinding,
    source_session_id: SessionId,
    consumer_session_id: SessionId,
    active: bool,
) -> Option<()> {
    bob.subscribe(
        protocol_session_id(&source_session_id),
        ProtocolDownloadStates {
            camera: Some(active),
            ..ProtocolDownloadStates::default()
        },
    )
    .await?;
    if !no_server_frame(bob, Duration::from_millis(150)).await {
        return None;
    }
    let (route_entry, consumer_session_key) = real_rtc_route_entry(
        server,
        channel,
        source_session_id,
        consumer_session_id,
        &published_track.mid,
    )
    .await?;
    if !route_entry.source_active
        || !route_has_consumer_activity(&route_entry, &consumer_session_key, active)
    {
        return None;
    }
    Some(())
}

async fn publish_camera_and_bootstrap_subscriber(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_session_id: &SessionId,
    publish_context: &str,
    renegotiation_context: &str,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    assert!(
        publisher
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "{publish_context}"
    );
    assert!(
        publisher.read_server_frame().await.is_some(),
        "{renegotiation_context}"
    );
    let track_snapshot = read_track_snapshot(subscriber).await;
    assert!(track_snapshot.is_some(), "{snapshot_context}");
    let track_snapshot = track_snapshot?;
    let track_binding = track_snapshot.first()?;
    assert_eq!(
        track_binding.session_id,
        protocol_session_id(publisher_session_id)
    );
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the remote-track renegotiation request"
    );
    Some(track_binding.clone())
}

async fn recover_subscriber_and_replay_track(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_session_id: &SessionId,
    reconnect_context: &str,
    welcome_context: &str,
    offer_context: &str,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    assert!(
        close_peer_and_observe_recovery(subscriber, publisher)
            .await
            .is_some()
    );
    assert!(
        subscriber
            .flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "{reconnect_context}"
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "{welcome_context}"
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "{offer_context}"
    );
    let replayed_track_snapshot = read_track_snapshot(subscriber).await;
    assert!(replayed_track_snapshot.is_some(), "{snapshot_context}");
    let replayed_track_snapshot = replayed_track_snapshot?;
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &protocol_session_id(publisher_session_id),
        ProtocolStreamType::Camera,
    );
    let replayed_track = replayed_track_snapshot.first()?;
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the replayed remote-track renegotiation request"
    );
    Some(replayed_track.clone())
}

async fn setup_fake_protocol_peers(
    adapter: Arc<FakeWebRtcAdapter>,
    channel_name: &str,
    alice_session_id: SessionId,
    bob_session_id: SessionId,
) -> Option<(
    TestServer,
    Arc<Channel>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_test_server_with_timeouts(
        1_000,
        10_000,
        60_000,
        100,
        RuntimeTransportAdapter::from_fake_adapter(adapter),
    )
    .await?;
    let channel = create_channel(&server, channel_name, None, CreateChannelQuery::default()).await;
    let alice_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), alice_session_id.clone())?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), bob_session_id.clone())?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, protocol_session_id(&bob_session_id)).await?;
    Some((server, channel, alice, bob))
}

async fn read_single_protocol_server_message(
    peer: &mut ProtocolHarnessPeer,
) -> Option<ServerMessage> {
    let payload = read_next_server_payload(peer.websocket.as_mut()?).await?;
    let batch = serde_json::from_str::<EnvelopeBatch>(&payload).ok()?;
    let mut messages = protocol_server_messages(&batch)?;
    if messages.len() != 1 {
        return None;
    }
    let message = messages.pop()?;
    let commands = peer.core.on_ws_message(&payload);
    peer.run_commands(commands).await?;
    Some(message)
}

fn protocol_session_id(session_id: &SessionId) -> ProtocolSessionId {
    match session_id {
        SessionId::Integer(value) => ProtocolSessionId::Integer(*value),
        SessionId::String(value) => ProtocolSessionId::String(value.clone()),
    }
}

async fn consume_peer_joined_update(
    peer: &mut ProtocolHarnessPeer,
    session_id: ProtocolSessionId,
) -> Option<()> {
    peer.read_server_frame().await?;
    assert_eq!(
        peer.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&session_id),
            ProtocolSessionInfo::default(),
        )]))),
        "peer join should project into the post-auth session-info update surface"
    );
    Some(())
}

fn assert_track_snapshot_contains(
    track_bindings: &[TrackBinding],
    session_id: &ProtocolSessionId,
    stream_type: ProtocolStreamType,
) {
    assert!(
        track_bindings.iter().any(|binding| {
            binding.session_id == *session_id
                && binding.stream_type == stream_type
                && binding.active
        }),
        "expected an active track binding for session {session_id:?} and stream {stream_type:?}"
    );
}

async fn setup_real_rtc_protocol_peers(
    channel_name: &str,
    alice_session_id: SessionId,
    bob_session_id: SessionId,
    alice_port: u16,
    bob_port: u16,
) -> Option<(
    TestServer,
    Arc<Channel>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_protocol_rtc_test_server(1_000, 100).await?;
    let channel = create_channel(&server, channel_name, None, CreateChannelQuery::default()).await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), alice_session_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), bob_session_id.clone())?;

    let mut alice = ProtocolHarnessPeer::with_real_rtc_negotiation(alice_port)?;
    let mut bob = ProtocolHarnessPeer::with_real_rtc_negotiation(bob_port)?;
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, protocol_session_id(&bob_session_id)).await?;

    Some((server, channel, alice, bob))
}

fn reduced_capability_rtc() -> Rtc {
    let mut config = Rtc::builder().clear_codecs();
    config.codec_config().add_config(
        111.into(),
        None,
        Codec::Opus,
        Frequency::FORTY_EIGHT_KHZ,
        Some(2),
        FormatParams {
            use_inband_fec: Some(true),
            ..Default::default()
        },
    );
    config.codec_config().add_config(
        96.into(),
        None,
        Codec::Vp8,
        Frequency::NINETY_KHZ,
        None,
        FormatParams::default(),
    );
    config.build(Instant::now())
}

fn recording_permissions() -> SessionPermissions {
    SessionPermissions {
        transcription: Some(true),
        audio_recording: Some(true),
        video_recording: Some(true),
    }
}

fn has_resolved_pending_request(
    commands: &[HostCommand],
    request_id: &RequestId,
    ok: bool,
) -> bool {
    commands.contains(&HostCommand::ResolvePendingRequest {
        request_id: request_id.clone(),
        ok,
    })
}

fn has_recording_update(
    updates: &[BundleUpdate],
    state: &RecordingState,
    stop_code: Option<ProtocolStopCode>,
) -> bool {
    updates.iter().any(|update| {
        matches!(
            update,
            BundleUpdate::ChannelInfoChange(RecordingStateUpdate {
                state: update_state,
                stop_code: update_stop_code,
            }) if *update_state == *state && *update_stop_code == stop_code
        )
    })
}

async fn drain_peer_until_recording_update(
    peer: &mut ProtocolHarnessPeer,
    state: &RecordingState,
    stop_code: Option<ProtocolStopCode>,
) -> bool {
    matches!(
        timeout(Duration::from_secs(1), async {
            loop {
                if peer
                    .pending_request_commands
                    .iter()
                    .any(|command| matches!(command, HostCommand::ResolvePendingRequest { .. }))
                    && has_recording_update(&peer.updates, state, stop_code)
                {
                    return Some(());
                }
                peer.read_server_frame().await?;
            }
        })
        .await,
        Ok(Some(()))
    )
}

async fn connect_protocol_recording_peer(
    server: &TestServer,
    channel: &Channel,
) -> Option<ProtocolHarnessPeer> {
    let token = signed_connect_claims_with_permissions(
        TEST_AUTH_KEY,
        channel.uuid(),
        SessionId::Integer(63),
        Some(recording_permissions()),
    )?;
    let mut peer = ProtocolHarnessPeer::default();
    peer.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &token, None)
        .await?;
    Some(peer)
}

fn pending_request_id(
    commands: &[HostCommand],
    request_kind: HostPendingRequestKind,
) -> Option<RequestId> {
    commands.iter().find_map(|command| match command {
        HostCommand::RegisterPendingRequest {
            request_id,
            request_kind: pending_kind,
        } if *pending_kind == request_kind => Some(request_id.clone()),
        _ => None,
    })
}

async fn assert_recording_request_roundtrip(
    peer: &mut ProtocolHarnessPeer,
    request_kind: HostPendingRequestKind,
    stop_code: Option<ProtocolStopCode>,
    expected_state: RecordingState,
) -> Option<RequestId> {
    if !drain_peer_until_recording_update(peer, &expected_state, stop_code).await {
        return None;
    }
    let request_id = pending_request_id(&peer.pending_request_commands, request_kind)?;
    has_resolved_pending_request(&peer.pending_request_commands, &request_id, true)
        .then_some(request_id)
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
async fn protocol_core_answers_real_server_offer_when_enabled() {
    let server = spawn_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol",
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
        "protocol core should connect to the protocol test server"
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "protocol core should consume the welcome frame"
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "protocol core should consume and answer the protocol offer"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Connected);
    assert!(
        peer.state_changes.iter().any(|change| {
            change.state == BundleConnectionState::Connected && change.cause.is_none()
        }),
        "protocol offer handling should drive the protocol core into the connected state"
    );
}

#[tokio::test]
async fn protocol_core_receives_protocol_broadcast_and_peer_updates() {
    let server = spawn_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-events",
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
    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(42))
            .await
            .is_some(),
        "existing peers should consume the protocol peer-joined update after a new session joins"
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
async fn protocol_session_emits_peerjoined_message_for_existing_peers() {
    let server = spawn_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-peerjoined",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(43));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(44));
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

    let Some(alice_websocket) = alice.websocket.as_mut() else {
        return;
    };
    let Some(peer_joined_payload) =
        timeout(Duration::from_secs(1), read_text_message(alice_websocket))
            .await
            .ok()
            .flatten()
    else {
        panic!("existing peer should receive a peerjoined message");
    };
    let peer_joined_batch = serde_json::from_str::<EnvelopeBatch>(&peer_joined_payload).ok();
    assert!(peer_joined_batch.is_some());
    let Some(peer_joined_batch) = peer_joined_batch else {
        return;
    };
    let peer_joined_messages = protocol_server_messages(&peer_joined_batch);
    assert!(peer_joined_messages.is_some());
    let Some(peer_joined_messages) = peer_joined_messages else {
        return;
    };
    assert!(
        matches!(
            peer_joined_messages.as_slice(),
            [ServerMessage::PeerJoined(_)]
        ),
        "existing peers should receive peerjoined rather than a generic peerinfo frame on join"
    );

    let peer_joined_commands = alice.core.on_ws_message(&peer_joined_payload);
    assert!(alice.run_commands(peer_joined_commands).await.is_some());
}

#[tokio::test]
async fn protocol_session_replacement_emits_peerleft_then_peerjoined_for_existing_peers() {
    let server = spawn_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-peer-replacement",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(45));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(46));
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(46))
            .await
            .is_some()
    );
    alice.updates.clear();

    let mut replacement = ProtocolHarnessPeer::default();
    assert!(
        replacement
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
            .await
            .is_some()
    );

    let close_code = read_close_code(match bob.websocket.as_mut() {
        Some(websocket) => websocket,
        None => return,
    })
    .await;
    assert_eq!(close_code, Some(CloseCode::Library(4003)));

    assert!(
        matches!(
            read_single_protocol_server_message(&mut alice).await,
            Some(ServerMessage::PeerLeft(_))
        ),
        "replacement should emit peerleft before rejoin"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::Disconnect(BundleDisconnectUpdate {
            session_id: ProtocolSessionId::Integer(46),
        }))
    );

    assert!(
        matches!(
            read_single_protocol_server_message(&mut alice).await,
            Some(ServerMessage::PeerJoined(_))
        ),
        "replacement should emit peerjoined after peerleft"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(46)),
            ProtocolSessionInfo::default(),
        )]))),
    );
}

#[tokio::test]
async fn protocol_core_receives_translated_track_snapshot_and_explicit_unpublish_removal() {
    let server = spawn_protocol_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-tracks",
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
    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(52))
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
    assert!(producer_id.is_some(), "protocol publisher should be ready");

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
            .publish(ProtocolStreamType::Camera, false)
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
async fn protocol_core_publish_round_trips_through_real_server_session_protocol() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let server = spawn_test_server_with_timeouts(
        1_000,
        10_000,
        60_000,
        100,
        RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&adapter)),
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-publish",
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(54))
            .await
            .is_some()
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
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
        bob.core.track_binding("fake-mid-0"),
        Some(&TrackBinding {
            mid: String::from("fake-mid-0"),
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
            FakeWebRtcEvent::PublishMediaRequested {
                session_id,
                media_kind,
            } if *session_id == SessionId::Integer(53) && *media_kind == MediaKind::Video
        )),
        "protocol publish should declare producer media through the transport adapter"
    );
}

#[tokio::test]
async fn protocol_core_publish_round_trips_through_real_rtc_server_session_protocol() {
    let server = spawn_protocol_rtc_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-rtc-publish",
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(72))
            .await
            .is_some()
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
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
    let track_messages = protocol_server_messages(&track_batch);
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
async fn protocol_handshake_uses_answer_derived_client_capabilities_for_session_state() {
    let server = spawn_protocol_rtc_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-rtc-capabilities",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(75));
    assert!(alice_token.is_some());
    let Some(alice_token) = alice_token else {
        return;
    };
    let Some(mut alice) =
        ProtocolHarnessPeer::with_custom_rtc_negotiation(56_305, reduced_capability_rtc)
    else {
        return;
    };

    assert!(
        alice
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
            .await
            .is_some()
    );

    let parsed_client_rtp_capabilities = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(capabilities) = channel
                .parsed_client_rtp_capabilities(&SessionId::Integer(75))
                .await
            {
                return capabilities;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        parsed_client_rtp_capabilities.is_ok(),
        "protocol handshake should store parsed client RTP capabilities"
    );
    let Some(parsed_client_rtp_capabilities) = parsed_client_rtp_capabilities.ok() else {
        return;
    };
    let codec_names = parsed_client_rtp_capabilities
        .codecs()
        .map(|codec| codec.codec_name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        codec_names,
        vec![
            String::from("opus"),
            String::from("VP8"),
            String::from("rtx"),
        ]
    );
    assert!(
        parsed_client_rtp_capabilities.codecs().any(|codec| {
            codec.codec_name() == "rtx"
                && codec
                    .parameters()
                    .any(|(key, value)| key == "apt" && value == "96")
        }),
        "the stored client RTP capabilities should preserve RTX support from the real RTC answer"
    );
    assert!(
        parsed_client_rtp_capabilities
            .codecs()
            .all(|codec| codec.codec_name() != "H264"),
        "the stored client RTP capabilities must reflect the real RTC answer"
    );
}

#[tokio::test]
async fn protocol_core_publish_queues_follow_up_renegotiation_until_first_answer_lands() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-publish-queue",
        SessionId::Integer(73),
        SessionId::Integer(74),
        56_303,
        56_304,
    ))
    .await
    else {
        return;
    };
    alice.auto_answer_negotiation = false;

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the first rtc-backed renegotiation request"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the first publish should leave one pending negotiation answer in the harness"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Screen, true)
            .await
            .is_some()
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the second publish should queue behind the in-flight negotiation instead of producing a second simultaneous offer"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the first queued negotiation"
    );
    let Some(first_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(first_track_bindings.len(), 1);
    assert_track_snapshot_contains(
        &first_track_bindings,
        &ProtocolSessionId::Integer(73),
        ProtocolStreamType::Camera,
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the first renegotiation request after the initial publish commit"
    );

    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the queued follow-up renegotiation only after the first answer lands"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the queued publish should surface exactly one follow-up negotiation request"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the queued follow-up negotiation"
    );
    let Some(updated_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(updated_track_bindings.len(), 2);
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(73),
        ProtocolStreamType::Camera,
    );
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(73),
        ProtocolStreamType::Screen,
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the follow-up renegotiation request for the queued publish"
    );
}

#[tokio::test]
async fn protocol_core_unpublish_cancels_pending_publish_before_commit() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-publish-cancel",
        SessionId::Integer(75),
        SessionId::Integer(76),
        56_305,
        56_306,
    ))
    .await
    else {
        return;
    };
    alice.auto_answer_negotiation = false;

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the staged publish renegotiation request"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the first publish should leave one pending negotiation answer in the harness"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "canceling the staged publish should not create an overlapping negotiation"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the staged publish negotiation"
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the follow-up renegotiation that removes the canceled publish"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "canceling the staged publish should queue exactly one follow-up removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber should not observe a track snapshot before the canceled publish is removed"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the follow-up removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber should not observe track or renegotiation updates for a publish canceled before commit"
    );
}

#[tokio::test]
async fn protocol_core_unpublish_round_trips_through_real_rtc_after_publish_commit() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-unpublish",
        SessionId::Integer(77),
        SessionId::Integer(78),
        56_307,
        56_308,
    ))
    .await
    else {
        return;
    };

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the initial rtc-backed publish renegotiation"
    );

    let Some(initial_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(initial_track_bindings.len(), 1);
    let Some(published_track) = initial_track_bindings.first() else {
        return;
    };
    assert_eq!(published_track.session_id, ProtocolSessionId::Integer(77));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should answer the follow-up renegotiation for the committed publish"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the rtc-backed unpublish renegotiation"
    );

    let Some(removed_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert!(
        removed_track_bindings.is_empty(),
        "committed unpublish should clear the authoritative track snapshot"
    );
    assert_eq!(
        bob.core.track_binding(&published_track.mid),
        None,
        "committed unpublish should remove the cached track binding"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should answer the rtc-backed renegotiation that removes the remote track"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should also receive the translated peer-info update for the committed unpublish"
    );
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(77)),
            ProtocolSessionInfo::default(),
        )]))),
        "committed unpublish should clear the publisher camera flag in the observable peer info"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "committed unpublish should not leave further rtc follow-up frames queued"
    );
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the regression keeps the full queued-removal rtc flow explicit in one place for reviewability"
)]
async fn protocol_core_unpublish_queues_subscriber_removal_until_in_flight_rtc_answer_lands() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-unpublish-removal-queue",
        SessionId::Integer(79),
        SessionId::Integer(80),
        56_309,
        56_310,
    ))
    .await
    else {
        return;
    };

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the initial rtc-backed publish renegotiation"
    );

    let Some(initial_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(initial_track_bindings.len(), 1);
    assert_track_snapshot_contains(
        &initial_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Camera,
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should answer the first rtc renegotiation so the initial consumer is committed"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Screen, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the second rtc-backed publish renegotiation"
    );

    let Some(updated_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(updated_track_bindings.len(), 2);
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Camera,
    );
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Screen,
    );

    bob.auto_answer_negotiation = false;
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the second rtc renegotiation request while the first consumer is already committed"
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "subscriber should keep the second renegotiation pending until the harness answers it"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the unpublish renegotiation while the subscriber still has the later addition offer pending"
    );
    let Some(removed_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(removed_track_bindings.len(), 1);
    assert_track_snapshot_contains(
        &removed_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Screen,
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "subscriber removal should not create an overlapping renegotiation while the later addition answer is pending"
    );
    let Some(bob_websocket) = bob.websocket.as_mut() else {
        return;
    };
    let Some(peer_info_payload) =
        timeout(Duration::from_millis(150), read_text_message(bob_websocket))
            .await
            .ok()
            .flatten()
    else {
        panic!(
            "subscriber should receive the translated peer-info update for the unpublished track"
        );
    };
    let peer_info_batch = serde_json::from_str::<EnvelopeBatch>(&peer_info_payload).ok();
    assert!(peer_info_batch.is_some());
    let Some(peer_info_batch) = peer_info_batch else {
        return;
    };
    let peer_info_messages = protocol_server_messages(&peer_info_batch);
    assert!(peer_info_messages.is_some());
    let Some(peer_info_messages) = peer_info_messages else {
        return;
    };
    assert!(
        matches!(peer_info_messages.as_slice(), [ServerMessage::PeerInfo(_)]),
        "the frame before the queued removal renegotiation should be the translated peer-info update"
    );
    let peer_info_commands = bob.core.on_ws_message(&peer_info_payload);
    assert!(bob.run_commands(peer_info_commands).await.is_some());
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber removal should stay queued until the in-flight addition answer lands"
    );

    assert!(
        bob.answer_next_negotiation().await.is_some(),
        "subscriber should answer the second renegotiation before the queued removal can flush"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the queued follow-up renegotiation after answering the in-flight addition offer"
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "queued consumer removal should surface exactly one follow-up renegotiation request"
    );

    assert!(
        bob.answer_next_negotiation().await.is_some(),
        "subscriber should answer the queued removal renegotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "consumer removal should not leave further rtc follow-up frames queued"
    );
}

#[tokio::test]
async fn protocol_core_subscribe_updates_consumer_activity() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let server = spawn_test_server_with_timeouts(
        1_000,
        10_000,
        60_000,
        100,
        RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&adapter)),
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-subscribe",
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
    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(62))
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
    assert!(producer_id.is_some(), "protocol publisher should be ready");
    assert!(bob.read_server_frame().await.is_some());
    assert!(bob.read_server_frame().await.is_some());

    assert!(
        bob.subscribe(
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
                    FakeWebRtcEvent::ConsumerActivityUpdated {
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
    assert!(observed, "fake adapter should record subscribe activity");
}

#[tokio::test]
async fn protocol_core_subscribe_updates_real_rtc_consumer_activity() {
    let Some((server, channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-subscribe",
        SessionId::Integer(91),
        SessionId::Integer(92),
        56_311,
        56_312,
    ))
    .await
    else {
        return;
    };

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "publisher should stage the initial protocol publish"
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should consume the rtc-backed renegotiation request and answer it"
    );

    let Some(track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(published_track) = track_bindings.first() else {
        return;
    };
    assert_eq!(published_track.session_id, ProtocolSessionId::Integer(91));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should consume the rtc-backed follow-up renegotiation request"
    );

    assert!(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &channel,
            published_track,
            SessionId::Integer(91),
            SessionId::Integer(92),
            false,
        )
        .await
        .is_some(),
        "subscriber should disable the existing rtc route without extra websocket signaling"
    );
    assert!(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &channel,
            published_track,
            SessionId::Integer(91),
            SessionId::Integer(92),
            true,
        )
        .await
        .is_some(),
        "real rtc route should mark the subscriber destination active again after subscribe(camera=true)"
    );
}

#[tokio::test]
async fn protocol_core_replays_latest_subscribe_after_real_server_recovery() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let alice_session_id = SessionId::Integer(83);
    let bob_session_id = SessionId::Integer(84);
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_fake_protocol_peers(
        Arc::clone(&adapter),
        "issuer-protocol-subscribe-recovery",
        alice_session_id.clone(),
        bob_session_id.clone(),
    ))
    .await
    else {
        return;
    };

    assert!(
        publish_camera_and_bootstrap_subscriber(
            &mut alice,
            &mut bob,
            &alice_session_id,
            "publisher should stage the initial protocol publish",
            "publisher should consume the initial publish renegotiation and answer it",
            "subscriber should receive the initial translated track snapshot",
        )
        .await
        .is_some()
    );

    assert!(
        bob.subscribe(
            protocol_session_id(&alice_session_id),
            ProtocolDownloadStates {
                camera: Some(false),
                ..ProtocolDownloadStates::default()
            },
        )
        .await
        .is_some()
    );
    let baseline_event_count = adapter.snapshot_events().len();

    assert!(
        recover_subscriber_and_replay_track(
            &mut alice,
            &mut bob,
            &alice_session_id,
            "recovery timer should reconnect the subscriber",
            "subscriber should consume the recovery welcome frame",
            "subscriber should consume the recovery initial offer",
            "subscriber should receive a replayed track snapshot after recovery",
        )
        .await
        .is_some()
    );

    let replayed_inactive = timeout(Duration::from_secs(1), async {
        loop {
            if adapter
                .snapshot_events()
                .iter()
                .skip(baseline_event_count)
                .any(|event| {
                    matches!(
                        event,
                        FakeWebRtcEvent::ConsumerActivityUpdated {
                            consumer_session_id,
                            source_session_id,
                            active: false,
                        } if *consumer_session_id == bob_session_id
                            && *source_session_id == alice_session_id
                    )
                })
            {
                return Some(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        matches!(replayed_inactive, Ok(Some(()))),
        "subscriber recovery should replay the latest muted camera subscription"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_replays_latest_subscribe_after_real_rtc_server_recovery() {
    let alice_session_id = SessionId::Integer(93);
    let bob_session_id = SessionId::Integer(94);
    let Some((server, channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-subscribe-recovery",
        alice_session_id.clone(),
        bob_session_id.clone(),
        56_391,
        56_392,
    ))
    .await
    else {
        return;
    };

    let Some(published_track) = publish_camera_and_bootstrap_subscriber(
        &mut alice,
        &mut bob,
        &alice_session_id,
        "publisher should stage the initial protocol publish on the real rtc path",
        "publisher should consume the initial real-rtc publish renegotiation and answer it",
        "subscriber should receive the initial translated track snapshot on the real rtc path",
    )
    .await
    else {
        return;
    };
    assert!(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &channel,
            &published_track,
            alice_session_id.clone(),
            bob_session_id.clone(),
            false,
        )
        .await
        .is_some(),
        "subscriber should mark the initial rtc route inactive before recovery"
    );

    let Some(replayed_track) = recover_subscriber_and_replay_track(
        &mut alice,
        &mut bob,
        &alice_session_id,
        "recovery timer should reconnect the real-rtc subscriber",
        "subscriber should consume the recovery welcome frame on the real rtc path",
        "subscriber should consume the recovery initial offer on the real rtc path",
        "subscriber should receive the replayed track snapshot after recovery on the real rtc path",
    )
    .await
    else {
        return;
    };
    let Some((route_entry, consumer_session_key)) = real_rtc_route_entry(
        &server,
        &channel,
        alice_session_id.clone(),
        bob_session_id.clone(),
        &replayed_track.mid,
    )
    .await
    else {
        panic!("recovered subscriber route should exist");
    };
    assert!(route_entry.source_active);
    assert!(
        route_has_consumer_activity(&route_entry, &consumer_session_key, false),
        "subscriber recovery should replay the latest muted camera subscription on the real rtc path"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_recording_requests_resolve_against_real_server_responses() {
    let server = spawn_test_server_with_feature_flags(
        1_000,
        100,
        RuntimeTransportAdapter::fake_for_testing(),
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(
        &server,
        "issuer-protocol-recording",
        None,
        CreateChannelQuery {
            recording_address: Some("https://record.example.com".to_owned()),
            ..CreateChannelQuery::default()
        },
    )
    .await;
    let mut peer = connect_protocol_recording_peer(&server, &channel).await;
    assert!(peer.is_some());
    let Some(ref mut peer) = peer else {
        return;
    };

    assert!(
        peer.start_recording(Some(true), Some(false), None)
            .await
            .is_some()
    );
    let start_request_id = assert_recording_request_roundtrip(
        peer,
        HostPendingRequestKind::StartRecording,
        None,
        RecordingState {
            recording: Some(true),
            audio: Some(true),
            video: Some(false),
            transcription: Some(false),
        },
    )
    .await;
    assert!(start_request_id.is_some());
    if start_request_id.is_none() {
        return;
    }

    peer.pending_request_commands.clear();
    peer.updates.clear();

    assert!(peer.stop_recording().await.is_some());
    let stop_request_id = assert_recording_request_roundtrip(
        peer,
        HostPendingRequestKind::StopRecording,
        Some(ProtocolStopCode::UserRequest),
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            video: Some(false),
            transcription: Some(false),
        },
    )
    .await;
    assert!(stop_request_id.is_some());
    if stop_request_id.is_none() {
        return;
    }
}

#[tokio::test]
async fn protocol_core_replays_latest_info_after_real_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_protocol_recovery_peers(
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(72))
            .await
            .is_some()
    );
    alice.updates.clear();

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

#[tokio::test]
async fn protocol_core_replays_latest_publish_after_real_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_protocol_recovery_peers(
        SessionId::Integer(81),
        SessionId::Integer(82),
    ))
    .await
    else {
        return;
    };

    assert!(
        bob.publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "publisher should stage the initial protocol publish"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the initial publish renegotiation and answer it"
    );
    let initial_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        initial_track_snapshot.is_some(),
        "subscriber should receive the initial translated track snapshot"
    );
    let Some(initial_track_snapshot) = initial_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &initial_track_snapshot,
        &ProtocolSessionId::Integer(82),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should receive the initial remote-track renegotiation request"
    );
    alice.updates.clear();

    assert!(
        close_peer_and_observe_recovery(&mut bob, &mut alice)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the departure-side renegotiation before recovery rejoin"
    );
    alice.updates.clear();

    assert!(
        bob.flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "recovery timer should reconnect the publisher"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery welcome frame"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery initial offer"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the replayed publish renegotiation after recovery"
    );

    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(82))
            .await
            .is_some()
    );
    let replayed_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        replayed_track_snapshot.is_some(),
        "subscriber should receive a replayed track snapshot after publisher recovery"
    );
    let Some(replayed_track_snapshot) = replayed_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &ProtocolSessionId::Integer(82),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should receive the replayed remote-track renegotiation request"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_replays_latest_publish_after_real_rtc_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-recovery",
        SessionId::Integer(91),
        SessionId::Integer(92),
        55_091,
        55_092,
    ))
    .await
    else {
        return;
    };

    assert!(
        bob.publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "publisher should stage the initial protocol publish on the real rtc path"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the initial real-rtc publish renegotiation and answer it"
    );
    let initial_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        initial_track_snapshot.is_some(),
        "subscriber should receive the initial translated track snapshot on the real rtc path"
    );
    let Some(initial_track_snapshot) = initial_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &initial_track_snapshot,
        &ProtocolSessionId::Integer(92),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the initial real-rtc remote-track renegotiation request"
    );
    alice.updates.clear();

    assert!(
        close_peer_and_observe_recovery(&mut bob, &mut alice)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the departure-side real-rtc renegotiation before recovery rejoin"
    );
    alice.updates.clear();

    assert!(
        bob.flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "recovery timer should reconnect the real-rtc publisher"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery welcome frame on the real rtc path"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery initial offer on the real rtc path"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the replayed real-rtc publish renegotiation after recovery"
    );

    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(92))
            .await
            .is_some()
    );
    let replayed_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        replayed_track_snapshot.is_some(),
        "subscriber should receive a replayed track snapshot after real-rtc publisher recovery"
    );
    let Some(replayed_track_snapshot) = replayed_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &ProtocolSessionId::Integer(92),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the replayed real-rtc remote-track renegotiation request"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

async fn setup_protocol_recovery_peers(
    alice_session_id: SessionId,
    bob_session_id: SessionId,
) -> Option<(
    TestServer,
    Arc<Channel>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_protocol_test_server(1_000, 100).await?;
    let channel = create_channel(
        &server,
        "issuer-protocol-recovery",
        None,
        CreateChannelQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), alice_session_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), bob_session_id.clone())?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, protocol_session_id(&bob_session_id)).await?;
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

fn sample_video_rtp_parameters(mid: &str) -> o_sfu_router::RtpParameters {
    router_sample_video_rtp_parameters(Some(mid), 22_222)
}
