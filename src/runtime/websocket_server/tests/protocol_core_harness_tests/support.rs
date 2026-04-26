pub(super) use std::{collections::BTreeMap, sync::Arc, time::Duration};
use std::{collections::VecDeque, net::SocketAddr, time::Instant};

pub(super) use o_sfu_protocol::{
    bundle_api::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate, BundleStateChange,
        BundleUpdate, bundle_session_info_key,
    },
    host_bridge::HostPendingRequestKind,
    shared::{
        AvailableFeatures, DownloadStates as ProtocolDownloadStates, RecordingState,
        StopCode as ProtocolStopCode, StreamType as ProtocolStreamType,
        UserId as ProtocolSessionId, UserInfo as ProtocolSessionInfo,
    },
    signaling::{EnvelopeBatch, RequestId, ServerMessage, TrackBinding},
};
use o_sfu_protocol::{
    core::{Command, NegotiationKind, ProtocolCore},
    host_bridge::{HostCommand, host_commands},
    shared::{RecordingStateUpdate, UserPermissions},
    signaling::RecordingOptions,
};
pub(super) use o_sfu_router::MediaKind;
use o_sfu_router::test_sample::sample_video_rtp_parameters as router_sample_video_rtp_parameters;
pub(super) use serde_json::json;
use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
    format::{Codec, FormatParams},
    media::{Frequency, Mid},
};
pub(super) use tokio::time::{sleep, timeout};
pub(super) use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

pub(super) use super::super::fixtures::*;
pub(super) use crate::{
    config::RuntimeFeatureFlags,
    runtime::{room::Room, transport_adapter::RuntimeTransportAdapter},
};

pub(super) const BATCH_FLUSH_DELAY_MS: u32 = 100;
pub(super) const RECOVERY_DELAY_MS: u32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RealRtcRouteActivity {
    pub(super) source_active: bool,
    pub(super) consumer_active: bool,
}

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
pub(super) struct PendingHarnessNegotiation {
    request_id: RequestId,
    kind: NegotiationKind,
    sdp: String,
}

pub(super) struct ProtocolHarnessPeer {
    pub(super) core: ProtocolCore,
    pub(super) pending_request_commands: Vec<HostCommand>,
    pub(super) pending_negotiations: VecDeque<PendingHarnessNegotiation>,
    rtc_peer_factory: Option<ProtocolHarnessRtcPeerFactory>,
    rtc_peer: Option<ProtocolHarnessRtcPeer>,
    pub(super) state_changes: Vec<BundleStateChange>,
    pub(super) timers: BTreeMap<u32, u32>,
    pub(super) updates: Vec<BundleUpdate>,
    pub(super) websocket: Option<TestWebSocket>,
    pub(super) auto_answer_negotiation: bool,
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
    pub(super) fn with_real_rtc_negotiation(port: u16) -> Option<Self> {
        let rtc_peer_factory =
            ProtocolHarnessRtcPeerFactory::new(port, default_protocol_harness_rtc);
        Some(Self {
            rtc_peer_factory: Some(rtc_peer_factory),
            rtc_peer: Some(rtc_peer_factory.build_peer()?),
            ..Self::default()
        })
    }

    pub(super) fn with_custom_rtc_negotiation(port: u16, build_rtc: fn() -> Rtc) -> Option<Self> {
        let rtc_peer_factory = ProtocolHarnessRtcPeerFactory::new(port, build_rtc);
        Some(Self {
            rtc_peer_factory: Some(rtc_peer_factory),
            rtc_peer: Some(rtc_peer_factory.build_peer()?),
            ..Self::default()
        })
    }

    pub(super) async fn connect(
        &mut self,
        url: &str,
        jwt: &str,
        room: Option<String>,
    ) -> Option<()> {
        let commands = self.core.connect(url.to_owned(), jwt.to_owned(), room);
        self.run_commands(commands).await
    }

    pub(super) async fn read_server_frame(&mut self) -> Option<()> {
        let payload = timeout(
            Duration::from_secs(1),
            read_text_message(self.websocket.as_mut()?),
        )
        .await
        .ok()??;
        let commands = self.core.on_ws_message(&payload);
        self.run_commands(commands).await
    }

    pub(super) async fn observe_close(&mut self, code: u16) -> Option<()> {
        let commands = self.core.on_ws_close(code);
        self.run_commands(commands).await
    }

    pub(super) async fn connect_and_finish_handshake(
        &mut self,
        url: &str,
        jwt: &str,
        room: Option<String>,
    ) -> Option<()> {
        self.connect(url, jwt, room).await?;
        self.read_server_frame().await?;
        self.read_server_frame().await?;
        Some(())
    }

    pub(super) async fn broadcast(&mut self, message: serde_json::Value) -> Option<()> {
        let commands = self.core.broadcast(message);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(super) async fn update_info(&mut self, info: ProtocolSessionInfo) -> Option<()> {
        let commands = self.core.update_info(info);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(super) async fn publish(
        &mut self,
        stream_type: ProtocolStreamType,
        active: bool,
    ) -> Option<()> {
        let commands = self.core.publish(stream_type, active);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(super) async fn subscribe(
        &mut self,
        user_id: ProtocolSessionId,
        states: ProtocolDownloadStates,
    ) -> Option<()> {
        let commands = self.core.subscribe(user_id, states);
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(super) async fn start_recording(
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

    pub(super) async fn stop_recording(&mut self) -> Option<()> {
        let commands = self.core.stop_recording();
        self.run_commands(commands).await?;
        self.flush_timers_with_delay(BATCH_FLUSH_DELAY_MS).await
    }

    pub(super) async fn flush_timers_with_delay(&mut self, delay_ms: u32) -> Option<()> {
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

    pub(super) async fn answer_next_negotiation(&mut self) -> Option<()> {
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

    pub(super) async fn run_commands(&mut self, commands: Vec<Command>) -> Option<()> {
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
                    upload_slots: _,
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

pub(super) async fn no_server_frame(peer: &mut ProtocolHarnessPeer, wait: Duration) -> bool {
    let Some(websocket) = peer.websocket.as_mut() else {
        return false;
    };
    timeout(wait, read_text_message(websocket)).await.is_err()
}

pub(super) async fn read_track_snapshot(
    peer: &mut ProtocolHarnessPeer,
) -> Option<Vec<TrackBinding>> {
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

pub(super) async fn real_rtc_route_activity(
    server: &TestServer,
    room: &Arc<Room>,
    source_user_id: UserId,
    consumer_user_id: UserId,
    mid: &str,
) -> Option<RealRtcRouteActivity> {
    let _source_connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&source_user_id)
        .await?;
    let consumer_connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&consumer_user_id)
        .await?;
    let consumer_session_key = room.transport_user_key(&consumer_user_id, consumer_connection_id);
    let route_entry = server
        .transport_adapter
        .debug_route_entry_by_consumer_mid(&consumer_session_key, Mid::from(mid))
        .await?;
    Some(RealRtcRouteActivity {
        source_active: route_entry.source_active,
        consumer_active: route_entry.destinations.iter().any(|destination| {
            destination.dest_session == consumer_session_key && destination.active
        }),
    })
}

pub(super) async fn assert_real_rtc_subscribe_activity(
    bob: &mut ProtocolHarnessPeer,
    server: &TestServer,
    room: &Arc<Room>,
    published_track: &TrackBinding,
    source_user_id: UserId,
    consumer_user_id: UserId,
    active: bool,
) -> Option<()> {
    bob.subscribe(
        protocol_user_id(&source_user_id),
        ProtocolDownloadStates {
            camera: Some(active),
            ..ProtocolDownloadStates::default()
        },
    )
    .await?;
    if !no_server_frame(bob, Duration::from_millis(150)).await {
        return None;
    }
    let route_activity = real_rtc_route_activity(
        server,
        room,
        source_user_id,
        consumer_user_id,
        &published_track.mid,
    )
    .await?;
    if route_activity
        != (RealRtcRouteActivity {
            source_active: true,
            consumer_active: active,
        })
    {
        return None;
    }
    Some(())
}

pub(super) async fn publish_camera_and_bootstrap_subscriber(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
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
    assert_eq!(track_binding.user_id, protocol_user_id(publisher_user_id));
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the remote-track renegotiation request"
    );
    Some(track_binding.clone())
}

pub(super) async fn recover_subscriber_and_replay_track(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
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
        &protocol_user_id(publisher_user_id),
        ProtocolStreamType::Camera,
    );
    let replayed_track = replayed_track_snapshot.first()?;
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the replayed remote-track renegotiation request"
    );
    Some(replayed_track.clone())
}

pub(super) async fn setup_fake_protocol_peers(
    adapter: Arc<FakeWebRtcAdapter>,
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
) -> Option<(
    TestServer,
    Arc<Room>,
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
    let room = create_room(&server, room_name, None, CreateRoomQuery::default()).await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), alice_user_id.clone())?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), bob_user_id.clone())?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, protocol_user_id(&bob_user_id)).await?;
    Some((server, room, alice, bob))
}

pub(super) async fn read_single_protocol_server_message(
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

pub(super) fn protocol_user_id(user_id: &UserId) -> ProtocolSessionId {
    match user_id {
        UserId::Integer(value) => ProtocolSessionId::Integer(*value),
        UserId::String(value) => ProtocolSessionId::String(value.clone()),
    }
}

pub(super) async fn consume_peer_joined_update(
    peer: &mut ProtocolHarnessPeer,
    user_id: ProtocolSessionId,
) -> Option<()> {
    peer.read_server_frame().await?;
    assert_eq!(
        peer.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&user_id),
            ProtocolSessionInfo::snapshot_defaults(),
        )]))),
        "peer join should project into the post-auth user-info update surface"
    );
    Some(())
}

pub(super) fn assert_track_snapshot_contains(
    track_bindings: &[TrackBinding],
    user_id: &ProtocolSessionId,
    stream_type: ProtocolStreamType,
) {
    assert!(
        track_bindings.iter().any(|binding| {
            binding.user_id == *user_id && binding.stream_type == stream_type && binding.active
        }),
        "expected an active track binding for user {user_id:?} and stream {stream_type:?}"
    );
}

pub(super) async fn setup_real_rtc_protocol_peers(
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
    alice_port: u16,
    bob_port: u16,
) -> Option<(
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_protocol_rtc_test_server(1_000, 100).await?;
    let room = create_room(&server, room_name, None, CreateRoomQuery::default()).await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), alice_user_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), bob_user_id.clone())?;

    let mut alice = ProtocolHarnessPeer::with_real_rtc_negotiation(alice_port)?;
    let mut bob = ProtocolHarnessPeer::with_real_rtc_negotiation(bob_port)?;
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, protocol_user_id(&bob_user_id)).await?;

    Some((server, room, alice, bob))
}

pub(super) fn reduced_capability_rtc() -> Rtc {
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

fn recording_permissions() -> UserPermissions {
    UserPermissions {
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

pub(super) async fn connect_protocol_recording_peer(
    server: &TestServer,
    room: &Room,
) -> Option<ProtocolHarnessPeer> {
    let token = signed_connect_claims_with_permissions(
        TEST_AUTH_KEY,
        room.uuid(),
        UserId::Integer(63),
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

pub(super) async fn assert_recording_request_roundtrip(
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

pub(super) async fn setup_protocol_recovery_peers(
    alice_user_id: UserId,
    bob_user_id: UserId,
) -> Option<(
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_protocol_test_server(1_000, 100).await?;
    let room = create_room(
        &server,
        "issuer-protocol-recovery",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), alice_user_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), bob_user_id.clone())?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, protocol_user_id(&bob_user_id)).await?;
    Some((server, room, alice, bob))
}

pub(super) async fn bob_update_info_and_deliver(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    alice.read_server_frame().await?;
    Some(())
}

pub(super) async fn close_peer_and_observe_recovery(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
) -> Option<()> {
    bob.websocket.as_mut()?.close(None).await.ok()?;
    bob.websocket = None;
    bob.observe_close(1011).await?;
    alice.read_server_frame().await?;
    Some(())
}

pub(super) async fn recover_peer_with_latest_info(
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

pub(super) fn peer_reached_state(peer: &ProtocolHarnessPeer, state: BundleConnectionState) -> bool {
    peer.state_changes
        .iter()
        .any(|change| change.state == state && change.cause.is_none())
}

pub(super) fn sample_video_rtp_parameters(mid: &str) -> o_sfu_router::MediaStream {
    router_sample_video_rtp_parameters(Some(mid), 22_222)
}
