//! Regression coverage for packet-loop contracts.
//!
//! These tests exercise the packet-loop helpers at their boundary points:
//! ingress demux caching, relay fanout, packet sink accounting, route-control
//! observations, keyframe feedback coalescing and scheduling deadlines. They
//! intentionally avoid running a full async worker unless the contract under
//! test requires worker scheduling behavior.

use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use str0m::{
    crypto::from_feature_flags,
    ice::{StunMessage, TransId},
    media::{KeyframeRequestKind, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers, RECEIVE_BUFFER_LEN},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::route_packet_to_matching_session,
    input::{PacketLoopInputReceivers, PacketLoopMailboxInput},
    keyframe_requests::{PendingKeyframeRequest, flush_pending_keyframe_requests},
    lag::PacketLoopLagSnapshot,
    loop_driver::{
        PacketLoopApplyContext, PacketLoopConfig, PacketLoopTurn, PacketLoopTurnInput,
        WaitPhaseSnapshot,
    },
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    runtime::{
        RoomInstanceId, UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{
            SourcePolicySignal, TransportMediaId, TransportSessionKey, TransportSourceKey,
        },
        metrics::{
            RtcMetricsRecorder, RtcRouteControlMetrics, RtpForwardDestinationKind,
            RtpMetricsRecorder, RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt,
        },
        packet_sink_registry::{
            PacketSink as MediaPacketSink, PacketSinkLookup, RegisteredPacketSink,
            RoomPacketSinkRegistry,
        },
        rtc_engine::{
            bitrate::BitrateRegistry,
            bootstrap,
            commands::{
                RemoteSourceControl, RouteControlRequest, RtcMediaControlCommand, RtcWorkerCommand,
            },
            demux::{MediaRouteDestination, MediaRouteEntry},
            forwarding_destination::PacketForward,
            media_registry::RegisteredMediaHandle,
            relay_registry::{RelayPacketMailbox, RelayTargetId},
            route_control::{KeyframeRequestDecision, PacketLayerGate},
            slots::ConsumerStreamHandle,
            state::{PacketLoopState, RtcSnapshotState, SharedRtcSocket},
            test_support::{
                DebugProbe, DebugProbeRequest, collect_ready_session_keys,
                sample_already_relayed_packet, sample_forwarded_packet,
                sample_forwarded_packet_with_audio_activity, sample_forwarded_packet_with_rid,
                sample_forwarded_packet_without_mid, sample_local_forwarded_packet,
                test_transport_session_key,
            },
        },
    },
};

struct CountingSink {
    packets: AtomicUsize,
    last_session: Mutex<Option<TransportSessionKey>>,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            packets: AtomicUsize::new(0),
            last_session: Mutex::new(None),
        }
    }

    fn last_session(&self) -> Option<TransportSessionKey> {
        match self.last_session.lock() {
            Ok(last_session) => last_session.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl MediaPacketSink for CountingSink {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        match self.last_session.lock() {
            Ok(mut last_session) => *last_session = Some(session_key.clone()),
            Err(poisoned) => *poisoned.into_inner() = Some(session_key.clone()),
        }
    }
}

struct IngressRoutingHarness {
    packet_loop_state: PacketLoopState,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    demux: super::super::routing_miss::DemuxRecoveryState,
    metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
}

impl IngressRoutingHarness {
    fn new(source_port: u16, candidate_port: u16) -> Self {
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        Self {
            packet_loop_state: PacketLoopState::default(),
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            demux: super::super::routing_miss::DemuxRecoveryState::new(),
            metrics,
            rtc_metrics,
            source_addr: SocketAddr::from(([127, 0, 0, 1], source_port)),
            candidate_addr: SocketAddr::from(([127, 0, 0, 1], candidate_port)),
        }
    }

    fn route(&mut self, packet: &[u8]) {
        route_packet_to_matching_session(
            &mut self.packet_loop_state,
            &self.snapshot_state,
            &mut self.demux,
            &self.rtc_metrics,
            self.source_addr,
            self.candidate_addr,
            packet,
        );
    }
}

struct MarkSessionDirtyProbe {
    session_key: TransportSessionKey,
}

impl DebugProbe for MarkSessionDirtyProbe {
    type Output = ();

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &super::super::worker::WorkerCommandContext<'_>,
    ) -> Self::Output {
        state.mark_session_dirty(&self.session_key);
    }
}

fn drain_ready_sessions(state: &mut PacketLoopState) -> Vec<TransportSessionKey> {
    collect_ready_session_keys(state, Instant::now())
}

#[allow(
    clippy::expect_used,
    reason = "test setup helpers should fail loudly when a required RTC fixture cannot be built"
)]
fn create_rtc_session(state: &mut PacketLoopState, session: &TransportSessionKey, port: u16) {
    let created = bootstrap::ensure_session_rtc_state(
        &mut state.users,
        session,
        SocketAddr::from(([127, 0, 0, 1], port)),
        Bitrate::from_mbps(10),
        MediaCodecFlags::default(),
    )
    .expect("test session should enter RTC state");
    assert!(created, "test session should be newly created");
}

#[allow(
    clippy::expect_used,
    reason = "test setup helpers should fail loudly when a required RTC fixture is missing"
)]
fn declare_video_tx(
    state: &mut PacketLoopState,
    session: &TransportSessionKey,
    port: u16,
    mid: Mid,
    ssrc: u32,
) {
    create_rtc_session(state, session, port);
    let session_state = state
        .users
        .get_mut(session)
        .expect("test session should be registered");
    let mut direct_api = session_state.rtc.direct_api();
    direct_api.declare_media(mid, MediaKind::Video);
    direct_api.declare_stream_tx(Ssrc::from(ssrc), None, mid, None);
}

fn insert_open_route(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    consumer_session: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    consumer_stream: ConsumerStreamHandle,
    consumer_mid: Mid,
) {
    let mut route_entry = MediaRouteEntry::new(true);
    route_entry.push_destination(MediaRouteDestination {
        dest_session: consumer_session.clone(),
        dest_transport_media_id: consumer_transport_media_id,
        dest_stream: consumer_stream,
        dest_mid: consumer_mid,
        dest_payload_type: None,
        nackable: true,
        active: true,
        packet_gate: PacketLayerGate::Open,
        pending_packet_gate: None,
    });
    state
        .media_route_index
        .insert(source_transport_media_id, route_entry);
}

fn register_producer_media(
    state: &mut PacketLoopState,
    producer_session: &TransportSessionKey,
    producer_mid: &str,
) -> TransportMediaId {
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from(producer_mid),
    })
}

#[allow(
    clippy::expect_used,
    reason = "test setup helpers should fail loudly when a required keyframe fixture cannot be built"
)]
fn local_keyframe_source(
    state: &mut PacketLoopState,
    source_session: &TransportSessionKey,
    port: u16,
    source_mid: &str,
    source_ssrc: u32,
) -> TransportMediaId {
    let mid = Mid::from(source_mid);
    create_rtc_session(state, source_session, port);
    let source_session_state = state
        .users
        .get_mut(source_session)
        .expect("source session should be registered");
    let mut direct_api = source_session_state.rtc.direct_api();
    direct_api.declare_media(mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(source_ssrc), None, mid, None);
    register_producer_media(state, source_session, source_mid)
}

fn register_consumer_media(
    state: &mut PacketLoopState,
    consumer_session: &TransportSessionKey,
    consumer_mid: &str,
    source_transport_media_id: TransportMediaId,
) -> TransportMediaId {
    state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session.clone(),
        mid: Mid::from(consumer_mid),
        source_transport_media_id,
    })
}

fn push_keyframe_request(
    buffers: &mut PacketLoopBuffers,
    consumer_session: TransportSessionKey,
    consumer_mid: &str,
    kind: KeyframeRequestKind,
) {
    buffers.pending_keyframe_requests.push((
        consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from(consumer_mid),
            consumer_rid: None,
            kind,
        },
    ));
}

#[allow(
    clippy::expect_used,
    reason = "test setup helpers should fail loudly when a required remote keyframe fixture cannot be built"
)]
fn remote_keyframe_source(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    target_id: RelayTargetId,
    capacity: usize,
) -> mpsc::Receiver<RtcWorkerCommand> {
    let (control_tx, control_rx) = mpsc::channel(capacity);
    let source = TransportSourceKey::new(source_session.clone(), source_transport_media_id);
    state
        .register_remote_source(&source, RemoteSourceControl::new(control_tx, target_id))
        .expect("remote source should register");
    control_rx
}

fn populate_forward_routes(
    state: &PacketLoopState,
    packet_sinks: &impl PacketSinkLookup,
    metrics: &impl RtcRouteControlMetrics,
    pending_packets: &mut [super::super::forwarded_packet::ForwardedPacket],
    forwards: &mut Vec<super::super::forwarding_destination::PacketForward>,
) {
    for (packet_idx, packet) in pending_packets.iter_mut().enumerate() {
        super::super::forwarding_planner::populate_forward_routes_for_packet(
            state,
            packet_sinks,
            metrics,
            packet_idx,
            packet,
            forwards,
        );
    }
}

struct PacketLoopHarness {
    state: PacketLoopState,
    buffers: PacketLoopBuffers,
    metrics: RuntimeMetrics,
    rtp_metrics: Arc<RtpMetricsRecorder>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

impl PacketLoopHarness {
    fn new() -> Self {
        let metrics = RuntimeMetrics::default();
        let rtp_metrics = metrics.register_rtp_worker();
        let datagram_metrics = metrics.register_rtc_worker();
        Self {
            state: PacketLoopState::default(),
            buffers: PacketLoopBuffers::new(),
            metrics,
            rtp_metrics,
            rtc_metrics: datagram_metrics,
        }
    }

    fn without_registered_worker_recorders() -> Self {
        Self {
            state: PacketLoopState::default(),
            buffers: PacketLoopBuffers::new(),
            metrics: RuntimeMetrics::default(),
            rtp_metrics: Arc::new(RtpMetricsRecorder::default()),
            rtc_metrics: Arc::new(RtcMetricsRecorder::default()),
        }
    }

    fn only_packet_index(&self) -> usize {
        assert_eq!(
            self.buffers.pending_packets.len(),
            1,
            "forward fixture should contain exactly one packet"
        );
        0
    }

    fn add_recording_sink(
        &mut self,
        source_transport_media_id: TransportMediaId,
        sink: &Arc<CountingSink>,
    ) {
        let packet_index = self.only_packet_index();
        self.buffers.forwards.push(PacketForward::from_packet_sink(
            packet_index,
            source_transport_media_id,
            RegisteredPacketSink::new(
                Arc::<CountingSink>::clone(sink),
                RtpForwardDestinationKind::Recording,
            ),
        ));
    }

    fn add_relay(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target: RelayPacketMailbox,
    ) {
        let packet_index = self.only_packet_index();
        self.buffers.forwards.push(PacketForward::from_relay_target(
            packet_index,
            source_transport_media_id,
            target,
        ));
    }

    fn add_local(&mut self, source_transport_media_id: TransportMediaId, destination_index: usize) {
        let packet_index = self.only_packet_index();
        self.buffers
            .forwards
            .push(PacketForward::from_local_route_destination(
                packet_index,
                source_transport_media_id,
                destination_index,
            ));
    }
}

fn packet_loop_config_for_test() -> PacketLoopConfig {
    let metrics = Arc::new(RuntimeMetrics::default());
    let outbound_recorder = metrics.register_rtp_worker();
    let datagram_recorder = metrics.register_rtc_worker();
    PacketLoopConfig {
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        max_bitrate_in: Bitrate::from_mbps(8),
        max_bitrate_out: Bitrate::from_mbps(10),
        video_bitrate_limits: VideoBitrateLimits::default(),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        codec_flags: MediaCodecFlags::default(),
        codec_preferences: CodecPreferences::default(),
        media_quality_interval: None,
        media_id_base: 0,
        diagnostics: Arc::new(DiagnosticsStore::default()),
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        source_policy_signal: Arc::new(SourcePolicySignal::default()),
        metrics,
        rtp_metrics: outbound_recorder,
        rtc_metrics: datagram_recorder,
        packet_loop_lag: Arc::new(PacketLoopLagSnapshot::new(Instant::now())),
    }
}

fn valid_rtp_packet(sequence_number: u16, ssrc: u32) -> Vec<u8> {
    let sequence_number = sequence_number.to_be_bytes();
    let ssrc = ssrc.to_be_bytes();
    vec![
        0x80,
        96,
        sequence_number[0],
        sequence_number[1],
        0,
        0,
        0,
        1,
        ssrc[0],
        ssrc[1],
        ssrc[2],
        ssrc[3],
    ]
}

fn serialize_stun_message(message: &StunMessage<'_>, password: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut buffer = [0_u8; 1024];
    let crypto_provider = from_feature_flags();
    let sha1_hmac = |key: &[u8], payloads: &[&[u8]]| {
        crypto_provider.sha1_hmac_provider.sha1_hmac(key, payloads)
    };
    let len = message.to_bytes(password, &mut buffer, sha1_hmac).ok()?;
    buffer.get(..len).map(<[u8]>::to_vec)
}

#[test]
fn recent_miss_cache_skips_repeated_scans_for_the_same_source() {
    let mut harness = IngressRoutingHarness::new(45_001, 45_000);
    let packet = valid_rtp_packet(1, 11);

    harness.route(&packet);
    harness.route(&packet);

    assert_eq!(harness.demux.fallback_attempts(), 1);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 0);
}

#[test]
fn recent_miss_cache_clears_on_topology_change() {
    let mut harness = IngressRoutingHarness::new(45_011, 45_010);
    let packet = valid_rtp_packet(2, 22);

    harness.route(&packet);
    harness.demux.clear_on_topology_change();
    harness.route(&packet);

    assert_eq!(harness.demux.fallback_attempts(), 2);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
}

#[test]
fn recent_miss_cache_does_not_skip_different_packets_from_the_same_source() {
    let mut harness = IngressRoutingHarness::new(45_021, 45_020);

    harness.route(&valid_rtp_packet(3, 33));
    harness.route(&valid_rtp_packet(4, 44));

    assert_eq!(harness.demux.fallback_attempts(), 2);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn source_rate_limiter_bounds_varied_unknown_source_misses() {
    let mut harness = IngressRoutingHarness::new(45_026, 45_025);

    for (sequence, ssrc) in [
        (5_u16, 55_u32),
        (6, 66),
        (7, 77),
        (8, 88),
        (9, 99),
        (10, 110),
    ] {
        harness.route(&valid_rtp_packet(sequence, ssrc));
    }

    assert_eq!(harness.demux.fallback_attempts(), 4);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 2);
}

#[test]
fn malformed_udp_datagram_counts_as_malformed_drop_without_scan_metrics() {
    let mut harness = IngressRoutingHarness::new(45_031, 45_030);

    harness.route(&[0x01, 0x02, 0x03]);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(harness.demux.fallback_attempts(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn multi_session_unknown_source_recovery_drops_without_whole_session_scan() {
    let mut harness = IngressRoutingHarness::new(45_041, 45_040);
    let first_session = test_transport_session_key(51, 0, 52, UserId::Integer(53));
    let second_session = test_transport_session_key(51, 0, 54, UserId::Integer(55));
    let packet = [22, 0, 0, 0];

    create_rtc_session(
        &mut harness.packet_loop_state,
        &first_session,
        harness.candidate_addr.port(),
    );
    create_rtc_session(
        &mut harness.packet_loop_state,
        &second_session,
        harness.candidate_addr.port(),
    );

    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(harness.demux.fallback_attempts(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
}

#[test]
fn indexed_route_stays_cached_without_touching_recent_miss_state() -> Result<(), &'static str> {
    let mut harness = IngressRoutingHarness::new(45_046, 45_045);
    let session_key = test_transport_session_key(51, 0, 56, UserId::Integer(57));

    create_rtc_session(
        &mut harness.packet_loop_state,
        &session_key,
        harness.candidate_addr.port(),
    );
    let local_ice_credentials = harness
        .packet_loop_state
        .users
        .get_mut(&session_key)
        .map(|session_state| session_state.rtc.direct_api().local_ice_credentials())
        .ok_or("session state missing after creation")?;
    assert!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .remember_remote_addr(harness.source_addr, &session_key)
    );
    {
        let Ok(mut snapshot) = harness.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .remember_remote_addr(harness.source_addr, &session_key)
        );
    }

    let username = format!("{}:remote-ufrag", local_ice_credentials.ufrag);
    let packet = serialize_stun_message(
        &StunMessage::binding_request(&username, TransId::new(), true, 1, 1, false),
        Some(local_ice_credentials.pass.as_bytes()),
    )
    .ok_or("failed to serialize STUN binding request")?;
    let miss_key = super::super::routing_miss::PacketLoopRoutingMissKey::new(
        harness.source_addr,
        harness.candidate_addr,
        &packet,
    );
    harness
        .demux
        .record_miss(miss_key, &packet, harness.source_addr, Instant::now());

    assert!(harness.demux.should_skip_scan(miss_key, &packet));
    assert!(harness.demux.is_tracking_source(harness.source_addr));

    harness.route(&packet);

    assert_eq!(
        drain_ready_sessions(&mut harness.packet_loop_state),
        vec![session_key.clone()]
    );
    assert_eq!(harness.demux.fallback_attempts(), 0);
    assert_eq!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(harness.source_addr),
        Some(&session_key)
    );
    {
        let Ok(snapshot) = harness.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert_eq!(
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(harness.source_addr),
            Some(&session_key)
        );
    }
    assert!(harness.demux.should_skip_scan(miss_key, &packet));
    assert!(harness.demux.is_tracking_source(harness.source_addr));
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    Ok(())
}

#[test]
fn stale_indexed_route_clears_worker_and_snapshot_pins() -> Result<(), &'static str> {
    let mut harness = IngressRoutingHarness::new(45_048, 45_047);
    let stale_session_key = test_transport_session_key(51, 0, 58, UserId::Integer(59));

    assert!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .remember_remote_addr(harness.source_addr, &stale_session_key)
    );
    {
        let Ok(mut snapshot) = harness.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .remember_remote_addr(harness.source_addr, &stale_session_key)
        );
    }

    harness.route(&valid_rtp_packet(11, 111));

    assert!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(harness.source_addr)
            .is_none()
    );
    {
        let Ok(snapshot) = harness.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(harness.source_addr)
                .is_none()
        );
    }
    assert_eq!(harness.demux.fallback_attempts(), 1);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    Ok(())
}

#[test]
fn recording_forward_destination_captures_packets_without_bypassing_the_contract() {
    let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());
    let mut harness = PacketLoopHarness::new();
    register_producer_media(&mut harness.state, &producer_session, "aud-up");

    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::<CountingSink>::clone(&sink),
        RtpForwardDestinationKind::Recording,
    );
    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        ));

    populate_forward_routes(
        &harness.state,
        &packet_sink_registry,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers.pending_packets,
        &mut harness.buffers.forwards,
    );
    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert_eq!(harness.buffers.forwards.len(), 1);
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
}

#[test]
fn flush_forward_routes_writes_hot_rtp_metrics_only_to_the_worker_recorder() {
    let source_session = test_transport_session_key(128, 0, 129, UserId::Integer(130));
    let source_transport_media_id = TransportMediaId::new(131);
    let mut harness = PacketLoopHarness::without_registered_worker_recorders();
    let sink = Arc::new(CountingSink::new());

    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet(
            source_session,
            "aud-up",
            b"payload",
        ));
    harness.add_recording_sink(source_transport_media_id, &sink);

    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 0);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
}

#[test]
fn record_incoming_stats_learns_dynamic_rid_ssrc_bindings_from_rtp_extensions() {
    let producer_session = test_transport_session_key(88, 0, 89, UserId::Integer(90));
    let mut state = PacketLoopState::default();
    let source_transport_media_id =
        register_producer_media(&mut state, &producer_session, "cam-up");
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &SourcePolicySignal::default(),
        &metrics,
        &rtp_metrics,
        &mut buffers,
    );

    let mut packet_without_extensions =
        sample_forwarded_packet_without_mid(producer_session, 4321, b"payload");
    assert_eq!(
        packet_without_extensions.resolve_source_transport_media_id(&state),
        Some(source_transport_media_id)
    );
    assert_eq!(
        packet_without_extensions
            .resolve_route_control_layer_metadata(&state)
            .rid(),
        Some(Rid::from("hi"))
    );
}

#[test]
fn flush_forward_routes_records_non_local_forwarding_volume_by_destination() {
    let source_session = test_transport_session_key(118, 0, 119, UserId::Integer(120));
    let source_transport_media_id = TransportMediaId::new(121);
    let mut harness = PacketLoopHarness::new();
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, mut intra_node_rx) = RelayPacketMailbox::channel_for_test();

    harness
        .buffers
        .pending_packets
        .push(sample_already_relayed_packet(
            source_session,
            source_transport_media_id,
            "aud-up",
            b"payload",
        ));
    harness.add_recording_sink(source_transport_media_id, &sink);
    harness.add_relay(source_transport_media_id, relay_mailbox);
    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert!(intra_node_rx.try_recv().is_ok());

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay(), 7);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_enqueued(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_observed(), 1);
}

#[test]
fn flush_forward_routes_records_packet_sink_source_key_for_local_packet() -> Result<(), &'static str>
{
    let source_session = test_transport_session_key(130, 0, 131, UserId::Integer(132));
    let source_transport_media_id = TransportMediaId::new(133);
    let mut harness = PacketLoopHarness::new();
    create_rtc_session(&mut harness.state, &source_session, 9);
    let source_handle = harness
        .state
        .users
        .handle_for_key(&source_session)
        .ok_or("source handle missing after session setup")?;
    let sink = Arc::new(CountingSink::new());

    harness
        .buffers
        .pending_packets
        .push(sample_local_forwarded_packet(
            source_handle,
            "aud-up",
            b"payload",
        ));
    harness.add_recording_sink(source_transport_media_id, &sink);

    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert_eq!(sink.last_session(), Some(source_session));
    Ok(())
}

#[test]
fn flush_forward_routes_records_closed_relays_and_keeps_later_destinations() {
    let source_session = test_transport_session_key(119, 0, 120, UserId::Integer(121));
    let source_transport_media_id = TransportMediaId::new(122);
    let mut harness = PacketLoopHarness::new();
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, relay_rx) = RelayPacketMailbox::channel_for_test();

    harness
        .buffers
        .pending_packets
        .push(sample_already_relayed_packet(
            source_session,
            source_transport_media_id,
            "aud-up",
            b"payload",
        ));

    drop(relay_rx);
    harness.add_relay(source_transport_media_id, relay_mailbox);
    harness.add_recording_sink(source_transport_media_id, &sink);
    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    let snapshot = harness.metrics.snapshot();
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_closed(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 0);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 0);
}

#[test]
fn flush_forward_routes_marks_local_consumer_sessions_dirty() -> Result<(), &'static str> {
    let producer_session = test_transport_session_key(218, 0, 219, UserId::Integer(220));
    let consumer_session = test_transport_session_key(218, 0, 221, UserId::Integer(222));
    let consumer_mid = Mid::from("cam-down");
    let mut harness = PacketLoopHarness::new();
    declare_video_tx(
        &mut harness.state,
        &consumer_session,
        45_051,
        consumer_mid,
        223_001,
    );
    let consumer_stream = harness
        .state
        .users
        .get_mut(&consumer_session)
        .ok_or("consumer session missing after setup")?
        .consumer_streams
        .allocate();

    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        ));
    let source_transport_media_id = TransportMediaId::new(224);
    insert_open_route(
        &mut harness.state,
        source_transport_media_id,
        &consumer_session,
        TransportMediaId::new(223),
        consumer_stream,
        consumer_mid,
    );
    harness.add_local(source_transport_media_id, 0);

    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert_eq!(
        drain_ready_sessions(&mut harness.state),
        vec![consumer_session]
    );
    assert_eq!(
        harness.metrics.snapshot().rtp_forwarded_packets_local_rtc(),
        1
    );
    Ok(())
}

#[test]
fn flush_forward_routes_drops_stale_local_consumer_stream_handle() {
    let producer_session = test_transport_session_key(219, 0, 220, UserId::Integer(221));
    let consumer_session = test_transport_session_key(219, 0, 222, UserId::Integer(223));
    let consumer_mid = Mid::from("cam-down");
    let mut harness = PacketLoopHarness::new();
    declare_video_tx(
        &mut harness.state,
        &consumer_session,
        45_053,
        consumer_mid,
        224_001,
    );

    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        ));
    let source_transport_media_id = TransportMediaId::new(225);
    insert_open_route(
        &mut harness.state,
        source_transport_media_id,
        &consumer_session,
        TransportMediaId::new(224),
        ConsumerStreamHandle::default(),
        consumer_mid,
    );
    harness.add_local(source_transport_media_id, 0);

    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert!(drain_ready_sessions(&mut harness.state).is_empty());
    assert_eq!(
        harness.metrics.snapshot().rtp_forwarded_packets_local_rtc(),
        0
    );
}

#[test]
fn packet_loop_dirty_marks_are_unique_until_the_session_is_drained() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(320));
    let now = Instant::now();

    create_rtc_session(&mut state, &session, 45_052);
    state.mark_session_dirty(&session);
    state.mark_session_dirty(&session);
    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert_eq!(ready_sessions, vec![session.clone()]);

    state.mark_session_dirty(&session);
    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert_eq!(ready_sessions, vec![session]);
}

#[test]
fn packet_loop_wakes_immediately_when_forwarding_marks_a_session_dirty() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(320));
    let future_timeout = Instant::now() + Duration::from_secs(30);

    create_rtc_session(&mut state, &session, 45_053);
    state.update_session_timeout(&session, Some(future_timeout));
    state.mark_session_dirty(&session);

    let deadline = super::loop_driver::next_timeout_deadline(&mut state);

    assert!(deadline.is_some_and(|deadline| deadline <= Instant::now()));
}

#[tokio::test]
async fn packet_loop_waits_for_one_control_then_pumps_before_the_next_control()
-> Result<(), &'static str> {
    let mut state = PacketLoopState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
    let config = packet_loop_config_for_test();
    let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    let mut turn = PacketLoopTurn::new(Instant::now());
    let (_command_tx, command_rx) = mpsc::channel(1);
    let (_relay_tx, relay_rx) = mpsc::channel(1);
    let (probe_tx, probe_rx) = mpsc::channel(2);
    let mut inputs = PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new())
        .with_probe_receiver(probe_rx);
    let session = test_transport_session_key(418, 0, 419, UserId::Integer(420));
    let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .map_err(|_error| "packet-loop socket should bind")?;
    let socket = Arc::new(socket);
    let candidate_addr = socket
        .local_addr()
        .map_err(|_error| "packet-loop socket should have a local addr")?;
    let (dirty_response, dirty_result) = oneshot::channel();
    let (queued_response, _queued_result) = oneshot::channel();

    state.shared_socket = Some(SharedRtcSocket {
        socket,
        candidate_addr,
    });
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &session,
            candidate_addr,
            Bitrate::from_mbps(10),
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    assert!(
        probe_tx
            .send(DebugProbeRequest::new(
                MarkSessionDirtyProbe {
                    session_key: session.clone(),
                },
                dirty_response,
            ))
            .await
            .is_ok()
    );
    assert!(
        probe_tx
            .send(DebugProbeRequest::new(
                MarkSessionDirtyProbe {
                    session_key: session.clone(),
                },
                queued_response,
            ))
            .await
            .is_ok()
    );

    let first_input = turn
        .wait_for_next_input(None, &mut inputs, &mut receive_buffer)
        .await
        .ok_or("first queued control input should wake the turn")?;

    turn.apply_input(
        &mut PacketLoopApplyContext {
            packet_loop_state: &mut state,
            bitrate_registry: &bitrate_registry,
            snapshot_state: &snapshot_state,
            config: &config,
            demux: &mut demux,
            inputs: &mut inputs,
            receive_buffer: &mut receive_buffer,
        },
        first_input,
    );
    dirty_result
        .await
        .map_err(|_error| "dirty probe response should arrive")?;
    assert!(state.has_dirty_sessions());

    let snapshot = turn
        .pump(&mut state, &snapshot_state, &config, inputs.relay_rx())
        .ok_or("packet loop should keep the socket snapshot")?;

    assert!(!state.has_dirty_sessions());

    let second_input = turn
        .wait_for_next_input(Some(snapshot), &mut inputs, &mut receive_buffer)
        .await
        .ok_or("second control input should wake after the pump")?;

    assert!(matches!(second_input, PacketLoopTurnInput::Control(_)));
    assert!(!state.has_dirty_sessions());
    Ok(())
}

#[tokio::test]
async fn packet_loop_wait_takes_one_queued_datagram() -> Result<(), &'static str> {
    let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .map_err(|_error| "packet-loop socket should bind")?;
    let socket = Arc::new(socket);
    let sender = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .map_err(|_error| "sender socket should bind")?;
    let socket_addr = socket
        .local_addr()
        .map_err(|_error| "socket should have a local addr")?;
    let (_command_tx, command_rx) = mpsc::channel(1);
    let (_relay_tx, relay_rx) = mpsc::channel(1);
    let mut inputs = PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new());
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    let mut turn = PacketLoopTurn::new(Instant::now());

    assert!(sender.send_to(b"one", socket_addr).await.is_ok());
    assert!(sender.send_to(b"second", socket_addr).await.is_ok());

    let input = turn
        .wait_for_next_input(
            Some(WaitPhaseSnapshot {
                socket: Arc::clone(&socket),
                candidate_addr: socket_addr,
                next_timeout: None,
                turn_started_at: Instant::now(),
            }),
            &mut inputs,
            &mut receive_buffer,
        )
        .await
        .ok_or("queued datagram should wake the turn")?;

    let first_payload = match input {
        PacketLoopTurnInput::Datagram { received_size, .. } => receive_buffer
            .get(..received_size)
            .ok_or("first datagram size should fit the receive buffer")?,
        _ => return Err("queued datagram should become a datagram input"),
    };

    let mut second_datagram = [0_u8; RECEIVE_BUFFER_LEN];
    let Ok(Ok((received_size, _source_addr))) = timeout(
        Duration::from_millis(100),
        socket.recv_from(&mut second_datagram),
    )
    .await
    else {
        return Err("second datagram should remain available for a later turn");
    };

    let second_payload = second_datagram
        .get(..received_size)
        .ok_or("second datagram size should fit the receive buffer")?;

    assert_ne!(first_payload, second_payload);
    assert!([b"one".as_slice(), b"second".as_slice()].contains(&first_payload));
    assert!([b"one".as_slice(), b"second".as_slice()].contains(&second_payload));
    Ok(())
}

#[tokio::test]
async fn packet_loop_mailbox_wakes_for_relay_without_control_or_socket_input() {
    let source_session = test_transport_session_key(26, 0, 27, UserId::Integer(28));
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
    let (_command_tx, command_rx) = mpsc::channel(1);
    let (relay_tx, relay_rx) = mpsc::channel(1);
    let mut inputs = PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new());

    assert!(relay_tx.send(packet).await.is_ok());

    assert!(matches!(
        inputs.recv_control_or_relay().await,
        Some(PacketLoopMailboxInput::Relay)
    ));
    assert!(inputs.take_woken_relay_packet().is_some());
}

#[test]
fn silent_audio_packets_are_dropped_from_routed_fanout_after_transport_activity_tracking() {
    let producer_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
    let consumer_session = test_transport_session_key(28, 0, 31, UserId::Integer(32));
    let mut harness = PacketLoopHarness::new();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let source_transport_media_id =
        register_producer_media(&mut harness.state, &producer_session, "aud-up");
    let consumer_transport_media_id = register_consumer_media(
        &mut harness.state,
        &consumer_session,
        "aud-down",
        source_transport_media_id,
    );
    let mut route_entry = MediaRouteEntry::new(true);
    route_entry.push_destination(MediaRouteDestination {
        dest_session: consumer_session,
        dest_transport_media_id: consumer_transport_media_id,
        dest_stream: ConsumerStreamHandle::default(),
        dest_mid: Mid::from("aud-down"),
        dest_payload_type: None,
        nackable: false,
        active: true,
        packet_gate: PacketLayerGate::Open,
        pending_packet_gate: None,
    });
    harness
        .state
        .media_route_index
        .insert(source_transport_media_id, route_entry);
    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session,
            "aud-up",
            Some(false),
            Some(-72),
            b"payload",
        ));

    record_incoming_stats(
        &mut harness.state,
        &SourcePolicySignal::default(),
        &harness.metrics,
        &harness.rtp_metrics,
        &mut harness.buffers,
    );
    populate_forward_routes(
        &harness.state,
        &packet_sink_registry,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers.pending_packets,
        &mut harness.buffers.forwards,
    );

    assert!(harness.buffers.forwards.is_empty());
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 0);
}

#[test]
fn repeated_active_audio_packets_do_not_republish_source_policy_dirty_room() {
    let producer_session = test_transport_session_key(39, 0, 40, UserId::Integer(41));
    let room_instance_id = producer_session.room_instance_id();
    let mut state = PacketLoopState::default();
    register_producer_media(&mut state, &producer_session, "aud-up");
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let source_policy_signal = SourcePolicySignal::default();
    let subscription = source_policy_signal.subscribe();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session.clone(),
            "aud-up",
            Some(true),
            Some(-30),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &source_policy_signal,
        &metrics,
        &rtp_metrics,
        &mut buffers,
    );
    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([room_instance_id])
    );

    buffers.clear();
    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session,
            "aud-up",
            Some(true),
            Some(-30),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &source_policy_signal,
        &metrics,
        &rtp_metrics,
        &mut buffers,
    );
    assert!(subscription.take_pending_updates().is_empty());
}

#[test]
fn active_audio_rank_change_publishes_source_policy_dirty_room() {
    let first_producer_session = test_transport_session_key(42, 0, 43, UserId::Integer(44));
    let second_producer_session = test_transport_session_key(42, 0, 45, UserId::Integer(46));
    let room_instance_id = first_producer_session.room_instance_id();
    let mut state = PacketLoopState::default();
    let first_transport_media_id =
        register_producer_media(&mut state, &first_producer_session, "aud-first");
    let second_transport_media_id =
        register_producer_media(&mut state, &second_producer_session, "aud-second");
    let shared_observed_at = Instant::now();
    assert!(state.route_control.observe_audio_activity(
        first_transport_media_id,
        Some(true),
        Some(-30),
        shared_observed_at,
    ));
    assert!(state.route_control.observe_audio_activity(
        second_transport_media_id,
        Some(true),
        Some(-10),
        shared_observed_at,
    ));
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let source_policy_signal = SourcePolicySignal::default();
    let subscription = source_policy_signal.subscribe();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            first_producer_session,
            "aud-first",
            Some(true),
            Some(-1),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &source_policy_signal,
        &metrics,
        &rtp_metrics,
        &mut buffers,
    );

    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([room_instance_id])
    );
}

#[test]
fn packet_loop_buffers_coalesce_source_policy_dirty_rooms_before_signal_flush() {
    let mut buffers = PacketLoopBuffers::new();
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(41));
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(41));
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(42));

    buffers.flush_source_policy_dirty(&signal);

    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([RoomInstanceId::from_raw(41), RoomInstanceId::from_raw(42),])
    );
    assert!(subscription.take_pending_updates().is_empty());
}

#[test]
fn drain_relay_packets_ingests_owned_forwarded_packets_from_the_mailbox() {
    let source_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut pending_packets = Vec::new();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();

    mailbox.forward_packet(
        &PacketLoopState::default(),
        &packet,
        TransportMediaId::new(17),
    );
    drain_relay_packets(
        &mut relay_rx,
        &mut pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
        &rtc_metrics,
    );

    assert_eq!(pending_packets.len(), 1);
    let forwarded = pending_packets.first_mut();
    assert!(forwarded.is_some());
    let Some(forwarded) = forwarded else {
        return;
    };
    assert_eq!(
        forwarded.source_session_key(&PacketLoopState::default()),
        Some(&source_session)
    );
    assert_eq!(forwarded.payload(), b"payload");
    assert_eq!(
        forwarded.resolve_source_transport_media_id(&PacketLoopState::default()),
        Some(TransportMediaId::new(17))
    );
    assert_eq!(metrics.snapshot().rtc_relay_drained_packets(), 1);
}

#[test]
fn drain_relay_packets_stops_at_the_configured_cap() {
    let source_session = test_transport_session_key(26, 0, 27, UserId::Integer(28));
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut pending_packets = Vec::new();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();

    mailbox.forward_packet(
        &PacketLoopState::default(),
        &packet,
        TransportMediaId::new(18),
    );
    mailbox.forward_packet(
        &PacketLoopState::default(),
        &packet,
        TransportMediaId::new(18),
    );

    let drained = drain_relay_packets(&mut relay_rx, &mut pending_packets, 1, &rtc_metrics);

    assert_eq!(drained, 1);
    assert_eq!(pending_packets.len(), 1);
    assert!(relay_rx.try_recv().is_ok());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_relay_drain_batches(), 1);
    assert_eq!(snapshot.rtc_relay_drained_packets(), 1);
    assert_eq!(snapshot.rtc_relay_drain_cap_hits(), 1);
}

#[test]
fn flush_forward_routes_records_relay_overload_drops() {
    let source_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let source_transport_media_id = TransportMediaId::new(32);
    let mut harness = PacketLoopHarness::new();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);

    harness
        .buffers
        .pending_packets
        .push(sample_already_relayed_packet(
            source_session.clone(),
            source_transport_media_id,
            "aud-up",
            b"payload",
        ));

    relay_mailbox.forward_packet(
        &harness.state,
        &sample_forwarded_packet(source_session, "aud-up", b"prefill"),
        source_transport_media_id,
    );
    harness.add_relay(source_transport_media_id, relay_mailbox);

    flush_forward_routes(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 0);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_overloaded(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_observed(), 1);
}

#[test]
fn flush_pending_keyframe_requests_marks_local_source_sessions_dirty() {
    let source_session = test_transport_session_key(61, 0, 62, UserId::Integer(63));
    let consumer_session = test_transport_session_key(61, 0, 64, UserId::Integer(65));
    let mut harness = PacketLoopHarness::new();

    let source_transport_media_id = local_keyframe_source(
        &mut harness.state,
        &source_session,
        45_050,
        "cam-up",
        44_444,
    );
    register_consumer_media(
        &mut harness.state,
        &consumer_session,
        "cam-down",
        source_transport_media_id,
    );
    push_keyframe_request(
        &mut harness.buffers,
        consumer_session,
        "cam-down",
        KeyframeRequestKind::Pli,
    );

    flush_pending_keyframe_requests(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
    );

    assert_eq!(
        drain_ready_sessions(&mut harness.state),
        vec![source_session.clone()]
    );
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_keyframe_requests_forwards_remote_sources_by_transport_media_id() {
    let source_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
    let consumer_session = test_transport_session_key(71, 1, 74, UserId::Integer(75));
    let source_transport_media_id = TransportMediaId::new(91);
    let mut harness = PacketLoopHarness::new();
    let mut control_rx = remote_keyframe_source(
        &mut harness.state,
        source_transport_media_id,
        &source_session,
        RelayTargetId::new(1),
        1,
    );

    register_consumer_media(
        &mut harness.state,
        &consumer_session,
        "cam-down",
        source_transport_media_id,
    );
    push_keyframe_request(
        &mut harness.buffers,
        consumer_session,
        "cam-down",
        KeyframeRequestKind::Fir,
    );

    flush_pending_keyframe_requests(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
    );

    let command = control_rx.try_recv().ok();
    assert!(matches!(
        command,
        Some(RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
            request: RouteControlRequest::RequestRemoteKeyframe {
                source,
                target_id,
                rid: None,
                kind: KeyframeRequestKind::Fir,
            },
            response: None,
        })) if source.session_key() == &source_session
            && target_id == RelayTargetId::new(1)
            && source.transport_media_id() == source_transport_media_id
    ));
    assert_eq!(harness.metrics.snapshot().rtc_route_control_forwarded(), 1);
}

#[test]
fn flush_pending_keyframe_requests_coalesces_duplicate_remote_requests() {
    let source_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let first_consumer_session = test_transport_session_key(81, 1, 84, UserId::Integer(85));
    let second_consumer_session = test_transport_session_key(81, 1, 86, UserId::Integer(87));
    let source_transport_media_id = TransportMediaId::new(101);
    let mut harness = PacketLoopHarness::new();
    let mut control_rx = remote_keyframe_source(
        &mut harness.state,
        source_transport_media_id,
        &source_session,
        RelayTargetId::new(4),
        2,
    );

    register_consumer_media(
        &mut harness.state,
        &first_consumer_session,
        "cam-down",
        source_transport_media_id,
    );
    register_consumer_media(
        &mut harness.state,
        &second_consumer_session,
        "cam-down-2",
        source_transport_media_id,
    );
    push_keyframe_request(
        &mut harness.buffers,
        first_consumer_session,
        "cam-down",
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        second_consumer_session,
        "cam-down-2",
        KeyframeRequestKind::Fir,
    );

    flush_pending_keyframe_requests(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
    );

    let command = control_rx.try_recv().ok();
    assert!(matches!(
        command,
        Some(RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
            request: RouteControlRequest::RequestRemoteKeyframe {
                source,
                target_id,
                rid: None,
                kind: KeyframeRequestKind::Fir,
            },
            response: None,
        })) if source.session_key() == &source_session
            && target_id == RelayTargetId::new(4)
            && source.transport_media_id() == source_transport_media_id
    ));
    assert!(control_rx.try_recv().is_err());
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_keyframe_requests_absorbs_duplicate_local_requests_within_one_flush() {
    let source_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let first_consumer_session = test_transport_session_key(91, 0, 94, UserId::Integer(95));
    let second_consumer_session = test_transport_session_key(91, 0, 96, UserId::Integer(97));
    let mut harness = PacketLoopHarness::new();

    let source_transport_media_id = local_keyframe_source(
        &mut harness.state,
        &source_session,
        45_060,
        "cam-up",
        55_555,
    );
    register_consumer_media(
        &mut harness.state,
        &first_consumer_session,
        "cam-down-1",
        source_transport_media_id,
    );
    register_consumer_media(
        &mut harness.state,
        &second_consumer_session,
        "cam-down-2",
        source_transport_media_id,
    );
    push_keyframe_request(
        &mut harness.buffers,
        first_consumer_session,
        "cam-down-1",
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        second_consumer_session,
        "cam-down-2",
        KeyframeRequestKind::Fir,
    );

    flush_pending_keyframe_requests(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
    );

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
    assert_eq!(
        drain_ready_sessions(&mut harness.state),
        vec![source_session.clone()]
    );
    assert_eq!(
        harness
            .state
            .route_control
            .decide_keyframe_request(source_transport_media_id, Instant::now()),
        KeyframeRequestDecision::Absorb
    );
}
