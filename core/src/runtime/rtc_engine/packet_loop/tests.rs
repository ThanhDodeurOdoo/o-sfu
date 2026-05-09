//! Regression coverage for packet-loop contracts.
//!
//! These tests exercise the packet-loop helpers at their boundary points:
//! ingress demux caching, relay fanout, packet sink accounting, route-control
//! observations, keyframe feedback coalescing and scheduling deadlines. They
//! intentionally avoid running a full async worker unless the contract under
//! test requires worker scheduling behavior.

use std::{
    net::SocketAddr,
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
    net::DatagramSend,
    rtp::Ssrc,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    super::{
        forwarded_packet::ForwardedPacket,
        routing_miss::{PacketLoopRoutingMissKey, PacketLoopRoutingState},
        session_adapter::HostSessionOutput,
    },
    forward_flush::record_incoming_stats,
    host_effects::{
        PacketLoopHostEffectContext, execute_packet_loop_effects, flush_packet_loop_forwards,
    },
    ingress_routing::{
        DatagramRouteInput,
        route_packet_to_matching_session as route_packet_to_matching_session_impl,
    },
    input::{PacketLoopInputReceivers, PacketLoopWakeInput},
    keyframe_requests::{PendingKeyframeRequest, flush_pending_keyframe_requests},
    loop_driver::drain_relay_packets_into_batch,
    machine::{
        effect::{HotRtpMetricEffect, PacketLoopEffect, PacketLoopEffects},
        scratch::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopScratch, PendingTransmit},
        turn::{PacketLoopTurn, PacketLoopTurnInput},
    },
    route_snapshot::{PacketLoopRouteSnapshot, RelayRouteRef},
    session_drain::{DrainedSessionOutput, apply_session_outputs},
    time::PacketLoopTime,
};
use crate::{
    MediaCodecFlags,
    runtime::{
        RoomInstanceId, UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
        metrics::{
            RtpForwardDestinationKind, RtpMetricsRecorder, RuntimeMetrics,
            test_support::RuntimeMetricsSnapshotTestExt,
        },
        packet_sink_registry::{
            PacketSink as MediaPacketSink, PacketSinkRouteRef, RoomPacketSinkRegistry,
            into_packet_sink,
        },
        rtc_engine::{
            bootstrap,
            commands::{RemoteSourceControl, RtcWorkerCommand},
            demux::{MediaRouteDestination, MediaRouteEntry},
            media_registry::RegisteredMediaHandle,
            relay_registry::{InterNodeRelaySender, RelayPacketMailbox, RelayTargetId},
            route_control::{KeyframeRequestDecision, PacketLayerGate},
            state::{RtcBootstrapState, RtcSnapshotState},
            test_support::{
                sample_forwarded_packet, sample_forwarded_packet_with_audio_activity,
                sample_forwarded_packet_with_rid, sample_forwarded_packet_without_mid,
                test_transport_session_key,
            },
        },
    },
};

struct CountingSink {
    packets: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            packets: AtomicUsize::new(0),
        }
    }
}

impl MediaPacketSink for CountingSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        self.packets.fetch_add(1, Ordering::Relaxed);
    }
}

fn counting_sink_registry(
    room_instance_id: RoomInstanceId,
    destination_kind: RtpForwardDestinationKind,
) -> (RoomPacketSinkRegistry, Arc<CountingSink>) {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());
    registry.register_room(
        room_instance_id,
        into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        destination_kind,
    );
    (registry, sink)
}

fn route_packet_to_matching_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    routing_state: &mut super::super::routing_miss::PacketLoopRoutingState,
    metrics: &RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    let mut effects = PacketLoopEffects::default();
    route_packet_to_matching_session_impl(
        state,
        routing_state,
        &mut effects,
        DatagramRouteInput {
            source_addr,
            candidate_addr,
            packet,
            received_at: Instant::now(),
            packet_time: PacketLoopTime::ZERO,
        },
    );
    let rtp_metrics = RtpMetricsRecorder::default();
    let diagnostics = Arc::new(DiagnosticsStore::default());
    let source_policy_signal = SourcePolicySignal::default();
    let context = PacketLoopHostEffectContext {
        snapshot_state,
        diagnostics: &diagnostics,
        metrics,
        source_policy_signal: &source_policy_signal,
        rtp_metrics: &rtp_metrics,
    };
    let scratch = PacketLoopScratch::new();
    execute_packet_loop_effects(state, &scratch, &context, &effects);
}

fn populate_forward_routes(
    state: &RtcBootstrapState,
    routes: &PacketLoopRouteSnapshot,
    scratch: &mut PacketLoopScratch,
    effects: &mut PacketLoopEffects,
) {
    scratch.plan_pending_packets(|packet_idx, packet, forwards| {
        super::super::forwarding_planner::populate_forward_routes_for_packet(
            &state.packet_loop,
            routes,
            effects,
            packet_idx,
            packet,
            forwards,
        );
    });
}

fn execute_effects(
    state: &mut RtcBootstrapState,
    scratch: &mut PacketLoopScratch,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    effects: &PacketLoopEffects,
) {
    let routes = route_snapshot_for(state, &RoomPacketSinkRegistry::default());
    execute_effects_with_routes(state, scratch, metrics, rtp_metrics, effects, &routes);
}

fn execute_effects_with_routes(
    state: &mut RtcBootstrapState,
    scratch: &mut PacketLoopScratch,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    effects: &PacketLoopEffects,
    routes: &PacketLoopRouteSnapshot,
) {
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let diagnostics = Arc::new(DiagnosticsStore::default());
    let source_policy_signal = SourcePolicySignal::default();
    let context = PacketLoopHostEffectContext {
        snapshot_state: &snapshot_state,
        diagnostics: &diagnostics,
        metrics,
        source_policy_signal: &source_policy_signal,
        rtp_metrics,
    };
    execute_packet_loop_effects(state, &*scratch, &context, effects);
    flush_packet_loop_forwards(state, scratch, routes, &context);
}

fn flush_forward_route_effects(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    scratch: &mut PacketLoopScratch,
    routes: &PacketLoopRouteSnapshot,
) -> PacketLoopEffects {
    let effects = PacketLoopEffects::default();
    execute_effects_with_routes(state, scratch, metrics, rtp_metrics, &effects, routes);
    effects
}

fn route_snapshot_for(
    state: &RtcBootstrapState,
    packet_sink_registry: &RoomPacketSinkRegistry,
) -> PacketLoopRouteSnapshot {
    let mut routes = PacketLoopRouteSnapshot::default();
    routes.refresh_from(state, packet_sink_registry);
    routes
}

fn flush_keyframe_effects(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    scratch: &mut PacketLoopScratch,
    now: PacketLoopTime,
) -> PacketLoopEffects {
    let mut effects = PacketLoopEffects::default();
    flush_pending_keyframe_requests(&mut state.packet_loop, &mut effects, scratch, now);
    let rtp_metrics = RtpMetricsRecorder::default();
    execute_effects(state, scratch, metrics, &rtp_metrics, &effects);
    effects
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

struct DemuxRouteHarness {
    bootstrap_state: RtcBootstrapState,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    routing_state: PacketLoopRoutingState,
    metrics: RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
}

impl DemuxRouteHarness {
    fn new(source_port: u16, candidate_port: u16) -> Self {
        Self {
            bootstrap_state: RtcBootstrapState::default(),
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            routing_state: PacketLoopRoutingState::new(),
            metrics: RuntimeMetrics::default(),
            source_addr: SocketAddr::from(([127, 0, 0, 1], source_port)),
            candidate_addr: SocketAddr::from(([127, 0, 0, 1], candidate_port)),
        }
    }

    fn route(&mut self, packet: &[u8]) {
        route_packet_to_matching_session(
            &mut self.bootstrap_state,
            &self.snapshot_state,
            &mut self.routing_state,
            &self.metrics,
            self.source_addr,
            self.candidate_addr,
            packet,
        );
    }

    fn route_valid_rtp(&mut self, sequence_number: u16, ssrc: u32) {
        self.route(&valid_rtp_packet(sequence_number, ssrc));
    }

    fn ensure_session(&mut self, session_key: &TransportSessionKey) {
        assert_eq!(
            bootstrap::ensure_session_rtc_state(
                &mut self.bootstrap_state.users,
                session_key,
                self.candidate_addr,
                10_000_000,
                MediaCodecFlags::default(),
            ),
            Ok(true)
        );
    }

    fn pin_source_to_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Result<(), &'static str> {
        assert!(
            self.bootstrap_state
                .packet_loop
                .remote_addr_demux
                .remember_remote_addr(self.source_addr, session_key)
        );
        let Ok(mut snapshot) = self.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .remember_remote_addr(self.source_addr, session_key)
        );
        Ok(())
    }

    fn assert_source_pin(&self, session_key: &TransportSessionKey) -> Result<(), &'static str> {
        assert_eq!(
            self.bootstrap_state
                .packet_loop
                .remote_addr_demux
                .session_key_for_remote_addr(self.source_addr),
            Some(session_key)
        );
        let Ok(snapshot) = self.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert_eq!(
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(self.source_addr),
            Some(session_key)
        );
        Ok(())
    }

    fn assert_source_pin_cleared(&self) -> Result<(), &'static str> {
        assert!(
            self.bootstrap_state
                .packet_loop
                .remote_addr_demux
                .session_key_for_remote_addr(self.source_addr)
                .is_none()
        );
        let Ok(snapshot) = self.snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(self.source_addr)
                .is_none()
        );
        Ok(())
    }

    fn record_miss(&mut self, packet: &[u8]) -> PacketLoopRoutingMissKey {
        let miss_key = PacketLoopRoutingMissKey::new(self.source_addr, self.candidate_addr, packet);
        self.routing_state
            .record_miss(miss_key, packet, self.source_addr, PacketLoopTime::ZERO);
        miss_key
    }
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
fn session_output_transmit_is_staged_without_a_real_rtc() {
    let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(7));
    let destination = SocketAddr::from(([127, 0, 0, 1], 49_001));
    let mut scratch = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();
    let mut session_outputs = vec![DrainedSessionOutput::new(
        session_key,
        HostSessionOutput::Transmit {
            destination,
            contents: DatagramSend::from(Vec::from(&b"payload"[..])),
        },
    )];

    apply_session_outputs(&mut session_outputs, &mut scratch, &mut effects);

    assert!(session_outputs.is_empty());
    assert_eq!(
        scratch
            .pending_transmit(0)
            .map(PendingTransmit::destination),
        Some(destination)
    );
    assert_eq!(
        scratch.pending_transmit(0).map(PendingTransmit::contents),
        Some(&b"payload"[..])
    );
}

#[test]
fn packet_loop_turn_returns_process_effects_without_executing_them() {
    let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let mut state = RtcBootstrapState::default();
    let (packet_sink_registry, sink) = counting_sink_registry(
        producer_session.room_instance_id(),
        RtpForwardDestinationKind::Recording,
    );
    let _source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from("aud-up"),
    });
    let routes = route_snapshot_for(&state, &packet_sink_registry);
    let mut relay_packets = vec![sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    )];
    let mut session_outputs = Vec::new();
    let mut scratch = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();

    PacketLoopTurn::step(
        &mut state.packet_loop,
        &mut scratch,
        &mut effects,
        PacketLoopTurnInput::new(
            PacketLoopTime::ZERO,
            &mut session_outputs,
            &mut relay_packets,
            &routes,
        ),
    );

    assert!(relay_packets.is_empty());
    assert_eq!(
        scratch.forward_count_by_destination_kind(RtpForwardDestinationKind::Recording),
        1
    );
    assert_eq!(sink.packets.load(Ordering::Relaxed), 0);
}

#[test]
fn recent_miss_cache_skips_repeated_scans_for_the_same_source() {
    let mut harness = DemuxRouteHarness::new(45_001, 45_000);
    let packet = valid_rtp_packet(1, 11);

    harness.route(&packet);
    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 0);
}

#[test]
fn recent_miss_cache_clears_on_topology_change() {
    let mut harness = DemuxRouteHarness::new(45_011, 45_010);
    let packet = valid_rtp_packet(2, 22);

    harness.route(&packet);
    harness.routing_state.clear_on_topology_change();
    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
}

#[test]
fn recent_miss_cache_does_not_skip_different_packets_from_the_same_source() {
    let mut harness = DemuxRouteHarness::new(45_021, 45_020);

    harness.route_valid_rtp(3, 33);
    harness.route_valid_rtp(4, 44);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn source_rate_limiter_bounds_varied_unknown_source_misses() {
    let mut harness = DemuxRouteHarness::new(45_026, 45_025);

    for (sequence, ssrc) in [
        (5_u16, 55_u32),
        (6, 66),
        (7, 77),
        (8, 88),
        (9, 99),
        (10, 110),
    ] {
        harness.route_valid_rtp(sequence, ssrc);
    }

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 2);
}

#[test]
fn malformed_udp_datagram_counts_as_malformed_drop_without_scan_metrics() {
    let mut harness = DemuxRouteHarness::new(45_031, 45_030);

    harness.route(&[0x01, 0x02, 0x03]);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn multi_session_unknown_source_recovery_drops_without_whole_session_scan() {
    let mut harness = DemuxRouteHarness::new(45_041, 45_040);
    let first_session = test_transport_session_key(51, 0, 52, UserId::Integer(53));
    let second_session = test_transport_session_key(51, 0, 54, UserId::Integer(55));
    let packet = [22, 0, 0, 0];

    harness.ensure_session(&first_session);
    harness.ensure_session(&second_session);
    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
}

#[test]
fn indexed_route_stays_cached_without_touching_recent_miss_state() -> Result<(), &'static str> {
    let mut harness = DemuxRouteHarness::new(45_046, 45_045);
    let session_key = test_transport_session_key(51, 0, 56, UserId::Integer(57));

    harness.ensure_session(&session_key);
    let local_ice_credentials = harness
        .bootstrap_state
        .users
        .get_mut(&session_key)
        .map(|session_state| {
            session_state
                .host_session
                .direct_api()
                .local_ice_credentials()
        })
        .ok_or("session state missing after creation")?;
    harness.pin_source_to_session(&session_key)?;

    let username = format!("{}:remote-ufrag", local_ice_credentials.ufrag);
    let packet = serialize_stun_message(
        &StunMessage::binding_request(&username, TransId::new(), true, 1, 1, false),
        Some(local_ice_credentials.pass.as_bytes()),
    )
    .ok_or("failed to serialize STUN binding request")?;
    let miss_key = harness.record_miss(&packet);

    assert!(harness.routing_state.should_skip_scan(miss_key, &packet));
    assert!(harness.routing_state.source_is_tracked(harness.source_addr));

    harness.route(&packet);

    harness.assert_source_pin(&session_key)?;
    assert!(harness.routing_state.should_skip_scan(miss_key, &packet));
    assert!(harness.routing_state.source_is_tracked(harness.source_addr));
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    Ok(())
}

#[test]
fn stale_indexed_route_clears_worker_and_snapshot_pins() -> Result<(), &'static str> {
    let mut harness = DemuxRouteHarness::new(45_048, 45_047);
    let stale_session_key = test_transport_session_key(51, 0, 58, UserId::Integer(59));

    harness.pin_source_to_session(&stale_session_key)?;

    harness.route_valid_rtp(11, 111);

    harness.assert_source_pin_cleared()?;
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    Ok(())
}

#[test]
fn recording_forward_destination_captures_packets_without_bypassing_the_contract() {
    let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let mut state = RtcBootstrapState::default();
    let (packet_sink_registry, sink) = counting_sink_registry(
        producer_session.room_instance_id(),
        RtpForwardDestinationKind::Recording,
    );
    let _source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();

    buffers.push_pending_packet(sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    ));

    let mut effects = PacketLoopEffects::default();
    let routes = route_snapshot_for(&state, &packet_sink_registry);
    populate_forward_routes(&state, &routes, &mut buffers, &mut effects);
    flush_forward_route_effects(&mut state, &metrics, &rtp_metrics, &mut buffers, &routes);

    assert_eq!(buffers.forwards().len(), 1);
    let Some(forward) = buffers.forward(0) else {
        return;
    };
    assert_eq!(forward.packet_idx, 0);
    assert_eq!(
        forward.destination.metrics_kind(),
        RtpForwardDestinationKind::Recording
    );
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.snapshot().rtp_payload_bytes_egress(), 0);
    assert_eq!(metrics.snapshot().rtp_forwarded_packets_recording(), 1);
}

#[test]
fn flush_forward_routes_writes_hot_rtp_metrics_only_to_the_worker_recorder() {
    let source_session = test_transport_session_key(128, 0, 129, UserId::Integer(130));
    let source_transport_media_id = TransportMediaId::new(131);
    let mut state = RtcBootstrapState::default();
    let (packet_sink_registry, sink) = counting_sink_registry(
        source_session.room_instance_id(),
        RtpForwardDestinationKind::Recording,
    );
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = RtpMetricsRecorder::default();
    let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload");
    let routes = route_snapshot_for(&state, &packet_sink_registry);
    let sink_route = routes.packet_sink_route_for_room(source_session.room_instance_id());
    assert!(sink_route.is_some(), "registered packet sink route");
    let Some(sink_route) = sink_route else {
        return;
    };

    buffers.push_pending_packet(packet);
    buffers.push_forward(
        super::super::forwarding_destination::PacketForward::from_packet_sink(
            0,
            source_transport_media_id,
            sink_route,
        ),
    );

    flush_forward_route_effects(&mut state, &metrics, &rtp_metrics, &mut buffers, &routes);

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 0);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
}

#[test]
fn record_incoming_stats_learns_dynamic_rid_ssrc_bindings_from_rtp_extensions() {
    let producer_session = test_transport_session_key(88, 0, 89, UserId::Integer(90));
    let producer_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: producer_mid,
    });
    let mut buffers = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();

    buffers.push_pending_packet(sample_forwarded_packet_with_rid(
        producer_session.clone(),
        "cam-up",
        Some("hi"),
        b"payload",
    ));
    record_incoming_stats(
        &mut state.packet_loop,
        &mut effects,
        &mut buffers,
        PacketLoopTime::ZERO,
    );

    assert!(effects.iter().any(|effect| matches!(
        effect,
        PacketLoopEffect::RecordIncomingBitrate {
            transport_media_id,
            payload_bytes: 7,
            ..
        } if *transport_media_id == source_transport_media_id
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        PacketLoopEffect::RecordHotRtpMetric(HotRtpMetricEffect::Ingress { payload_bytes: 7 })
    )));

    let mut packet_without_extensions =
        sample_forwarded_packet_without_mid(producer_session, 4321, b"payload");
    assert_eq!(
        packet_without_extensions.resolve_source_transport_media_id(&state.packet_loop),
        Some(source_transport_media_id)
    );
    assert_eq!(
        packet_without_extensions
            .resolve_route_control_layer_metadata(&state.packet_loop)
            .rid(),
        Some(Rid::from("hi"))
    );
}

#[test]
fn flush_forward_routes_records_non_local_forwarding_volume_by_destination() {
    let source_session = test_transport_session_key(118, 0, 119, UserId::Integer(120));
    let source_transport_media_id = TransportMediaId::new(121);
    let mut state = RtcBootstrapState::default();
    let (packet_sink_registry, sink) = counting_sink_registry(
        source_session.room_instance_id(),
        RtpForwardDestinationKind::Recording,
    );
    let (relay_mailbox, mut intra_node_rx) = RelayPacketMailbox::channel_for_test();
    let (inter_node_sender, mut inter_node_rx) = InterNodeRelaySender::channel_for_test();
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload")
        .share_for_relay(source_transport_media_id);
    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(1),
        relay_mailbox.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(1), true);
    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(2),
        inter_node_sender.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(2), true);
    let routes = route_snapshot_for(&state, &packet_sink_registry);
    let sink_route = routes.packet_sink_route_for_room(source_session.room_instance_id());
    assert!(sink_route.is_some(), "registered packet sink route");
    let Some(sink_route) = sink_route else {
        return;
    };
    let relay_routes = routes.relay_routes_for_source(source_transport_media_id);
    assert!(relay_routes.is_some(), "registered relay routes");
    let Some(relay_routes) = relay_routes else {
        return;
    };
    let intra_node_route = relay_routes
        .iter()
        .map(|route| route.route_ref())
        .find(|route_ref| matches!(route_ref, RelayRouteRef::IntraNode(_)));
    assert!(
        intra_node_route.is_some(),
        "registered intra-node relay route"
    );
    let Some(intra_node_route) = intra_node_route else {
        return;
    };
    let inter_node_route = relay_routes
        .iter()
        .map(|route| route.route_ref())
        .find(|route_ref| matches!(route_ref, RelayRouteRef::InterNode(_)));
    assert!(
        inter_node_route.is_some(),
        "registered inter-node relay route"
    );
    let Some(inter_node_route) = inter_node_route else {
        return;
    };

    buffers.push_pending_packet(packet);
    buffers.push_forward(
        super::super::forwarding_destination::PacketForward::from_packet_sink(
            0,
            source_transport_media_id,
            sink_route,
        ),
    );
    buffers.push_forward(
        super::super::forwarding_destination::PacketForward::from_relay_sink(
            0,
            source_transport_media_id,
            intra_node_route,
        ),
    );
    buffers.push_forward(
        super::super::forwarding_destination::PacketForward::from_relay_sink(
            0,
            source_transport_media_id,
            inter_node_route,
        ),
    );

    flush_forward_route_effects(&mut state, &metrics, &rtp_metrics, &mut buffers, &routes);

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert!(intra_node_rx.try_recv().is_ok());
    assert!(inter_node_rx.try_recv().is_ok());

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_inter_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay(), 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_inter_node_relay(), 7);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
}

#[test]
fn flush_forward_routes_marks_local_consumer_sessions_dirty() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_051));
    let producer_session = test_transport_session_key(218, 0, 219, UserId::Integer(220));
    let consumer_session = test_transport_session_key(218, 0, 221, UserId::Integer(222));
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(consumer_session_state) = state.users.get_mut(&consumer_session) else {
        return;
    };
    let mut direct_api = consumer_session_state.host_session.direct_api();
    direct_api.declare_media(consumer_mid, MediaKind::Video);
    direct_api.declare_stream_tx(Ssrc::from(223_001_u32), None, consumer_mid, None);

    buffers.push_pending_packet(sample_forwarded_packet(
        producer_session,
        "cam-up",
        b"payload",
    ));
    buffers.push_forward(
        super::super::forwarding_destination::PacketForward::from_local_route_destination(
            0,
            &MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: TransportMediaId::new(223),
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            },
        ),
    );

    let routes = route_snapshot_for(&state, &RoomPacketSinkRegistry::default());
    flush_forward_route_effects(&mut state, &metrics, &rtp_metrics, &mut buffers, &routes);

    assert!(state.packet_loop.dirty_sessions.contains(&consumer_session));
    assert_eq!(metrics.snapshot().rtp_forwarded_packets_local_rtc(), 1);
}

#[test]
fn packet_loop_wakes_immediately_when_forwarding_marks_a_session_dirty() {
    let mut state = RtcBootstrapState::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(320));
    let now = PacketLoopTime::from_millis(10);
    let future_timeout = now + Duration::from_secs(30);

    state
        .packet_loop
        .update_session_timeout(&session, Some(future_timeout));
    state.packet_loop.mark_session_dirty(&session);

    let deadline = super::loop_driver::next_timeout_deadline(&mut state.packet_loop, now);

    assert_eq!(deadline, Some(now));
}

#[test]
fn dirty_session_scheduler_reuses_capacity_after_warmup() {
    let mut state = RtcBootstrapState::default();
    let mut ready_sessions = Vec::new();
    let sessions = (0_u64..96)
        .map(|idx| {
            test_transport_session_key(
                418,
                0,
                idx,
                UserId::Integer(i64::try_from(idx).map_or(i64::MAX, |value| value)),
            )
        })
        .collect::<Vec<_>>();

    for session in &sessions {
        state.packet_loop.mark_session_dirty(session);
        state.packet_loop.mark_session_dirty(session);
    }
    state
        .packet_loop
        .drain_ready_sessions(PacketLoopTime::ZERO, &mut ready_sessions);
    let dirty_capacity = state.packet_loop.dirty_session_capacity();
    let ready_capacity = ready_sessions.capacity();

    for session in &sessions {
        state.packet_loop.mark_session_dirty(session);
        state.packet_loop.mark_session_dirty(session);
    }
    state
        .packet_loop
        .drain_ready_sessions(PacketLoopTime::ZERO, &mut ready_sessions);

    assert_eq!(ready_sessions.len(), sessions.len());
    assert_eq!(state.packet_loop.dirty_session_capacity(), dirty_capacity);
    assert_eq!(ready_sessions.capacity(), ready_capacity);
}

#[test]
fn packet_loop_lag_reporter_rate_limits_snapshot_updates() {
    let mut reporter = super::loop_driver::PacketLoopLagReporter::default();
    let start = Instant::now();

    assert_eq!(reporter.record(start, 1), Some(1));
    assert_eq!(reporter.record(start + Duration::from_millis(10), 5), None);
    assert_eq!(reporter.record(start + Duration::from_millis(20), 3), None);
    assert_eq!(
        reporter.record(start + Duration::from_millis(50), 2),
        Some(5)
    );
    assert_eq!(
        reporter.record(start + Duration::from_millis(100), 4),
        Some(4)
    );
}

#[test]
fn silent_audio_packets_are_dropped_from_routed_fanout_after_transport_activity_tracking() {
    let producer_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
    let consumer_session = test_transport_session_key(28, 0, 31, UserId::Integer(32));
    let mut state = RtcBootstrapState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from("aud-up"),
    });
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: Mid::from("aud-down"),
            source_transport_media_id,
        });
    state.packet_loop.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session,
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: Mid::from("aud-down"),
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            }],
        },
    );
    let mut buffers = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();
    buffers.push_pending_packet(sample_forwarded_packet_with_audio_activity(
        producer_session,
        "aud-up",
        Some(false),
        Some(-72),
        b"payload",
    ));

    record_incoming_stats(
        &mut state.packet_loop,
        &mut effects,
        &mut buffers,
        PacketLoopTime::ZERO,
    );
    let routes = route_snapshot_for(&state, &packet_sink_registry);
    populate_forward_routes(&state, &routes, &mut buffers, &mut effects);
    execute_effects_with_routes(
        &mut state,
        &mut buffers,
        &metrics,
        &rtp_metrics,
        &effects,
        &routes,
    );

    assert!(buffers.forwards().is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 0);
}

#[test]
fn packet_loop_scratch_clear_reuses_capacity_across_turns() {
    let mut scratch = PacketLoopScratch::new();
    let base_capacity = scratch.capacities();
    let source_session = test_transport_session_key(35, 0, 36, UserId::Integer(37));
    let sink_route = PacketSinkRouteRef::for_test(0, RtpForwardDestinationKind::Recording);

    for port_offset in 0_u16..70 {
        let destination = SocketAddr::from(([127, 0, 0, 1], 40_000 + port_offset));
        scratch.push_pending_transmit(destination, b"payload");
    }
    for _ in 0..40 {
        scratch.push_pending_packet(sample_forwarded_packet(
            source_session.clone(),
            "aud-up",
            b"payload",
        ));
    }
    for index in 0..70 {
        scratch.push_forward(
            super::super::forwarding_destination::PacketForward::from_packet_sink(
                index % 40,
                TransportMediaId::new(38),
                sink_route,
            ),
        );
    }
    for index in 0..12 {
        scratch.push_pending_keyframe_request(
            source_session.clone(),
            PendingKeyframeRequest {
                consumer_mid: Mid::from("cam-down"),
                consumer_rid: None,
                kind: KeyframeRequestKind::Pli,
            },
        );
        scratch.mark_source_policy_dirty(RoomInstanceId::from_raw(index));
    }
    scratch.observe_pending_packets(|_packet_idx, _packet, observation_scratch| {
        observation_scratch
            .rid_readiness()
            .ready
            .push(Rid::from("hi"));
        observation_scratch
            .rid_readiness()
            .stale
            .push(Rid::from("mid"));
        observation_scratch
            .rid_readiness()
            .pending_selected
            .push(Rid::from("lo"));
    });
    scratch.with_forwarding_buffers(|_forwards, _pending_packets, _relay_packets| {});

    let warmed_capacity = scratch.capacities();
    assert!(warmed_capacity.retained_at_least(base_capacity));

    scratch.clear();

    assert!(scratch.is_turn_empty());
    assert_eq!(scratch.capacities(), warmed_capacity);
}

#[test]
fn drain_relay_packets_ingests_owned_forwarded_packets_from_the_mailbox() {
    let source_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut relay_packets = Vec::new();
    let mut pending_relay_packet = None;

    mailbox.forward_packet(&packet, TransportMediaId::new(17));
    drain_relay_packets_into_batch(
        &mut relay_rx,
        &mut relay_packets,
        &mut pending_relay_packet,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );

    assert_eq!(relay_packets.len(), 1);
    let forwarded = relay_packets.get_mut(0);
    assert!(forwarded.is_some());
    let Some(forwarded) = forwarded else {
        return;
    };
    assert_eq!(forwarded.source_session_key(), &source_session);
    assert_eq!(forwarded.payload(), b"payload");
    assert_eq!(
        forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default().packet_loop),
        Some(TransportMediaId::new(17))
    );
}

#[test]
fn drain_relay_packets_stops_at_the_configured_cap() {
    let source_session = test_transport_session_key(26, 0, 27, UserId::Integer(28));
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut relay_packets = Vec::new();
    let mut pending_relay_packet = None;

    mailbox.forward_packet(&packet, TransportMediaId::new(18));
    mailbox.forward_packet(&packet, TransportMediaId::new(18));

    let drained = drain_relay_packets_into_batch(
        &mut relay_rx,
        &mut relay_packets,
        &mut pending_relay_packet,
        1,
    );

    assert_eq!(drained, 1);
    assert_eq!(relay_packets.len(), 1);
    assert!(relay_rx.try_recv().is_ok());
}

#[test]
fn drain_relay_packets_preserves_the_packet_that_woke_the_loop() {
    let source_session = test_transport_session_key(27, 0, 28, UserId::Integer(29));
    let pending_packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"pending");
    let mailbox_packet = sample_forwarded_packet(source_session, "aud-up", b"mailbox");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut relay_packets = Vec::new();
    let mut pending_relay_packet = Some(pending_packet);

    mailbox.forward_packet(&mailbox_packet, TransportMediaId::new(18));

    let drained = drain_relay_packets_into_batch(
        &mut relay_rx,
        &mut relay_packets,
        &mut pending_relay_packet,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );

    assert_eq!(drained, 1);
    assert_eq!(relay_packets.len(), 2);
    assert_eq!(
        relay_packets.first().map(ForwardedPacket::payload),
        Some(b"pending".as_slice())
    );
    assert_eq!(
        relay_packets.get(1).map(ForwardedPacket::payload),
        Some(b"mailbox".as_slice())
    );
    assert!(pending_relay_packet.is_none());
}

#[test]
fn drain_relay_packets_keeps_pending_wake_packet_when_cap_is_zero() {
    let source_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
    let pending_packet = sample_forwarded_packet(source_session, "aud-up", b"pending");
    let (_mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut relay_packets = Vec::new();
    let mut pending_relay_packet = Some(pending_packet);

    let drained = drain_relay_packets_into_batch(
        &mut relay_rx,
        &mut relay_packets,
        &mut pending_relay_packet,
        0,
    );

    assert_eq!(drained, 0);
    assert!(relay_packets.is_empty());
    assert!(pending_relay_packet.is_some());
}

#[tokio::test]
async fn packet_loop_wait_wakes_on_relay_packet() {
    let (_command_tx, command_rx) = mpsc::channel(1);
    let (relay_tx, relay_rx) = mpsc::channel(1);
    let shutdown_token = CancellationToken::new();
    let mut inputs = PacketLoopInputReceivers::new(command_rx, relay_rx, shutdown_token);
    let source_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
    let mut pending_relay_packet = None;

    assert!(relay_tx.send(packet).await.is_ok());

    let input = inputs
        .recv_control_or_relay(&mut pending_relay_packet)
        .await;

    assert!(matches!(input, Some(PacketLoopWakeInput::Relay)));
    assert!(pending_relay_packet.is_some());
}

#[test]
fn flush_forward_routes_records_relay_overload_drops() {
    let source_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let source_transport_media_id = TransportMediaId::new(32);
    let mut state = RtcBootstrapState::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload")
        .share_for_relay(source_transport_media_id);

    relay_mailbox.forward_packet(
        &sample_forwarded_packet(
            test_transport_session_key(29, 0, 30, UserId::Integer(31)),
            "aud-up",
            b"prefill",
        ),
        source_transport_media_id,
    );
    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(1),
        relay_mailbox.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(1), true);
    let routes = route_snapshot_for(&state, &RoomPacketSinkRegistry::default());
    let relay_route = routes
        .relay_routes_for_source(source_transport_media_id)
        .and_then(|routes| routes.first().copied())
        .map(super::route_snapshot::PacketLoopRelayRoute::route_ref);
    assert!(relay_route.is_some(), "registered relay route");
    let Some(relay_route) = relay_route else {
        return;
    };
    buffers.push_pending_packet(packet);
    buffers.push_forward(
        super::super::forwarding_destination::PacketForward::from_relay_sink(
            0,
            source_transport_media_id,
            relay_route,
        ),
    );

    flush_forward_route_effects(&mut state, &metrics, &rtp_metrics, &mut buffers, &routes);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 0);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 1);
}

#[test]
fn flush_pending_keyframe_requests_marks_local_source_sessions_dirty() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_050));
    let source_session = test_transport_session_key(61, 0, 62, UserId::Integer(63));
    let consumer_session = test_transport_session_key(61, 0, 64, UserId::Integer(65));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &source_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.users.get_mut(&source_session) else {
        return;
    };
    let mut direct_api = source_session_state.host_session.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(44_444_u32), None, source_mid, None);

    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    });
    let _consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    buffers.push_pending_keyframe_request(
        consumer_session,
        PendingKeyframeRequest {
            consumer_mid,
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );

    flush_keyframe_effects(&mut state, &metrics, &mut buffers, PacketLoopTime::ZERO);

    assert!(state.packet_loop.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_keyframe_requests_forwards_remote_sources_by_transport_media_id() {
    let source_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
    let consumer_session = test_transport_session_key(71, 1, 74, UserId::Integer(75));
    let consumer_mid = Mid::from("cam-down");
    let source_transport_media_id = TransportMediaId::new(91);
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let (control_tx, mut control_rx) = mpsc::channel(1);

    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(control_tx, RelayTargetId::new(1)),
            )
            .is_ok()
    );
    let _consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    buffers.push_pending_keyframe_request(
        consumer_session,
        PendingKeyframeRequest {
            consumer_mid,
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    );

    flush_keyframe_effects(&mut state, &metrics, &mut buffers, PacketLoopTime::ZERO);

    let command = control_rx.try_recv().ok();
    assert!(matches!(
        command,
        Some(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id: forwarded_transport_media_id,
            target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        }) if source_session_key == source_session
            && target_id == RelayTargetId::new(1)
            && forwarded_transport_media_id == source_transport_media_id
    ));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded(), 1);
}

#[test]
fn flush_pending_keyframe_requests_coalesces_duplicate_remote_requests() {
    let source_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let first_consumer_session = test_transport_session_key(81, 1, 84, UserId::Integer(85));
    let second_consumer_session = test_transport_session_key(81, 1, 86, UserId::Integer(87));
    let consumer_mid = Mid::from("cam-down");
    let source_transport_media_id = TransportMediaId::new(101);
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();
    let (control_tx, mut control_rx) = mpsc::channel(2);

    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(control_tx, RelayTargetId::new(4)),
            )
            .is_ok()
    );
    let _first_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: first_consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    let _second_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: second_consumer_session.clone(),
            mid: Mid::from("cam-down-2"),
            source_transport_media_id,
        });
    buffers.push_pending_keyframe_request(
        first_consumer_session,
        PendingKeyframeRequest {
            consumer_mid,
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );
    buffers.push_pending_keyframe_request(
        second_consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("cam-down-2"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    );

    flush_keyframe_effects(&mut state, &metrics, &mut buffers, PacketLoopTime::ZERO);

    let command = control_rx.try_recv().ok();
    assert!(matches!(
        command,
        Some(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id: forwarded_transport_media_id,
            target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        }) if source_session_key == source_session
            && target_id == RelayTargetId::new(4)
            && forwarded_transport_media_id == source_transport_media_id
    ));
    assert!(control_rx.try_recv().is_err());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_keyframe_requests_absorbs_duplicate_local_requests_within_one_flush() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_060));
    let source_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let first_consumer_session = test_transport_session_key(91, 0, 94, UserId::Integer(95));
    let second_consumer_session = test_transport_session_key(91, 0, 96, UserId::Integer(97));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopScratch::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &source_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.users.get_mut(&source_session) else {
        return;
    };
    let mut direct_api = source_session_state.host_session.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(55_555_u32), None, source_mid, None);

    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    });
    let _first_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: first_consumer_session.clone(),
            mid: Mid::from("cam-down-1"),
            source_transport_media_id,
        });
    let _second_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: second_consumer_session.clone(),
            mid: Mid::from("cam-down-2"),
            source_transport_media_id,
        });
    buffers.push_pending_keyframe_request(
        first_consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("cam-down-1"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );
    buffers.push_pending_keyframe_request(
        second_consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("cam-down-2"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    );

    flush_keyframe_effects(&mut state, &metrics, &mut buffers, PacketLoopTime::ZERO);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
    assert!(state.packet_loop.dirty_sessions.contains(&source_session));
    assert_eq!(
        state
            .packet_loop
            .route_control
            .decide_keyframe_request(source_transport_media_id, PacketLoopTime::ZERO),
        KeyframeRequestDecision::Absorb
    );
}
