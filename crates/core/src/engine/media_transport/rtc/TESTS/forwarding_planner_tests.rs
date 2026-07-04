use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use str0m::media::Mid;

use super::fixtures::RuntimeMetricsSnapshotTestExt;
use crate::engine::{
    ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
    media_transport::{
        TransportMediaId, TransportSessionKey,
        rtc::{
            forwarded_packet::ForwardedPacket,
            forwarding_destination::{ForwardingDestination, PacketForward},
            forwarding_planner::plan_forwards as plan_pkt_forwards,
            relay_registry::{RelayPacketMailbox, RelayTargetId},
            route_control::{PacketLayerGate, PacketOperatingPointGate},
            state::PacketLoopState,
            test_support::{
                MediaWorkerScenario, sample_already_relayed_packet, sample_forwarded_packet,
                sample_forwarded_packet_with_frame_mark, sample_forwarded_packet_with_rid,
                test_transport_session_key,
            },
        },
    },
    metrics::{RtpForwardDestinationKind, RuntimeMetrics},
    packet_sink_registry::{
        PacketSink as MediaPacketSink, PacketSinkRouteCache, RoomPacketSinkRegistry,
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

fn populate_forward_routes(
    state: &PacketLoopState,
    packet_sinks: &RoomPacketSinkRegistry,
    metrics: &RuntimeMetrics,
    pending_packets: &mut [ForwardedPacket],
    forwards: &mut Vec<PacketForward>,
) {
    let mut packet_sink_cache = PacketSinkRouteCache::default();
    packet_sink_cache.refresh_from(packet_sinks);
    for (pkt_idx, packet) in pending_packets.iter_mut().enumerate() {
        plan_pkt_forwards(
            state,
            &packet_sink_cache,
            metrics,
            pkt_idx,
            packet,
            forwards,
        );
    }
}

fn local_destination_session<'a>(
    state: &'a PacketLoopState,
    destination: &ForwardingDestination,
) -> Option<&'a TransportSessionKey> {
    let (src_media, dst_idx) = destination.local_route()?;
    state
        .routes
        .local_route(src_media)?
        .destinations
        .get(dst_idx)
        .map(|destination| &destination.dest_session)
}

enum ExpectedForward<'a> {
    Local(&'a TransportSessionKey),
    PacketSink,
    Kind(RtpForwardDestinationKind),
}

fn plan_forwards(
    state: &PacketLoopState,
    packet_sinks: &RoomPacketSinkRegistry,
    metrics: &RuntimeMetrics,
    mut pending_packets: Vec<ForwardedPacket>,
) -> Vec<PacketForward> {
    let mut forwards = Vec::new();
    populate_forward_routes(
        state,
        packet_sinks,
        metrics,
        &mut pending_packets,
        &mut forwards,
    );
    forwards
}

fn assert_forward_plan(
    state: &PacketLoopState,
    forwards: &[PacketForward],
    expected: &[(usize, ExpectedForward<'_>)],
) {
    assert_eq!(forwards.len(), expected.len());
    for (forward, (pkt_idx, destination)) in forwards.iter().zip(expected) {
        assert_eq!(forward.pkt_idx, *pkt_idx);
        match destination {
            ExpectedForward::Local(session) => assert!(matches!(
                &forward.destination,
                destination if local_destination_session(state, destination) == Some(*session)
            )),
            ExpectedForward::PacketSink => assert!(matches!(
                &forward.destination,
                ForwardingDestination::PacketSink(_)
            )),
            ExpectedForward::Kind(kind) => {
                assert_eq!(forward.destination.metrics_kind(), *kind);
            }
        }
    }
}

#[test]
fn plan_forwards_wraps_local_rtc_destinations_in_the_named_contract() {
    let producer_session = TransportSessionKey::new(
        RoomInstanceId::from_raw(12),
        MediaWorkerId::from_raw(0),
        ConnectionId::from_raw(13),
        UserId::Integer(14),
    );
    let consumer_session = TransportSessionKey::new(
        RoomInstanceId::from_raw(12),
        MediaWorkerId::from_raw(0),
        ConnectionId::from_raw(13),
        UserId::Integer(15),
    );
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    scenario.destination(src_media, consumer_session.clone(), Mid::from("aud-down"));
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[(0, ExpectedForward::Local(&consumer_session))],
    );
}

#[test]
fn plan_forwards_keeps_recording_and_local_rtc_destinations_together() {
    let producer_session = test_transport_session_key(21, 0, 22, UserId::Integer(23));
    let consumer_session = test_transport_session_key(21, 0, 22, UserId::Integer(24));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let sink = Arc::new(CountingSink::new());
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    let consumer_media =
        scenario.destination(src_media, consumer_session.clone(), Mid::from("aud-down"));
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::<CountingSink>::clone(&sink),
        RtpForwardDestinationKind::Recording,
    );
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session.clone(),
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (0, ExpectedForward::PacketSink),
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
            ),
        ],
    );
    assert!(
        state
            .routes
            .remove_consumer_route(src_media, &consumer_session, consumer_media)
            .is_some()
    );
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(&state, &forwards, &[(0, ExpectedForward::PacketSink)]);
}

#[test]
fn plan_forwards_reserves_dense_fanout_before_pushing_destinations() {
    const DESTINATION_COUNT: usize = 128;

    let producer_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let consumer_session = test_transport_session_key(25, 0, 28, UserId::Integer(29));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    for _ in 0..DESTINATION_COUNT {
        scenario.destination(src_media, consumer_session.clone(), Mid::from("cam-down"));
    }
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        )],
    );

    assert_eq!(forwards.len(), DESTINATION_COUNT);
    assert!(forwards.capacity() >= DESTINATION_COUNT);
}

#[test]
fn plan_forwards_skips_inactive_consumer_destinations() {
    let producer_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let inactive_consumer_session = test_transport_session_key(29, 0, 32, UserId::Integer(33));
    let active_consumer_session = test_transport_session_key(29, 0, 34, UserId::Integer(35));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    let inactive_transport_media_id = scenario.destination(
        src_media,
        inactive_consumer_session.clone(),
        Mid::from("cam-down-inactive"),
    );
    scenario.destination(
        src_media,
        active_consumer_session.clone(),
        Mid::from("cam-down-active"),
    );

    state
        .routes
        .set_consumer_active(
            src_media,
            0,
            &inactive_consumer_session,
            inactive_transport_media_id,
            false,
        )
        .unwrap();

    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[(0, ExpectedForward::Local(&active_consumer_session))],
    );
}

#[test]
fn plan_forwards_plans_relay_destinations_without_displacing_local_rtc_flush_order() {
    let producer_session = test_transport_session_key(31, 0, 32, UserId::Integer(33));
    let consumer_session = test_transport_session_key(31, 0, 32, UserId::Integer(34));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let recording_sink = Arc::new(CountingSink::new());
    let (first_relay_mailbox, _first_relay_rx) = RelayPacketMailbox::channel_for_test();
    let (second_relay_mailbox, _second_relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    scenario.destination(src_media, consumer_session, Mid::from("aud-down"));
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::<CountingSink>::clone(&recording_sink),
        RtpForwardDestinationKind::Recording,
    );
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(1), first_relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(1), true);
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(2), second_relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(2), true);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (0, ExpectedForward::PacketSink),
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ),
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ),
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
            ),
        ],
    );
}

#[test]
fn plan_forwards_keeps_relay_packets_out_of_recording_and_second_hop_relay_sinks() {
    let producer_session = test_transport_session_key(41, 0, 42, UserId::Integer(43));
    let consumer_session = test_transport_session_key(41, 1, 44, UserId::Integer(45));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let recording_sink = Arc::new(CountingSink::new());
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(51);
    let mut scenario = MediaWorkerScenario::new(&mut state);
    scenario.destination(src_media, consumer_session, Mid::from("aud-down"));
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::<CountingSink>::clone(&recording_sink),
        RtpForwardDestinationKind::Recording,
    );
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(1), true);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_already_relayed_packet(
            producer_session,
            src_media,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[(
            0,
            ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
        )],
    );
}

#[test]
fn plan_forwards_only_relays_the_registered_source_media() {
    let first_producer_session = test_transport_session_key(52, 0, 53, UserId::Integer(54));
    let second_producer_session = test_transport_session_key(52, 0, 53, UserId::Integer(55));
    let remote_consumer_session = test_transport_session_key(52, 1, 56, UserId::Integer(57));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let first_src_media = scenario.source(first_producer_session.clone(), Mid::from("aud-up-1"));
    let second_src_media = scenario.source(second_producer_session.clone(), Mid::from("aud-up-2"));
    scenario.destination(
        first_src_media,
        remote_consumer_session,
        Mid::from("aud-down"),
    );
    state
        .routes
        .add_relay_target(first_src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(first_src_media, RelayTargetId::new(1), true);
    let pending_packets = vec![
        sample_forwarded_packet(first_producer_session, "aud-up-1", b"payload-1"),
        sample_forwarded_packet(second_producer_session, "aud-up-2", b"payload-2"),
    ];
    let mut forwards = Vec::new();
    let mut pending_packets = pending_packets;

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ),
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
            ),
        ],
    );
    assert_eq!(
        pending_packets
            .get_mut(1)
            .and_then(|packet| packet.resolve_src_media(&state)),
        Some(second_src_media)
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "the two-consumer route matrix is clearer as one complete planner regression"
)]
#[test]
fn plan_forwards_enforces_per_consumer_rid_gates_after_aggregate_admits() {
    let producer_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let lo_consumer_session = test_transport_session_key(81, 0, 82, UserId::Integer(84));
    let hi_consumer_session = test_transport_session_key(81, 0, 82, UserId::Integer(85));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination_with_gate(
        src_media,
        lo_consumer_session.clone(),
        Mid::from("cam-down-lo"),
        PacketLayerGate::Rid("lo".into()),
    );
    scenario.destination_with_gate(
        src_media,
        hi_consumer_session.clone(),
        Mid::from("cam-down-hi"),
        PacketLayerGate::Rid("hi".into()),
    );
    let mut pending_packets = vec![
        sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"hi-packet",
        ),
        sample_forwarded_packet_with_rid(producer_session, "cam-up", Some("lo"), b"lo-packet"),
    ];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (0, ExpectedForward::Local(&hi_consumer_session)),
            (1, ExpectedForward::Local(&lo_consumer_session)),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[allow(
    clippy::too_many_lines,
    reason = "the temporal-ceiling route matrix is clearer as one complete planner regression"
)]
#[test]
fn plan_forwards_enforces_per_consumer_temporal_ceilings_after_aggregate_admits() {
    let producer_session = test_transport_session_key(86, 0, 87, UserId::Integer(88));
    let base_consumer_session = test_transport_session_key(86, 0, 87, UserId::Integer(89));
    let high_consumer_session = test_transport_session_key(86, 0, 87, UserId::Integer(90));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination_with_gate(
        src_media,
        base_consumer_session.clone(),
        Mid::from("cam-down-base"),
        PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 0)),
    );
    scenario.destination_with_gate(
        src_media,
        high_consumer_session.clone(),
        Mid::from("cam-down-high"),
        PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 2)),
    );
    let mut pending_packets = vec![
        sample_forwarded_packet_with_frame_mark(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            1_u32 << 24,
            b"temporal-one",
        ),
        sample_forwarded_packet_with_frame_mark(
            producer_session,
            "cam-up",
            Some("hi"),
            0,
            b"temporal-zero",
        ),
    ];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (0, ExpectedForward::Local(&high_consumer_session)),
            (1, ExpectedForward::Local(&base_consumer_session)),
            (1, ExpectedForward::Local(&high_consumer_session)),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[allow(
    clippy::too_many_lines,
    reason = "the two-relay-target matrix is clearer as one complete planner regression"
)]
#[test]
fn plan_forwards_enforces_per_relay_target_gates_after_aggregate_admits() {
    let producer_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (hi_mailbox, _hi_rx) = RelayPacketMailbox::channel_for_test();
    let (lo_mailbox, _lo_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    let hi_target_id = RelayTargetId::new(1);
    let lo_target_id = RelayTargetId::new(2);
    state
        .routes
        .add_relay_target(src_media, hi_target_id, hi_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, hi_target_id, true);
    state
        .routes
        .add_relay_target(src_media, lo_target_id, lo_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, lo_target_id, true);
    state
        .routes
        .set_relay_pkt_gate(src_media, hi_target_id, PacketLayerGate::Rid("hi".into()));
    state
        .routes
        .set_relay_pkt_gate(src_media, lo_target_id, PacketLayerGate::Rid("lo".into()));
    let mut pending_packets = vec![
        sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"hi-packet",
        ),
        sample_forwarded_packet_with_rid(producer_session, "cam-up", Some("lo"), b"lo-packet"),
    ];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (
                0,
                ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ),
            (
                1,
                ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[allow(
    clippy::too_many_lines,
    reason = "the mixed local-plus-remote routing setup is easiest to audit when the full source-to-destination matrix stays inline in one regression test"
)]
#[test]
fn plan_forwards_gates_only_the_selected_source_media() {
    let gated_producer_session = test_transport_session_key(61, 0, 62, UserId::Integer(63));
    let open_producer_session = test_transport_session_key(61, 0, 62, UserId::Integer(64));
    let gated_consumer_session = test_transport_session_key(61, 0, 62, UserId::Integer(65));
    let open_consumer_session = test_transport_session_key(61, 0, 62, UserId::Integer(66));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let recording_sink = Arc::new(CountingSink::new());
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let gated_src_media = scenario.source(gated_producer_session.clone(), Mid::from("cam-up"));
    let open_src_media = scenario.source(open_producer_session.clone(), Mid::from("screen-up"));
    scenario.destination(
        gated_src_media,
        gated_consumer_session,
        Mid::from("cam-down"),
    );
    scenario.destination(
        open_src_media,
        open_consumer_session.clone(),
        Mid::from("screen-down"),
    );
    state
        .routes
        .set_local_pkt_gate(gated_src_media, Some(PacketLayerGate::Rid("hi".into())));
    packet_sink_registry.register_room(
        gated_producer_session.room_instance_id(),
        Arc::<CountingSink>::clone(&recording_sink),
        RtpForwardDestinationKind::Recording,
    );
    state
        .routes
        .add_relay_target(gated_src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(gated_src_media, RelayTargetId::new(1), true);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![
            sample_forwarded_packet_with_rid(
                gated_producer_session,
                "cam-up",
                Some("lo"),
                b"camera-packet",
            ),
            sample_forwarded_packet(open_producer_session, "screen-up", b"screen-packet"),
        ],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            (0, ExpectedForward::PacketSink),
            (1, ExpectedForward::PacketSink),
            (1, ExpectedForward::Local(&open_consumer_session)),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
}

#[test]
fn plan_forwards_applies_operating_point_packet_gates() {
    let producer_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
    let consumer_session = test_transport_session_key(71, 0, 72, UserId::Integer(74));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination(src_media, consumer_session.clone(), Mid::from("cam-down"));
    state.routes.set_local_pkt_gate(
        src_media,
        Some(PacketLayerGate::OperatingPoint(
            PacketOperatingPointGate::new(Some("hi".into()), 1),
        )),
    );
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![
            sample_forwarded_packet_with_frame_mark(
                producer_session.clone(),
                "cam-up",
                Some("hi"),
                2_u32 << 24,
                b"high-temporal",
            ),
            sample_forwarded_packet_with_frame_mark(
                producer_session,
                "cam-up",
                Some("hi"),
                1_u32 << 24,
                b"selected-temporal",
            ),
        ],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[(1, ExpectedForward::Local(&consumer_session))],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
}
