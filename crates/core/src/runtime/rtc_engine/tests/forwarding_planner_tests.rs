use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use str0m::media::Mid;

use super::{
    super::forwarding_planner::populate_forward_routes_for_packet,
    fixtures::RuntimeMetricsSnapshotTestExt,
};
use crate::runtime::{
    ConnectionId, RoomInstanceId, UserId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{RtpForwardDestinationKind, RuntimeMetrics},
    packet_sink_registry::{
        PacketSink as MediaPacketSink, PacketSinkLookup, RoomPacketSinkRegistry, into_packet_sink,
    },
    rtc_engine::{
        forwarded_packet::ForwardedPacket,
        forwarding_destination::{ForwardingDestination, PacketForward},
        relay_registry::{InterNodeRelaySender, RelayPacketMailbox, RelayTargetId},
        route_control::{PacketLayerGate, PacketOperatingPointGate},
        state::PacketLoopState,
        test_support::{
            MediaWorkerScenario, sample_forwarded_packet, sample_forwarded_packet_with_frame_mark,
            sample_forwarded_packet_with_rid, test_transport_session_key,
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

fn populate_forward_routes(
    state: &PacketLoopState,
    packet_sinks: &impl PacketSinkLookup,
    metrics: &RuntimeMetrics,
    pending_packets: &mut [ForwardedPacket],
    forwards: &mut Vec<PacketForward>,
) {
    for (packet_idx, packet) in pending_packets.iter_mut().enumerate() {
        populate_forward_routes_for_packet(
            state,
            packet_sinks,
            metrics,
            packet_idx,
            packet,
            forwards,
        );
    }
}

#[test]
fn populate_forward_routes_wraps_local_rtc_destinations_in_the_named_contract() {
    let producer_session = TransportSessionKey::new(
        RoomInstanceId::from_raw(12),
        0,
        ConnectionId::from_raw(13),
        UserId::Integer(14),
    );
    let consumer_session = TransportSessionKey::new(
        RoomInstanceId::from_raw(12),
        0,
        ConnectionId::from_raw(13),
        UserId::Integer(15),
    );
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    scenario.destination(
        source_transport_media_id,
        consumer_session.clone(),
        Mid::from("aud-down"),
    );
    let pending_packets = vec![sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    )];
    let mut forwards = Vec::new();
    let mut pending_packets = pending_packets;

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_eq!(forwards.len(), 1);
    assert_eq!(forwards.first().map(PacketForward::packet_idx), Some(0));
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination)
            if destination.session_key() == Some(&consumer_session)
    ));
}

#[test]
fn populate_forward_routes_keeps_recording_and_local_rtc_destinations_together() {
    let producer_session = test_transport_session_key(21, 0, 22, UserId::Integer(23));
    let consumer_session = test_transport_session_key(21, 0, 22, UserId::Integer(24));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let sink = Arc::new(CountingSink::new());
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    scenario.destination(
        source_transport_media_id,
        consumer_session,
        Mid::from("aud-down"),
    );
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        RtpForwardDestinationKind::Recording,
    );
    let pending_packets = vec![sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    )];
    let mut forwards = Vec::new();
    let mut pending_packets = pending_packets;

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_eq!(forwards.len(), 2);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(ForwardingDestination::PacketSink(_))
    ));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(ForwardingDestination::LocalRtc(_))
    ));
}

#[test]
fn populate_forward_routes_reserves_dense_fanout_before_pushing_destinations() {
    const DESTINATION_COUNT: usize = 128;

    let producer_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let consumer_session = test_transport_session_key(25, 0, 28, UserId::Integer(29));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    for _ in 0..DESTINATION_COUNT {
        scenario.destination(
            source_transport_media_id,
            consumer_session.clone(),
            Mid::from("cam-down"),
        );
    }
    let mut pending_packets = vec![sample_forwarded_packet(
        producer_session,
        "cam-up",
        b"payload",
    )];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_eq!(forwards.len(), DESTINATION_COUNT);
    assert!(forwards.capacity() >= DESTINATION_COUNT);
}

#[test]
fn populate_forward_routes_plans_relay_destinations_without_displacing_local_rtc_flush_order() {
    let producer_session = test_transport_session_key(31, 0, 32, UserId::Integer(33));
    let consumer_session = test_transport_session_key(31, 0, 32, UserId::Integer(34));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let recording_sink = Arc::new(CountingSink::new());
    let (first_relay_mailbox, _first_relay_rx) = RelayPacketMailbox::channel_for_test();
    let (second_relay_mailbox, _second_relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    scenario.destination(
        source_transport_media_id,
        consumer_session,
        Mid::from("aud-down"),
    );
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        RtpForwardDestinationKind::Recording,
    );
    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(1),
        first_relay_mailbox.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(1), true);
    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(2),
        second_relay_mailbox.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(2), true);
    let pending_packets = vec![sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    )];
    let mut forwards = Vec::new();
    let mut pending_packets = pending_packets;

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_eq!(forwards.len(), 4);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(ForwardingDestination::PacketSink(_))
    ));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(destination)
            if destination.metrics_kind() == RtpForwardDestinationKind::IntraNodeRelay
    ));
    assert!(matches!(
        forwards.get(2).map(PacketForward::destination),
        Some(destination)
            if destination.metrics_kind() == RtpForwardDestinationKind::IntraNodeRelay
    ));
    assert!(matches!(
        forwards.get(3).map(PacketForward::destination),
        Some(ForwardingDestination::LocalRtc(_))
    ));
}

#[test]
fn populate_forward_routes_keeps_relay_packets_out_of_recording_and_second_hop_relay_sinks() {
    let producer_session = test_transport_session_key(41, 0, 42, UserId::Integer(43));
    let consumer_session = test_transport_session_key(41, 1, 44, UserId::Integer(45));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let recording_sink = Arc::new(CountingSink::new());
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id = TransportMediaId::new(51);
    let mut scenario = MediaWorkerScenario::new(&mut state);
    scenario.existing_source(source_transport_media_id);
    scenario.destination(
        source_transport_media_id,
        consumer_session,
        Mid::from("aud-down"),
    );
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        RtpForwardDestinationKind::Recording,
    );
    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(1),
        relay_mailbox.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(1), true);
    let pending_packets = vec![
        sample_forwarded_packet(producer_session, "aud-up", b"payload")
            .share_for_relay(source_transport_media_id),
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

    assert_eq!(forwards.len(), 1);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(ForwardingDestination::LocalRtc(_))
    ));
}

#[test]
fn populate_forward_routes_only_relays_the_registered_source_media() {
    let first_producer_session = test_transport_session_key(52, 0, 53, UserId::Integer(54));
    let second_producer_session = test_transport_session_key(52, 0, 53, UserId::Integer(55));
    let remote_consumer_session = test_transport_session_key(52, 1, 56, UserId::Integer(57));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let first_source_transport_media_id =
        scenario.source(first_producer_session.clone(), Mid::from("aud-up-1"));
    let second_source_transport_media_id =
        scenario.source(second_producer_session.clone(), Mid::from("aud-up-2"));
    scenario.destination(
        first_source_transport_media_id,
        remote_consumer_session,
        Mid::from("aud-down"),
    );
    state.add_relay_target(
        first_source_transport_media_id,
        RelayTargetId::new(1),
        relay_mailbox.into(),
    );
    state.set_relay_target_active(first_source_transport_media_id, RelayTargetId::new(1), true);
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

    assert_eq!(forwards.len(), 2);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination)
            if destination.metrics_kind() == RtpForwardDestinationKind::IntraNodeRelay
    ));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(ForwardingDestination::LocalRtc(_))
    ));
    assert_eq!(
        pending_packets
            .get_mut(1)
            .and_then(|packet| packet.resolve_source_transport_media_id(&state)),
        Some(second_source_transport_media_id)
    );
}

#[test]
fn populate_forward_routes_plans_inter_node_relay_targets_without_new_packet_shape() {
    let producer_session = test_transport_session_key(58, 0, 59, UserId::Integer(60));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (inter_node_sender, _inter_node_rx) = InterNodeRelaySender::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("aud-up"));

    state.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(9),
        inter_node_sender.into(),
    );
    state.set_relay_target_active(source_transport_media_id, RelayTargetId::new(9), true);

    let pending_packets = vec![sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    )];
    let mut forwards = Vec::new();
    let mut pending_packets = pending_packets;

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_eq!(forwards.len(), 1);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination)
            if destination.metrics_kind() == RtpForwardDestinationKind::InterNodeRelay
    ));
}

#[allow(
    clippy::too_many_lines,
    reason = "the two-consumer route matrix is clearer as one complete planner regression"
)]
#[test]
fn populate_forward_routes_enforces_per_consumer_rid_gates_after_aggregate_admits() {
    let producer_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let lo_consumer_session = test_transport_session_key(81, 0, 82, UserId::Integer(84));
    let hi_consumer_session = test_transport_session_key(81, 0, 82, UserId::Integer(85));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination_with_gate(
        source_transport_media_id,
        lo_consumer_session.clone(),
        Mid::from("cam-down-lo"),
        PacketLayerGate::Rid("lo".into()),
    );
    scenario.destination_with_gate(
        source_transport_media_id,
        hi_consumer_session.clone(),
        Mid::from("cam-down-hi"),
        PacketLayerGate::Rid("hi".into()),
    );
    state
        .route_control
        .set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
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

    assert_eq!(forwards.len(), 2);
    assert_eq!(forwards.first().map(PacketForward::packet_idx), Some(0));
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination) if destination.session_key() == Some(&hi_consumer_session)
    ));
    assert_eq!(forwards.get(1).map(PacketForward::packet_idx), Some(1));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(destination) if destination.session_key() == Some(&lo_consumer_session)
    ));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[allow(
    clippy::too_many_lines,
    reason = "the temporal-ceiling route matrix is clearer as one complete planner regression"
)]
#[test]
fn populate_forward_routes_enforces_per_consumer_temporal_ceilings_after_aggregate_admits() {
    let producer_session = test_transport_session_key(86, 0, 87, UserId::Integer(88));
    let base_consumer_session = test_transport_session_key(86, 0, 87, UserId::Integer(89));
    let high_consumer_session = test_transport_session_key(86, 0, 87, UserId::Integer(90));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination_with_gate(
        source_transport_media_id,
        base_consumer_session.clone(),
        Mid::from("cam-down-base"),
        PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 0)),
    );
    scenario.destination_with_gate(
        source_transport_media_id,
        high_consumer_session.clone(),
        Mid::from("cam-down-high"),
        PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 2)),
    );
    state.route_control.set_local_packet_gate(
        source_transport_media_id,
        Some(PacketLayerGate::OperatingPoint(
            PacketOperatingPointGate::new(Some("hi".into()), 2),
        )),
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

    assert_eq!(forwards.len(), 3);
    assert_eq!(forwards.first().map(PacketForward::packet_idx), Some(0));
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination) if destination.session_key() == Some(&high_consumer_session)
    ));
    assert_eq!(forwards.get(1).map(PacketForward::packet_idx), Some(1));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(destination) if destination.session_key() == Some(&base_consumer_session)
    ));
    assert_eq!(forwards.get(2).map(PacketForward::packet_idx), Some(1));
    assert!(matches!(
        forwards.get(2).map(PacketForward::destination),
        Some(destination) if destination.session_key() == Some(&high_consumer_session)
    ));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[allow(
    clippy::too_many_lines,
    reason = "the heterogeneous relay-target matrix is clearer as one complete planner regression"
)]
#[test]
fn populate_forward_routes_enforces_per_relay_target_gates_after_aggregate_admits() {
    let producer_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (intra_node_mailbox, _intra_node_rx) = RelayPacketMailbox::channel_for_test();
    let (inter_node_sender, _inter_node_rx) = InterNodeRelaySender::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    let hi_target_id = RelayTargetId::new(1);
    let lo_target_id = RelayTargetId::new(2);
    state.add_relay_target(
        source_transport_media_id,
        hi_target_id,
        intra_node_mailbox.into(),
    );
    state.set_relay_target_active(source_transport_media_id, hi_target_id, true);
    state.add_relay_target(
        source_transport_media_id,
        lo_target_id,
        inter_node_sender.into(),
    );
    state.set_relay_target_active(source_transport_media_id, lo_target_id, true);
    state.route_control.set_relay_packet_gate(
        source_transport_media_id,
        hi_target_id,
        PacketLayerGate::Rid("hi".into()),
    );
    state.route_control.set_relay_packet_gate(
        source_transport_media_id,
        lo_target_id,
        PacketLayerGate::Rid("lo".into()),
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

    assert_eq!(forwards.len(), 2);
    assert_eq!(forwards.first().map(PacketForward::packet_idx), Some(0));
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination)
            if destination.metrics_kind() == RtpForwardDestinationKind::IntraNodeRelay
    ));
    assert_eq!(forwards.get(1).map(PacketForward::packet_idx), Some(1));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(destination)
            if destination.metrics_kind() == RtpForwardDestinationKind::InterNodeRelay
    ));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[allow(
    clippy::too_many_lines,
    reason = "the mixed local-plus-remote routing setup is easiest to audit when the full source-to-destination matrix stays inline in one regression test"
)]
#[test]
fn populate_forward_routes_gates_only_the_selected_source_media() {
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
    let gated_source_transport_media_id =
        scenario.source(gated_producer_session.clone(), Mid::from("cam-up"));
    let open_source_transport_media_id =
        scenario.source(open_producer_session.clone(), Mid::from("screen-up"));
    scenario.destination(
        gated_source_transport_media_id,
        gated_consumer_session,
        Mid::from("cam-down"),
    );
    scenario.destination(
        open_source_transport_media_id,
        open_consumer_session.clone(),
        Mid::from("screen-down"),
    );
    state.set_local_packet_gate(
        gated_source_transport_media_id,
        PacketLayerGate::Rid("hi".into()),
    );
    packet_sink_registry.register_room(
        gated_producer_session.room_instance_id(),
        into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        RtpForwardDestinationKind::Recording,
    );
    state.add_relay_target(
        gated_source_transport_media_id,
        RelayTargetId::new(1),
        relay_mailbox.into(),
    );
    state.set_relay_target_active(gated_source_transport_media_id, RelayTargetId::new(1), true);
    let pending_packets = vec![
        sample_forwarded_packet_with_rid(
            gated_producer_session,
            "cam-up",
            Some("lo"),
            b"camera-packet",
        ),
        sample_forwarded_packet(open_producer_session, "screen-up", b"screen-packet"),
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

    assert_eq!(forwards.len(), 3);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(ForwardingDestination::PacketSink(_))
    ));
    assert!(matches!(
        forwards.get(1).map(PacketForward::destination),
        Some(ForwardingDestination::PacketSink(_))
    ));
    assert!(matches!(
        forwards.get(2).map(PacketForward::destination),
        Some(destination)
            if destination.session_key() == Some(&open_consumer_session)
    ));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
}

#[test]
fn populate_forward_routes_applies_operating_point_packet_gates() {
    let producer_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
    let consumer_session = test_transport_session_key(71, 0, 72, UserId::Integer(74));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let source_transport_media_id = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination(
        source_transport_media_id,
        consumer_session.clone(),
        Mid::from("cam-down"),
    );
    state.set_local_packet_gate(
        source_transport_media_id,
        PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 1)),
    );
    let mut pending_packets = vec![
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
    ];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_eq!(forwards.len(), 1);
    assert!(matches!(
        forwards.first().map(PacketForward::destination),
        Some(destination) if destination.session_key() == Some(&consumer_session)
    ));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
}
