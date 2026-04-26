//! Packet forwarding planner for the RTC adapter.
//!
//! The planner converts buffered `ForwardedPacket` entries into concrete
//! forwarding destinations after route-control gates have already been
//! projected into packet-facing terms. It owns mechanical fanout only: local
//! RTC destinations, packet sinks such as recording, and relay mailboxes. It
//! does not interpret room layout, receiver budget, or source-policy reasons.

use tracing::debug;

use super::{
    demux::{MediaRouteDestination, MediaRouteEntry},
    forwarded_packet::ForwardedPacket,
    forwarding_destination::PacketForward,
    relay_registry::{ActiveRelayTarget, RelayRegistry, RelayTargetId, RelayTargetTransport},
    route_control::{PacketLayerMetadata, PacketRouteDecision},
    state::RtcBootstrapState,
};
use crate::runtime::{
    metrics::{RtcRouteControlOutcome, RuntimeMetrics},
    packet_sink_registry::RoomPacketSinkRegistry,
    transport_adapter::TransportMediaId as RouteTransportMediaId,
};

pub(super) fn populate_forward_routes(
    state: &RtcBootstrapState,
    packet_sink_registry: &RoomPacketSinkRegistry,
    relay_registry: &RelayRegistry,
    metrics: &RuntimeMetrics,
    pending_packets: &mut [ForwardedPacket],
    forwards: &mut Vec<PacketForward>,
) {
    for (packet_idx, packet) in pending_packets.iter_mut().enumerate() {
        populate_forward_routes_for_packet(
            state,
            packet_sink_registry,
            relay_registry,
            metrics,
            packet_idx,
            packet,
            forwards,
        );
    }
}

fn populate_forward_routes_for_packet(
    state: &RtcBootstrapState,
    packet_sink_registry: &RoomPacketSinkRegistry,
    relay_registry: &RelayRegistry,
    metrics: &RuntimeMetrics,
    packet_idx: usize,
    packet: &mut ForwardedPacket,
    forwards: &mut Vec<PacketForward>,
) {
    let Some(source_transport_media_id) = packet.resolve_source_transport_media_id(state) else {
        return;
    };
    push_origin_sink_forward(
        packet_sink_registry,
        packet_idx,
        packet,
        source_transport_media_id,
        forwards,
    );
    let relay_targets = packet
        .visits_origin_sinks()
        .then(|| relay_registry.targets_for_source(source_transport_media_id))
        .flatten();
    let route_entry = state.media_route_index.get(&source_transport_media_id);
    if !has_routed_forward(relay_targets.as_deref(), route_entry) {
        return;
    }
    let metadata = packet.route_control_layer_metadata(state);
    if !source_packet_gate_permits(state, metrics, source_transport_media_id, metadata) {
        return;
    }
    populate_relay_forwards(
        state,
        relay_targets.as_deref(),
        packet_idx,
        source_transport_media_id,
        metadata,
        forwards,
    );
    if let Some(route_entry) = route_entry {
        populate_local_forwards(
            route_entry,
            packet_idx,
            source_transport_media_id,
            metadata,
            forwards,
        );
    }
}

fn push_origin_sink_forward(
    packet_sink_registry: &RoomPacketSinkRegistry,
    packet_idx: usize,
    packet: &ForwardedPacket,
    source_transport_media_id: RouteTransportMediaId,
    forwards: &mut Vec<PacketForward>,
) {
    if !packet.visits_origin_sinks() {
        return;
    }
    let Some(sink) =
        packet_sink_registry.sink_for_room(packet.source_session_key().room_instance_id())
    else {
        return;
    };
    forwards.push(PacketForward::from_packet_sink(
        packet_idx,
        source_transport_media_id,
        sink,
    ));
}

fn source_packet_gate_permits(
    state: &RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_transport_media_id: RouteTransportMediaId,
    metadata: PacketLayerMetadata,
) -> bool {
    match state
        .route_control
        .decide_packet_route(source_transport_media_id, metadata)
    {
        PacketRouteDecision::Forward => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
            true
        }
        PacketRouteDecision::Drop => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
            false
        }
    }
}

fn populate_relay_forwards(
    state: &RtcBootstrapState,
    relay_targets: Option<&[ActiveRelayTarget<RelayTargetId, RelayTargetTransport>]>,
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    metadata: PacketLayerMetadata,
    forwards: &mut Vec<PacketForward>,
) {
    let Some(relay_targets) = relay_targets else {
        return;
    };
    for relay_target in relay_targets {
        if !relay_target_gate_permits(
            state,
            source_transport_media_id,
            relay_target.target_id(),
            metadata,
        ) {
            continue;
        }
        push_relay_forward(
            packet_idx,
            source_transport_media_id,
            relay_target,
            forwards,
        );
    }
}

fn push_relay_forward(
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    relay_target: &ActiveRelayTarget<RelayTargetId, RelayTargetTransport>,
    forwards: &mut Vec<PacketForward>,
) {
    match relay_target.target().clone() {
        RelayTargetTransport::IntraNodeMailbox(mailbox) => {
            forwards.push(PacketForward::from_intra_node_relay_sink(
                packet_idx,
                source_transport_media_id,
                mailbox,
            ));
        }
        RelayTargetTransport::InterNodeSender(sender) => {
            forwards.push(PacketForward::from_inter_node_relay_sink(
                packet_idx,
                source_transport_media_id,
                sender,
            ));
        }
    }
}

fn populate_local_forwards(
    route_entry: &MediaRouteEntry,
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    metadata: PacketLayerMetadata,
    forwards: &mut Vec<PacketForward>,
) {
    if !route_entry.source_active {
        debug!(
            ?source_transport_media_id,
            "skipped forwarding because source route is inactive"
        );
        return;
    }
    for destination in &route_entry.destinations {
        if destination_packet_gate_permits(source_transport_media_id, destination, metadata) {
            forwards.push(PacketForward::from_local_route_destination(
                packet_idx,
                destination,
            ));
        }
    }
}

fn destination_packet_gate_permits(
    source_transport_media_id: RouteTransportMediaId,
    destination: &MediaRouteDestination,
    metadata: PacketLayerMetadata,
) -> bool {
    if !destination.active {
        debug!(
            ?source_transport_media_id,
            consumer_session_key = ?destination.dest_session,
            consumer_transport_media_id = ?destination.dest_transport_media_id,
            "skipped forwarding because destination route is inactive"
        );
        return false;
    }
    if destination.packet_gate.permits(metadata) {
        return true;
    }
    debug!(
        ?source_transport_media_id,
        consumer_session_key = ?destination.dest_session,
        consumer_transport_media_id = ?destination.dest_transport_media_id,
        ?metadata,
        packet_gate = ?destination.packet_gate,
        "dropped RTP packet by destination packet gate"
    );
    false
}

fn relay_target_gate_permits(
    state: &RtcBootstrapState,
    source_transport_media_id: RouteTransportMediaId,
    target_id: RelayTargetId,
    metadata: PacketLayerMetadata,
) -> bool {
    state
        .route_control
        .relay_packet_gate(source_transport_media_id, target_id)
        .is_none_or(|packet_gate| packet_gate.permits(metadata))
}

fn has_routed_forward(
    relay_targets: Option<&[ActiveRelayTarget<RelayTargetId, RelayTargetTransport>]>,
    route_entry: Option<&MediaRouteEntry>,
) -> bool {
    relay_targets.is_some_and(|targets| !targets.is_empty())
        || route_entry.is_some_and(|entry| {
            entry.source_active
                && entry
                    .destinations
                    .iter()
                    .any(|destination| destination.active)
        })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };

    use o_sfu_protocol::shared::UserId;
    use str0m::media::Mid;

    use super::*;
    use crate::runtime::{
        ConnectionId, RoomInstanceId,
        metrics::{RtpForwardDestinationKind, RuntimeMetrics},
        packet_sink_registry::{
            PacketSink as MediaPacketSink, RoomPacketSinkRegistry as MediaTap, into_packet_sink,
        },
        rtc_adapter::{
            demux::{MediaRouteDestination, MediaRouteEntry},
            forwarding_destination::ForwardingDestination,
            media_registry::RegisteredMediaHandle,
            relay_registry::{
                InterNodeRelaySender, RelayPacketMailbox, RelayRegistry, RelayTargetId,
            },
            route_control::{PacketLayerGate, PacketOperatingPointGate},
            test_support::{
                sample_forwarded_packet, sample_forwarded_packet_with_frame_mark,
                sample_forwarded_packet_with_rid, test_transport_session_key,
            },
        },
        transport_adapter::{TransportMediaId, TransportSessionKey},
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

    trait MediaSource {
        fn activate_room(&self, room_instance_id: RoomInstanceId, sink: Arc<dyn MediaPacketSink>);
    }

    impl MediaSource for MediaTap {
        fn activate_room(&self, room_instance_id: RoomInstanceId, sink: Arc<dyn MediaPacketSink>) {
            self.register_room(room_instance_id, sink, RtpForwardDestinationKind::Recording);
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("aud-up"),
            });
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: Mid::from("aud-down"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: consumer_session.clone(),
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("aud-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
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
            &media_tap,
            &relay_registry,
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let sink = Arc::new(CountingSink::new());
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("aud-up"),
            });
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: Mid::from("aud-down"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: consumer_session,
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("aud-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        media_tap.activate_room(
            producer_session.room_instance_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&sink)),
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
            &media_tap,
            &relay_registry,
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
    fn populate_forward_routes_plans_relay_destinations_without_displacing_local_rtc_flush_order() {
        let producer_session = test_transport_session_key(31, 0, 32, UserId::Integer(33));
        let consumer_session = test_transport_session_key(31, 0, 32, UserId::Integer(34));
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let recording_sink = Arc::new(CountingSink::new());
        let (first_relay_mailbox, _first_relay_rx) = RelayPacketMailbox::channel_for_test();
        let (second_relay_mailbox, _second_relay_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("aud-up"),
            });
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: Mid::from("aud-down"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: consumer_session,
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("aud-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        media_tap.activate_room(
            producer_session.room_instance_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        );
        relay_registry.activate_source_target(
            source_transport_media_id,
            RelayTargetId::new(1),
            first_relay_mailbox.into(),
        );
        relay_registry.set_source_target_active(
            source_transport_media_id,
            RelayTargetId::new(1),
            true,
        );
        relay_registry.activate_source_target(
            source_transport_media_id,
            RelayTargetId::new(2),
            second_relay_mailbox.into(),
        );
        relay_registry.set_source_target_active(
            source_transport_media_id,
            RelayTargetId::new(2),
            true,
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
            &media_tap,
            &relay_registry,
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
            Some(ForwardingDestination::IntraNodeRelay(_))
        ));
        assert!(matches!(
            forwards.get(2).map(PacketForward::destination),
            Some(ForwardingDestination::IntraNodeRelay(_))
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let recording_sink = Arc::new(CountingSink::new());
        let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(51);
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: Mid::from("aud-down"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: consumer_session,
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("aud-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        media_tap.activate_room(
            producer_session.room_instance_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        );
        relay_registry.activate_source_target(
            source_transport_media_id,
            RelayTargetId::new(1),
            relay_mailbox.into(),
        );
        relay_registry.set_source_target_active(
            source_transport_media_id,
            RelayTargetId::new(1),
            true,
        );
        let pending_packets = vec![
            sample_forwarded_packet(producer_session, "aud-up", b"payload")
                .share_for_relay(source_transport_media_id),
        ];
        let mut forwards = Vec::new();
        let mut pending_packets = pending_packets;

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
        let first_source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: first_producer_session.clone(),
                mid: Mid::from("aud-up-1"),
            });
        let second_source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: second_producer_session.clone(),
                mid: Mid::from("aud-up-2"),
            });
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: remote_consumer_session.clone(),
                mid: Mid::from("aud-down"),
                source_transport_media_id: first_source_transport_media_id,
            });
        state.media_route_index.insert(
            first_source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: remote_consumer_session,
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("aud-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        relay_registry.activate_source_target(
            first_source_transport_media_id,
            RelayTargetId::new(1),
            relay_mailbox.into(),
        );
        relay_registry.set_source_target_active(
            first_source_transport_media_id,
            RelayTargetId::new(1),
            true,
        );
        let pending_packets = vec![
            sample_forwarded_packet(first_producer_session, "aud-up-1", b"payload-1"),
            sample_forwarded_packet(second_producer_session, "aud-up-2", b"payload-2"),
        ];
        let mut forwards = Vec::new();
        let mut pending_packets = pending_packets;

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &mut pending_packets,
            &mut forwards,
        );

        assert_eq!(forwards.len(), 2);
        assert!(matches!(
            forwards.first().map(PacketForward::destination),
            Some(ForwardingDestination::IntraNodeRelay(_))
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let (inter_node_sender, _inter_node_rx) = InterNodeRelaySender::channel_for_test();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("aud-up"),
            });

        relay_registry.activate_source_target(
            source_transport_media_id,
            RelayTargetId::new(9),
            inter_node_sender.into(),
        );
        relay_registry.set_source_target_active(
            source_transport_media_id,
            RelayTargetId::new(9),
            true,
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
            &media_tap,
            &relay_registry,
            &metrics,
            &mut pending_packets,
            &mut forwards,
        );

        assert_eq!(forwards.len(), 1);
        assert!(matches!(
            forwards.first().map(PacketForward::destination),
            Some(ForwardingDestination::InterNodeRelay(_))
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("cam-up"),
            });
        let lo_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: lo_consumer_session.clone(),
                mid: Mid::from("cam-down-lo"),
                source_transport_media_id,
            });
        let hi_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: hi_consumer_session.clone(),
                mid: Mid::from("cam-down-hi"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![
                    MediaRouteDestination {
                        dest_session: lo_consumer_session.clone(),
                        dest_transport_media_id: lo_consumer_transport_media_id,
                        dest_mid: Mid::from("cam-down-lo"),
                        active: true,
                        packet_gate: PacketLayerGate::Rid("lo".into()),
                    },
                    MediaRouteDestination {
                        dest_session: hi_consumer_session.clone(),
                        dest_transport_media_id: hi_consumer_transport_media_id,
                        dest_mid: Mid::from("cam-down-hi"),
                        active: true,
                        packet_gate: PacketLayerGate::Rid("hi".into()),
                    },
                ],
            },
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
            &media_tap,
            &relay_registry,
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
        assert_eq!(snapshot.rtc_route_control_layer_allowed, 2);
        assert_eq!(snapshot.rtc_route_control_layer_dropped, 0);
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("cam-up"),
            });
        let base_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: base_consumer_session.clone(),
                mid: Mid::from("cam-down-base"),
                source_transport_media_id,
            });
        let high_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: high_consumer_session.clone(),
                mid: Mid::from("cam-down-high"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![
                    MediaRouteDestination {
                        dest_session: base_consumer_session.clone(),
                        dest_transport_media_id: base_consumer_transport_media_id,
                        dest_mid: Mid::from("cam-down-base"),
                        active: true,
                        packet_gate: PacketLayerGate::OperatingPoint(
                            PacketOperatingPointGate::new(Some("hi".into()), 0),
                        ),
                    },
                    MediaRouteDestination {
                        dest_session: high_consumer_session.clone(),
                        dest_transport_media_id: high_consumer_transport_media_id,
                        dest_mid: Mid::from("cam-down-high"),
                        active: true,
                        packet_gate: PacketLayerGate::OperatingPoint(
                            PacketOperatingPointGate::new(Some("hi".into()), 2),
                        ),
                    },
                ],
            },
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
            &media_tap,
            &relay_registry,
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
        assert_eq!(snapshot.rtc_route_control_layer_allowed, 2);
        assert_eq!(snapshot.rtc_route_control_layer_dropped, 0);
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the heterogeneous relay-target matrix is clearer as one complete planner regression"
    )]
    #[test]
    fn populate_forward_routes_enforces_per_relay_target_gates_after_aggregate_admits() {
        let producer_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let (intra_node_mailbox, _intra_node_rx) = RelayPacketMailbox::channel_for_test();
        let (inter_node_sender, _inter_node_rx) = InterNodeRelaySender::channel_for_test();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("cam-up"),
            });
        let hi_target_id = RelayTargetId::new(1);
        let lo_target_id = RelayTargetId::new(2);
        relay_registry.activate_source_target(
            source_transport_media_id,
            hi_target_id,
            intra_node_mailbox.into(),
        );
        relay_registry.set_source_target_active(source_transport_media_id, hi_target_id, true);
        relay_registry.activate_source_target(
            source_transport_media_id,
            lo_target_id,
            inter_node_sender.into(),
        );
        relay_registry.set_source_target_active(source_transport_media_id, lo_target_id, true);
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
            &media_tap,
            &relay_registry,
            &metrics,
            &mut pending_packets,
            &mut forwards,
        );

        assert_eq!(forwards.len(), 2);
        assert_eq!(forwards.first().map(PacketForward::packet_idx), Some(0));
        assert!(matches!(
            forwards.first().map(PacketForward::destination),
            Some(ForwardingDestination::IntraNodeRelay(_))
        ));
        assert_eq!(forwards.get(1).map(PacketForward::packet_idx), Some(1));
        assert!(matches!(
            forwards.get(1).map(PacketForward::destination),
            Some(ForwardingDestination::InterNodeRelay(_))
        ));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_layer_allowed, 2);
        assert_eq!(snapshot.rtc_route_control_layer_dropped, 0);
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
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let recording_sink = Arc::new(CountingSink::new());
        let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
        let gated_source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: gated_producer_session.clone(),
                mid: Mid::from("cam-up"),
            });
        let open_source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: open_producer_session.clone(),
                mid: Mid::from("screen-up"),
            });
        let gated_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: gated_consumer_session.clone(),
                mid: Mid::from("cam-down"),
                source_transport_media_id: gated_source_transport_media_id,
            });
        let open_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: open_consumer_session.clone(),
                mid: Mid::from("screen-down"),
                source_transport_media_id: open_source_transport_media_id,
            });
        state.media_route_index.insert(
            gated_source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: gated_consumer_session,
                    dest_transport_media_id: gated_consumer_transport_media_id,
                    dest_mid: Mid::from("cam-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        state.media_route_index.insert(
            open_source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: open_consumer_session.clone(),
                    dest_transport_media_id: open_consumer_transport_media_id,
                    dest_mid: Mid::from("screen-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        state.set_local_packet_gate(
            gated_source_transport_media_id,
            PacketLayerGate::Rid("hi".into()),
        );
        media_tap.activate_room(
            gated_producer_session.room_instance_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        );
        relay_registry.activate_source_target(
            gated_source_transport_media_id,
            RelayTargetId::new(1),
            relay_mailbox.into(),
        );
        relay_registry.set_source_target_active(
            gated_source_transport_media_id,
            RelayTargetId::new(1),
            true,
        );
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
            &media_tap,
            &relay_registry,
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
        assert_eq!(snapshot.rtc_route_control_layer_dropped, 1);
        assert_eq!(snapshot.rtc_route_control_layer_allowed, 1);
    }

    #[test]
    fn populate_forward_routes_applies_operating_point_packet_gates() {
        let producer_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
        let consumer_session = test_transport_session_key(71, 0, 72, UserId::Integer(74));
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("cam-up"),
            });
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: Mid::from("cam-down"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: consumer_session.clone(),
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("cam-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
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
            &media_tap,
            &relay_registry,
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
        assert_eq!(snapshot.rtc_route_control_layer_dropped, 1);
        assert_eq!(snapshot.rtc_route_control_layer_allowed, 1);
    }
}
