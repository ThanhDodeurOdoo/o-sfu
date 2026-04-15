use crate::runtime::metrics::{RtcRouteControlOutcome, RuntimeMetrics};
use crate::runtime::recording::MediaTap;

use super::{
    forwarded_packet::ForwardedPacket,
    forwarding_destination::PacketForward,
    relay_registry::{RelayRegistry, RelayTargetTransport},
    route_control::PacketRouteDecision,
    state::RtcBootstrapState,
};

pub(super) fn populate_forward_routes(
    state: &RtcBootstrapState,
    media_tap: &MediaTap,
    relay_registry: &RelayRegistry,
    metrics: &RuntimeMetrics,
    pending_packets: &[ForwardedPacket],
    forwards: &mut Vec<PacketForward>,
) {
    for (packet_idx, packet) in pending_packets.iter().enumerate() {
        let Some(source_transport_media_id) = packet.resolve_source_transport_media_id(state)
        else {
            continue;
        };
        if packet.uses_channel_side_sinks()
            && let Some(sink) =
                media_tap.sink_for_channel(packet.source_session_key().channel_runtime_id())
        {
            forwards.push(PacketForward::from_recording_sink(
                packet_idx,
                source_transport_media_id,
                sink,
            ));
        }
        let relay_targets = if packet.uses_channel_side_sinks() {
            relay_registry.targets_for_source(source_transport_media_id)
        } else {
            None
        };
        let route_entry = state.media_route_index.get(&source_transport_media_id);
        if has_routed_forward(relay_targets.as_deref(), route_entry) {
            match state
                .route_control
                .decide_packet_route(source_transport_media_id, packet.route_control_rid())
            {
                PacketRouteDecision::Forward => {
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
                }
                PacketRouteDecision::Drop => {
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
                    continue;
                }
            }
        }
        if let Some(relay_targets) = relay_targets {
            for relay_target in relay_targets.iter().cloned() {
                match relay_target {
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
        }
        let Some(route_entry) = route_entry else {
            continue;
        };
        if !route_entry.source_active {
            continue;
        }
        for destination in &route_entry.destinations {
            if !destination.active {
                continue;
            }
            forwards.push(PacketForward::from_local_route_destination(
                packet_idx,
                destination,
            ));
        }
    }
}

fn has_routed_forward(
    relay_targets: Option<&[RelayTargetTransport]>,
    route_entry: Option<&super::demux::MediaRouteEntry>,
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Instant;

    use str0m::media::Mid;

    use super::*;
    use crate::runtime::metrics::RuntimeMetrics;
    use crate::runtime::recording::{MediaPacketSink, MediaSource, MediaTap, into_packet_sink};
    use crate::runtime::rtc_adapter::{
        demux::{MediaRouteDestination, MediaRouteEntry},
        forwarding_destination::ForwardingDestination,
        media_registry::RegisteredMediaHandle,
        relay_registry::{InterNodeRelaySender, RelayPacketMailbox, RelayRegistry, RelayTargetId},
        route_control::PacketLayerGate,
        sample_forwarded_packet, sample_forwarded_packet_with_rid,
    };
    use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};
    use crate::signaling::shared::SessionId;

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

    #[test]
    fn populate_forward_routes_wraps_local_rtc_destinations_in_the_named_contract() {
        let producer_session = TransportSessionKey::new(12, 0, 13, SessionId::Integer(14));
        let consumer_session = TransportSessionKey::new(12, 0, 13, SessionId::Integer(15));
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

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
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
        let producer_session = TransportSessionKey::new(21, 0, 22, SessionId::Integer(23));
        let consumer_session = TransportSessionKey::new(21, 0, 22, SessionId::Integer(24));
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
        media_tap.activate_channel(
            producer_session.channel_runtime_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        );
        let pending_packets = vec![sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        )];
        let mut forwards = Vec::new();

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
            &mut forwards,
        );

        assert_eq!(forwards.len(), 2);
        assert!(matches!(
            forwards.first().map(PacketForward::destination),
            Some(ForwardingDestination::Recording(_))
        ));
        assert!(matches!(
            forwards.get(1).map(PacketForward::destination),
            Some(ForwardingDestination::LocalRtc(_))
        ));
    }

    #[test]
    fn populate_forward_routes_plans_relay_destinations_without_displacing_local_rtc_flush_order() {
        let producer_session = TransportSessionKey::new(31, 0, 32, SessionId::Integer(33));
        let consumer_session = TransportSessionKey::new(31, 0, 32, SessionId::Integer(34));
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
        media_tap.activate_channel(
            producer_session.channel_runtime_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        );
        relay_registry.activate_source_target(
            producer_session.channel_runtime_id(),
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
            producer_session.channel_runtime_id(),
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

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
            &mut forwards,
        );

        assert_eq!(forwards.len(), 4);
        assert!(matches!(
            forwards.first().map(PacketForward::destination),
            Some(ForwardingDestination::Recording(_))
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
        let producer_session = TransportSessionKey::new(41, 0, 42, SessionId::Integer(43));
        let consumer_session = TransportSessionKey::new(41, 1, 44, SessionId::Integer(45));
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
        media_tap.activate_channel(
            producer_session.channel_runtime_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        );
        relay_registry.activate_source_target(
            producer_session.channel_runtime_id(),
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

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
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
        let first_producer_session = TransportSessionKey::new(52, 0, 53, SessionId::Integer(54));
        let second_producer_session = TransportSessionKey::new(52, 0, 53, SessionId::Integer(55));
        let remote_consumer_session = TransportSessionKey::new(52, 1, 56, SessionId::Integer(57));
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
            first_producer_session.channel_runtime_id(),
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

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
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
                .get(1)
                .and_then(|packet| packet.resolve_source_transport_media_id(&state)),
            Some(second_source_transport_media_id)
        );
    }

    #[test]
    fn populate_forward_routes_plans_inter_node_relay_targets_without_new_packet_shape() {
        let producer_session = TransportSessionKey::new(58, 0, 59, SessionId::Integer(60));
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
            producer_session.channel_runtime_id(),
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

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
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
        reason = "the mixed local-plus-remote routing setup is easiest to audit when the full source-to-destination matrix stays inline in one regression test"
    )]
    #[test]
    fn populate_forward_routes_gates_only_the_selected_source_media() {
        let gated_producer_session = TransportSessionKey::new(61, 0, 62, SessionId::Integer(63));
        let open_producer_session = TransportSessionKey::new(61, 0, 62, SessionId::Integer(64));
        let gated_consumer_session = TransportSessionKey::new(61, 0, 62, SessionId::Integer(65));
        let open_consumer_session = TransportSessionKey::new(61, 0, 62, SessionId::Integer(66));
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
        state.set_source_packet_gate(
            gated_source_transport_media_id,
            PacketLayerGate::Rid("hi".into()),
        );
        media_tap.activate_channel(
            gated_producer_session.channel_runtime_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&recording_sink)),
        );
        relay_registry.activate_source_target(
            gated_producer_session.channel_runtime_id(),
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

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &pending_packets,
            &mut forwards,
        );

        assert_eq!(forwards.len(), 3);
        assert!(matches!(
            forwards.first().map(PacketForward::destination),
            Some(ForwardingDestination::Recording(_))
        ));
        assert!(matches!(
            forwards.get(1).map(PacketForward::destination),
            Some(ForwardingDestination::Recording(_))
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
}
