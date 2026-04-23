use std::fmt;

use str0m::RtcError;

use crate::runtime::{
    metrics::{RtpForwardDestinationKind, RtpRelayDropKind},
    packet_sink_registry::RegisteredPacketSink,
    transport_adapter::{TransportMediaId, TransportSessionKey},
};

use super::{
    demux::MediaRouteDestination,
    forwarded_packet::ForwardedPacket,
    local_forwarding::LocalPacketDestination,
    relay_registry::{InterNodeRelaySender, RelayEnqueueOutcome, RelayPacketMailbox},
    state::RtcBootstrapState,
};

#[derive(Debug, Clone)]
pub(super) struct PacketForward {
    packet_idx: usize,
    destination: ForwardingDestination,
}

#[derive(Debug, Clone)]
pub(super) enum ForwardingDestination {
    LocalRtc(LocalRtcPacketDestination),
    PacketSink(PacketSinkDestination),
    IntraNodeRelay(IntraNodeRelayPacketDestination),
    InterNodeRelay(InterNodeRelayPacketDestination),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ForwardSendOutcome {
    LocalRtc { payload_bytes: Option<usize> },
    SideEffect,
    OverloadedRelay,
}

#[derive(Debug, Clone)]
pub(super) struct LocalRtcPacketDestination {
    session_key: TransportSessionKey,
    sender: LocalPacketDestination,
}

#[derive(Clone)]
pub(super) struct PacketSinkDestination {
    transport_media_id: TransportMediaId,
    sink: RegisteredPacketSink,
}

#[derive(Clone)]
pub(super) struct IntraNodeRelayPacketDestination {
    transport_media_id: TransportMediaId,
    mailbox: RelayPacketMailbox,
}

#[derive(Clone)]
pub(super) struct InterNodeRelayPacketDestination {
    transport_media_id: TransportMediaId,
    sender: InterNodeRelaySender,
}

impl PacketForward {
    pub(super) fn from_local_route_destination(
        packet_idx: usize,
        route_destination: &MediaRouteDestination,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::LocalRtc(LocalRtcPacketDestination::new(
                route_destination.dest_session.clone(),
                LocalPacketDestination::new(
                    route_destination.dest_transport_media_id,
                    route_destination.dest_mid,
                ),
            )),
        }
    }

    pub(super) fn from_packet_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        sink: RegisteredPacketSink,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::PacketSink(PacketSinkDestination {
                transport_media_id,
                sink,
            }),
        }
    }

    pub(super) fn from_intra_node_relay_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        mailbox: RelayPacketMailbox,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::IntraNodeRelay(IntraNodeRelayPacketDestination {
                transport_media_id,
                mailbox,
            }),
        }
    }

    pub(super) fn from_inter_node_relay_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        sender: InterNodeRelaySender,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::InterNodeRelay(InterNodeRelayPacketDestination {
                transport_media_id,
                sender,
            }),
        }
    }

    pub(super) const fn packet_idx(&self) -> usize {
        self.packet_idx
    }

    pub(super) fn destination(&self) -> &ForwardingDestination {
        &self.destination
    }
}

impl ForwardingDestination {
    #[cfg(test)]
    pub(super) fn session_key(&self) -> Option<&TransportSessionKey> {
        match self {
            Self::LocalRtc(destination) => Some(destination.session_key()),
            Self::PacketSink(_) | Self::IntraNodeRelay(_) | Self::InterNodeRelay(_) => None,
        }
    }

    pub(super) const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        match self {
            Self::LocalRtc(_) => RtpForwardDestinationKind::LocalRtc,
            Self::PacketSink(destination) => destination.metrics_kind(),
            Self::IntraNodeRelay(_) => RtpForwardDestinationKind::IntraNodeRelay,
            Self::InterNodeRelay(_) => RtpForwardDestinationKind::InterNodeRelay,
        }
    }

    pub(super) const fn relay_drop_kind(&self) -> Option<RtpRelayDropKind> {
        match self {
            Self::IntraNodeRelay(_) => Some(RtpRelayDropKind::IntraNodeRelay),
            Self::InterNodeRelay(_) => Some(RtpRelayDropKind::InterNodeRelay),
            Self::LocalRtc(_) | Self::PacketSink(_) => None,
        }
    }

    pub(super) fn send(
        &self,
        state: &mut RtcBootstrapState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<ForwardSendOutcome, RtcError> {
        match self {
            Self::LocalRtc(destination) => destination.send(state, packet, is_last_destination),
            Self::PacketSink(destination) => Ok(destination.send(packet)),
            Self::IntraNodeRelay(destination) => Ok(destination.send(packet)),
            Self::InterNodeRelay(destination) => Ok(destination.send(packet)),
        }
    }
}

impl LocalRtcPacketDestination {
    fn new(session_key: TransportSessionKey, sender: LocalPacketDestination) -> Self {
        Self {
            session_key,
            sender,
        }
    }

    #[cfg(test)]
    fn session_key(&self) -> &TransportSessionKey {
        &self.session_key
    }

    fn send(
        &self,
        state: &mut RtcBootstrapState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<ForwardSendOutcome, RtcError> {
        let Some(session_state) = state.sessions.get_mut(&self.session_key) else {
            return Ok(ForwardSendOutcome::LocalRtc {
                payload_bytes: None,
            });
        };
        let payload_bytes = self.sender.send(
            session_state,
            packet.local_send_packet(),
            is_last_destination,
        )?;
        Ok(ForwardSendOutcome::LocalRtc { payload_bytes })
    }
}

impl PacketSinkDestination {
    const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        self.sink.forward_destination_kind()
    }

    fn send(&self, packet: &ForwardedPacket) -> ForwardSendOutcome {
        self.sink.record_packet(
            packet.source_session_key(),
            self.transport_media_id,
            packet.received_at(),
            packet.payload().as_slice(),
        );
        ForwardSendOutcome::SideEffect
    }
}

impl IntraNodeRelayPacketDestination {
    fn send(&self, packet: &ForwardedPacket) -> ForwardSendOutcome {
        match self.mailbox.forward_packet(packet, self.transport_media_id) {
            RelayEnqueueOutcome::Overloaded => ForwardSendOutcome::OverloadedRelay,
            RelayEnqueueOutcome::Enqueued | RelayEnqueueOutcome::Closed => {
                ForwardSendOutcome::SideEffect
            }
        }
    }
}

impl InterNodeRelayPacketDestination {
    fn send(&self, packet: &ForwardedPacket) -> ForwardSendOutcome {
        match self.sender.forward_packet(packet, self.transport_media_id) {
            RelayEnqueueOutcome::Overloaded => ForwardSendOutcome::OverloadedRelay,
            RelayEnqueueOutcome::Enqueued | RelayEnqueueOutcome::Closed => {
                ForwardSendOutcome::SideEffect
            }
        }
    }
}

impl fmt::Debug for PacketSinkDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PacketSinkDestination")
            .field("transport_media_id", &self.transport_media_id)
            .field(
                "forward_destination_kind",
                &self.sink.forward_destination_kind(),
            )
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for IntraNodeRelayPacketDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntraNodeRelayPacketDestination")
            .field("transport_media_id", &self.transport_media_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for InterNodeRelayPacketDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InterNodeRelayPacketDestination")
            .field("transport_media_id", &self.transport_media_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use str0m::media::Mid;

    use super::*;
    use crate::runtime::recording::MediaPacketSink;
    use crate::runtime::rtc_adapter::{
        relay_registry::InterNodeRelaySender, route_control::PacketLayerGate,
        sample_forwarded_packet, test_support::test_transport_session_key,
    };
    use crate::runtime::transport_adapter::TransportMediaId;
    use o_sfu_protocol::shared::SessionId;

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
    fn packet_forward_wraps_local_route_destinations_in_the_named_contract() {
        let dest_session = test_transport_session_key(11, 0, 12, SessionId::Integer(13));
        let route_destination = MediaRouteDestination {
            dest_session: dest_session.clone(),
            dest_transport_media_id: TransportMediaId::default(),
            dest_mid: Mid::from("aud-down"),
            active: true,
            packet_gate: PacketLayerGate::Open,
        };

        let forward = PacketForward::from_local_route_destination(4, &route_destination);

        assert_eq!(forward.packet_idx(), 4);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::LocalRtc(destination)
                if destination.session_key == dest_session
        ));
    }

    #[test]
    fn packet_forward_wraps_packet_sinks_in_the_named_contract() {
        let sink = RegisteredPacketSink::new(
            Arc::new(CountingSink::new()),
            RtpForwardDestinationKind::Recording,
        );
        let forward = PacketForward::from_packet_sink(5, TransportMediaId::new(8), sink);

        assert_eq!(forward.packet_idx(), 5);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::PacketSink(destination)
                if destination.transport_media_id == TransportMediaId::new(8)
        ));
    }

    #[test]
    fn packet_forward_wraps_intra_node_relay_sinks_in_the_named_contract() {
        let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
        let packet = sample_forwarded_packet(
            test_transport_session_key(11, 0, 12, SessionId::Integer(13)),
            "aud-up",
            b"payload",
        );
        let forward =
            PacketForward::from_intra_node_relay_sink(6, TransportMediaId::new(9), mailbox);
        let mut relay_packet = packet.share_for_relay(TransportMediaId::new(8));

        assert_eq!(forward.packet_idx(), 6);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::IntraNodeRelay(destination)
                if destination.transport_media_id == TransportMediaId::new(9)
        ));
        let _ =
            forward
                .destination()
                .send(&mut RtcBootstrapState::default(), &mut relay_packet, true);
        assert!(relay_rx.try_recv().is_ok());
    }

    #[test]
    fn packet_forward_wraps_inter_node_relay_sinks_in_the_named_contract() {
        let (sender, mut relay_rx) = InterNodeRelaySender::channel_for_test();
        let packet = sample_forwarded_packet(
            test_transport_session_key(21, 0, 22, SessionId::Integer(23)),
            "aud-up",
            b"payload",
        );
        let forward =
            PacketForward::from_inter_node_relay_sink(7, TransportMediaId::new(10), sender);
        let mut relay_packet = packet.share_for_relay(TransportMediaId::new(8));

        assert_eq!(forward.packet_idx(), 7);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::InterNodeRelay(destination)
                if destination.transport_media_id == TransportMediaId::new(10)
        ));
        let _ =
            forward
                .destination()
                .send(&mut RtcBootstrapState::default(), &mut relay_packet, true);
        assert!(relay_rx.try_recv().is_ok());
    }
}
