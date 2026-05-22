//! executable forwarding destinations for one packet-loop flush
//!
//! the forwarding planner records destination intent in this file's types before
//! `flush_forward_routes` executes the side effects in a stable order
//! keeping destinations as data lets planning stay read-only over route state
//! while the flush step owns mutable rtc state, payload sharing and destination
//! metrics

use std::fmt;

use str0m::RtcError;

use super::{
    forwarded_packet::ForwardedPacket,
    local_forwarding::LocalPacketDestination,
    relay_registry::{
        RelayEnqueueOutcome, RelayEnqueueReport, RelayTargetKind, RelayTargetTransport,
    },
    state::PacketLoopState,
};
use crate::runtime::{
    media_transport::TransportMediaId,
    metrics::{RtcRelayEnqueueResult, RtpForwardDestinationKind, RtpRelayDropKind},
    packet_sink_registry::RegisteredPacketSink,
};

/// one planned packet-to-destination edge for the current packet-loop turn
///
/// `packet_idx` points into the turn-local pending packet buffer
/// the value is only valid until the current flush completes, which keeps the
/// hot path from cloning packet payloads while destinations are being planned
#[derive(Debug, Clone)]
pub(super) struct PacketForward {
    packet_idx: usize,
    destination: ForwardingDestination,
}

/// concrete side effect performed for one planned packet
///
/// local rtc destinations may rewrite and enqueue RTP into str0m
/// packet sinks and relay destinations observe or enqueue source packets
/// without taking mutable session state
#[derive(Debug, Clone)]
pub(super) enum ForwardingDestination {
    /// local browser consumer reached through a worker-owned `Rtc`
    LocalRtc(LocalRtcPacketDestination),
    /// room-scoped packet sink such as recording
    PacketSink(PacketSinkDestination),
    /// relay target for another worker or another node
    Relay(RelayPacketDestination),
}

/// flush result used for destination metrics and overload accounting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ForwardSendOutcome {
    /// local rtc send path, with payload bytes only when str0m accepted a write
    LocalRtc { payload_bytes: Option<usize> },
    /// non-local side effect completed or had no stronger delivery signal
    SideEffect,
    /// relay enqueue completed with a concrete target outcome
    RelayEnqueue(RelayEnqueueReport),
}

/// turn-local handle for one local rtc route destination
///
/// the handle names a source route plus the destination index observed while
/// planning the current packet-loop turn
/// it deliberately does not clone the consumer session or RTP rewrite identity
/// those route-stable facts are resolved during flush while `PacketLoopState`
/// is borrowed mutably
///
/// callers must not persist this value beyond the flush that owns the matching
/// `PacketForward`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LocalRtcPacketDestination {
    /// source route that owned the destination when planning ran
    source_transport_media_id: TransportMediaId,
    /// destination slot inside the route for this packet-loop turn
    destination_index: usize,
}

/// packet sink destination tied to the source transport media id
#[derive(Clone)]
pub(super) struct PacketSinkDestination {
    transport_media_id: TransportMediaId,
    sink: RegisteredPacketSink,
}

/// relay destination tied to the source transport media id
#[derive(Clone)]
pub(super) struct RelayPacketDestination {
    transport_media_id: TransportMediaId,
    target: RelayTargetTransport,
}

impl PacketForward {
    /// builds a local rtc destination from an already-authorized route slot
    ///
    /// the caller must pass the destination index produced while walking the
    /// current `MediaRouteEntry`
    /// the index is resolved again during flush so planning can stay clone-free
    pub(super) fn from_local_route_destination(
        packet_idx: usize,
        source_transport_media_id: TransportMediaId,
        destination_index: usize,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::LocalRtc(LocalRtcPacketDestination::new(
                source_transport_media_id,
                destination_index,
            )),
        }
    }

    /// builds a packet sink destination for the source side of a route
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

    /// builds a relay destination for the source side of a route
    pub(super) fn from_relay_target(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        target: RelayTargetTransport,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::Relay(RelayPacketDestination {
                transport_media_id,
                target,
            }),
        }
    }

    /// returns the pending packet buffer index for this planned destination
    pub(super) const fn packet_idx(&self) -> usize {
        self.packet_idx
    }

    /// returns the executable destination for the flush step
    pub(super) fn destination(&self) -> &ForwardingDestination {
        &self.destination
    }
}

impl ForwardingDestination {
    /// exposes the local rtc route handle for route-planner assertions
    #[cfg(test)]
    pub(super) fn local_route(&self) -> Option<LocalRtcPacketDestination> {
        match self {
            Self::LocalRtc(destination) => Some(*destination),
            Self::PacketSink(_) | Self::Relay(_) => None,
        }
    }

    /// maps the destination to the recorder bucket used by flush metrics
    pub(super) const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        match self {
            Self::LocalRtc(_) => RtpForwardDestinationKind::LocalRtc,
            Self::PacketSink(destination) => destination.metrics_kind(),
            Self::Relay(destination) => destination.metrics_kind(),
        }
    }

    /// maps relay destinations to the overload metric namespace
    pub(super) const fn relay_drop_kind(&self) -> Option<RtpRelayDropKind> {
        match self {
            Self::LocalRtc(_) | Self::PacketSink(_) => None,
            Self::Relay(destination) => Some(destination.relay_drop_kind()),
        }
    }

    /// performs this destination's side effect during route flushing
    ///
    /// local rtc sends can return a `RtcError` from str0m
    /// packet sinks and relay destinations collapse their result into
    /// `ForwardSendOutcome` so the packet loop can continue flushing other
    /// destinations
    ///
    /// `is_last_destination` lets the local send path move the payload instead
    /// of cloning it when no later destination for the same packet exists
    pub(super) fn send(
        &self,
        state: &mut PacketLoopState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<ForwardSendOutcome, RtcError> {
        match self {
            Self::LocalRtc(destination) => destination.send(state, packet, is_last_destination),
            Self::PacketSink(destination) => Ok(destination.send(state, packet)),
            Self::Relay(destination) => Ok(destination.send(state, packet)),
        }
    }
}

impl LocalRtcPacketDestination {
    /// stores the compact route handle chosen by route planning
    ///
    /// this constructor is private so only the forwarding planner can create a
    /// local rtc destination after route-control gates have already accepted
    /// the packet
    const fn new(source_transport_media_id: TransportMediaId, destination_index: usize) -> Self {
        Self {
            source_transport_media_id,
            destination_index,
        }
    }

    #[cfg(test)]
    pub(super) const fn source_transport_media_id(self) -> TransportMediaId {
        self.source_transport_media_id
    }

    #[cfg(test)]
    pub(super) const fn destination_index(self) -> usize {
        self.destination_index
    }

    /// writes one packet to the destination session when the route is still live
    ///
    /// the compact handle is best-effort within the current flush
    /// if the route slot or destination session disappeared, the send is a no-op
    /// because cleanup already made the route non-authoritative
    ///
    /// successful writes clone the destination session key only after `str0m`
    /// accepted the packet, so failed or stale local sends do not touch dirty
    /// session scheduling
    fn send(
        &self,
        state: &mut PacketLoopState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<ForwardSendOutcome, RtcError> {
        let (payload_bytes, dirty_session_key) = {
            let Some(route_destination) = state
                .media_route_index
                .get(&self.source_transport_media_id)
                .and_then(|route_entry| route_entry.destinations.get(self.destination_index))
            else {
                return Ok(ForwardSendOutcome::LocalRtc {
                    payload_bytes: None,
                });
            };
            let session_key = &route_destination.dest_session;
            let Some(session_state) = state.users.get_mut(session_key) else {
                return Ok(ForwardSendOutcome::LocalRtc {
                    payload_bytes: None,
                });
            };
            let sender = LocalPacketDestination::new(
                route_destination.dest_transport_media_id,
                route_destination.dest_stream,
                route_destination.dest_mid,
                route_destination.dest_payload_type,
                route_destination.nackable,
            );
            let vp8_payload = packet.local_vp8_payload();
            let payload_bytes = sender.send(
                session_state,
                packet.local_send_packet(),
                vp8_payload,
                is_last_destination,
            )?;
            let dirty_session_key = payload_bytes.is_some().then(|| session_key.clone());
            (payload_bytes, dirty_session_key)
        };
        if let (Some(payload_bytes), Some(session_key)) = (payload_bytes, dirty_session_key) {
            let _ = state.record_egress_bitrate(&session_key, packet.received_at(), payload_bytes);
            state.mark_session_dirty(&session_key);
        }
        Ok(ForwardSendOutcome::LocalRtc { payload_bytes })
    }
}

impl PacketSinkDestination {
    /// uses the sink-provided metric kind instead of exposing sink internals
    const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        self.sink.forward_destination_kind()
    }

    /// records the source packet without mutating rtc session state
    fn send(&self, state: &PacketLoopState, packet: &ForwardedPacket) -> ForwardSendOutcome {
        let Some(source_session_key) = packet.source_session_key(state) else {
            return ForwardSendOutcome::SideEffect;
        };
        self.sink.record_packet(
            source_session_key,
            self.transport_media_id,
            packet.received_at(),
            packet.payload(),
        );
        ForwardSendOutcome::SideEffect
    }
}

impl RelayPacketDestination {
    /// maps the relay transport kind to the recorder bucket used by metrics
    const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        self.target.kind().forward_destination_kind()
    }

    /// maps the relay transport kind to the overload metric namespace
    const fn relay_drop_kind(&self) -> RtpRelayDropKind {
        self.target.kind().relay_drop_kind()
    }

    /// enqueues a shared relay packet for another worker or node
    fn send(&self, state: &PacketLoopState, packet: &ForwardedPacket) -> ForwardSendOutcome {
        self.target
            .forward_packet(state, packet, self.transport_media_id)
            .map_or(
                ForwardSendOutcome::SideEffect,
                ForwardSendOutcome::RelayEnqueue,
            )
    }
}

pub(super) const fn relay_enqueue_result(report: RelayEnqueueReport) -> RtcRelayEnqueueResult {
    match (report.target_kind(), report.outcome()) {
        (RelayTargetKind::IntraNode, RelayEnqueueOutcome::Enqueued) => {
            RtcRelayEnqueueResult::IntraNodeEnqueued
        }
        (RelayTargetKind::IntraNode, RelayEnqueueOutcome::Overloaded) => {
            RtcRelayEnqueueResult::IntraNodeOverloaded
        }
        (RelayTargetKind::IntraNode, RelayEnqueueOutcome::Closed) => {
            RtcRelayEnqueueResult::IntraNodeClosed
        }
        (RelayTargetKind::InterNode, RelayEnqueueOutcome::Enqueued) => {
            RtcRelayEnqueueResult::InterNodeEnqueued
        }
        (RelayTargetKind::InterNode, RelayEnqueueOutcome::Overloaded) => {
            RtcRelayEnqueueResult::InterNodeOverloaded
        }
        (RelayTargetKind::InterNode, RelayEnqueueOutcome::Closed) => {
            RtcRelayEnqueueResult::InterNodeClosed
        }
    }
}

impl RelayTargetKind {
    const fn forward_destination_kind(self) -> RtpForwardDestinationKind {
        match self {
            Self::IntraNode => RtpForwardDestinationKind::IntraNodeRelay,
            Self::InterNode => RtpForwardDestinationKind::InterNodeRelay,
        }
    }

    const fn relay_drop_kind(self) -> RtpRelayDropKind {
        match self {
            Self::IntraNode => RtpRelayDropKind::IntraNodeRelay,
            Self::InterNode => RtpRelayDropKind::InterNodeRelay,
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

impl fmt::Debug for RelayPacketDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayPacketDestination")
            .field("transport_media_id", &self.transport_media_id)
            .field("relay_target_kind", &self.target.kind())
            .finish_non_exhaustive()
    }
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

    use super::*;
    use crate::runtime::{
        UserId,
        media_transport::{TransportMediaId, TransportSessionKey},
        packet_sink_registry::PacketSink as MediaPacketSink,
        rtc_engine::{
            relay_registry::{InterNodeRelaySender, RelayPacketMailbox},
            test_support::{sample_already_relayed_packet, test_transport_session_key},
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

    #[test]
    fn packet_forward_wraps_local_route_destinations_in_the_named_contract() {
        let source_transport_media_id = TransportMediaId::new(7);
        let forward = PacketForward::from_local_route_destination(4, source_transport_media_id, 3);

        assert_eq!(forward.packet_idx(), 4);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::LocalRtc(destination)
                if *destination == LocalRtcPacketDestination::new(source_transport_media_id, 3)
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
        let mut packet = sample_already_relayed_packet(
            test_transport_session_key(11, 0, 12, UserId::Integer(13)),
            TransportMediaId::new(8),
            "aud-up",
            b"payload",
        );
        let forward = PacketForward::from_relay_target(6, TransportMediaId::new(9), mailbox.into());
        let mut state = PacketLoopState::default();

        assert_eq!(forward.packet_idx(), 6);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::Relay(destination)
                if destination.transport_media_id == TransportMediaId::new(9)
                    && destination.target.kind() == RelayTargetKind::IntraNode
        ));
        let _ = forward.destination().send(&mut state, &mut packet, true);
        assert!(relay_rx.try_recv().is_ok());
    }

    #[test]
    fn packet_forward_wraps_inter_node_relay_sinks_in_the_named_contract() {
        let (sender, mut relay_rx) = InterNodeRelaySender::channel_for_test();
        let mut packet = sample_already_relayed_packet(
            test_transport_session_key(21, 0, 22, UserId::Integer(23)),
            TransportMediaId::new(8),
            "aud-up",
            b"payload",
        );
        let forward = PacketForward::from_relay_target(7, TransportMediaId::new(10), sender.into());
        let mut state = PacketLoopState::default();

        assert_eq!(forward.packet_idx(), 7);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::Relay(destination)
                if destination.transport_media_id == TransportMediaId::new(10)
                    && destination.target.kind() == RelayTargetKind::InterNode
        ));
        let _ = forward.destination().send(&mut state, &mut packet, true);
        assert!(relay_rx.try_recv().is_ok());
    }
}
