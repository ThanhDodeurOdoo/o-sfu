//! executable forwarding destinations for one packet-loop flush
//!
//! the forwarding planner records destination intent in this file's types before
//! `flush_packet_forwards` executes the side effects in a stable order
//! keeping destinations as data lets planning stay read-only over route state
//! while the flush step owns mutable rtc state, payload fanout and destination
//! metrics

use std::fmt;

use super::{
    forwarded_packet::ForwardedPacket,
    local_forwarding::LocalPacketDestination,
    relay_registry::{RelayEnqueueOutcome, RelayEnqueueReport, RelayPacketMailbox},
    state::PacketLoopState,
};
use crate::engine::{
    media_transport::TransportMediaId,
    metrics::{RtcRelayEnqueueResult, RtpForwardDestinationKind, RtpRelayDropKind},
    packet_sink_registry::RegisteredPacketSink,
};

/// concrete side effect performed for one planned packet
///
/// local rtc destinations may rewrite and enqueue RTP into str0m
/// packet sinks and relay destinations observe or enqueue source packets
/// without taking mutable session state
#[derive(Debug)]
pub(super) enum ForwardingDestination {
    /// local browser consumer reached through a worker-local [`str0m::Rtc`]
    LocalRtc(LocalRtcPacketDestination),
    /// room-scoped packet sink such as recording
    PacketSink(PacketSinkDestination),
    /// relay target for another local worker
    Relay(RelayPacketDestination),
}

/// flush result used for destination metrics and overload accounting
#[derive(Debug)]
pub(super) enum ForwardSendOutcome {
    /// local rtc send path, with payload bytes only when str0m queued a write
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
/// destination
#[derive(Debug, Copy, Clone)]
pub(super) struct LocalRtcPacketDestination {
    /// source route that owned the destination when planning ran
    src_media: TransportMediaId,
    /// destination slot inside the route for this packet-loop turn
    dst_idx: usize,
}

/// packet sink destination tied to the source transport media id
pub(super) struct PacketSinkDestination {
    transport_media_id: TransportMediaId,
    sink: RegisteredPacketSink,
}

/// relay destination tied to the source transport media id
pub(super) struct RelayPacketDestination {
    transport_media_id: TransportMediaId,
    target: RelayPacketMailbox,
}

impl ForwardingDestination {
    /// builds a local rtc destination from an already-authorized route slot
    ///
    /// the caller must pass the destination index produced while walking the
    /// current `MediaRouteEntry`
    /// the index is resolved again during flush so planning can stay clone-free
    pub(super) fn from_local_route_destination(
        src_media: TransportMediaId,
        dst_idx: usize,
    ) -> Self {
        Self::LocalRtc(LocalRtcPacketDestination::new(src_media, dst_idx))
    }

    /// builds a packet sink destination for the source side of a route
    pub(super) fn from_packet_sink(
        transport_media_id: TransportMediaId,
        sink: RegisteredPacketSink,
    ) -> Self {
        Self::PacketSink(PacketSinkDestination {
            transport_media_id,
            sink,
        })
    }

    /// builds a relay destination for the source side of a route
    pub(super) fn from_relay_target(
        transport_media_id: TransportMediaId,
        target: RelayPacketMailbox,
    ) -> Self {
        Self::Relay(RelayPacketDestination {
            transport_media_id,
            target,
        })
    }

    /// exposes the local rtc route handle for route-planner assertions
    #[cfg(test)]
    pub(super) const fn local_route(&self) -> Option<(TransportMediaId, usize)> {
        match self {
            Self::LocalRtc(destination) => Some((destination.src_media, destination.dst_idx)),
            Self::PacketSink(_) | Self::Relay(_) => None,
        }
    }

    /// maps the destination to the recorder bucket used by flush metrics
    pub(super) const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        match self {
            Self::LocalRtc(_) => RtpForwardDestinationKind::LocalRtc,
            Self::PacketSink(destination) => destination.metrics_kind(),
            Self::Relay(_) => RtpForwardDestinationKind::IntraNodeRelay,
        }
    }

    /// maps relay destinations to the overload metric namespace
    pub(super) const fn relay_drop_kind(&self) -> Option<RtpRelayDropKind> {
        match self {
            Self::LocalRtc(_) | Self::PacketSink(_) => None,
            Self::Relay(_) => Some(RtpRelayDropKind::IntraNodeRelay),
        }
    }

    /// performs this destination's side effect during route flushing
    ///
    /// local rtc sends enqueue into str0m when the route remains live
    /// packet sinks and relay destinations collapse their result into
    /// `ForwardSendOutcome` so the packet loop can continue flushing other
    /// destinations
    pub(super) fn send(
        &self,
        state: &mut PacketLoopState,
        packet: &ForwardedPacket,
    ) -> ForwardSendOutcome {
        match self {
            Self::LocalRtc(destination) => destination.send(state, packet),
            Self::PacketSink(destination) => destination.send(state, packet),
            Self::Relay(destination) => destination.send(state, packet),
        }
    }
}

impl LocalRtcPacketDestination {
    /// stores the compact route handle chosen by route planning
    ///
    /// this constructor is private so only the forwarding planner can create a
    /// local rtc destination after route-control gates have already accepted
    /// the packet
    const fn new(src_media: TransportMediaId, dst_idx: usize) -> Self {
        Self { src_media, dst_idx }
    }

    /// writes one packet to the destination session when the route is still live
    ///
    /// the compact handle is best-effort within the current flush
    /// if the route slot or destination session disappeared, the send is a no-op
    /// because cleanup already made the route non-authoritative
    ///
    /// successful writes clone the destination session key only after `str0m`
    /// queues the packet, so stale local sends do not touch dirty
    /// session scheduling
    fn send(&self, state: &mut PacketLoopState, packet: &ForwardedPacket) -> ForwardSendOutcome {
        let (payload_bytes, session_key) = {
            let Some(route_destination) = state
                .routes
                .local_route(self.src_media)
                .and_then(|route_entry| route_entry.destinations.get(self.dst_idx))
            else {
                return ForwardSendOutcome::LocalRtc {
                    payload_bytes: None,
                };
            };
            let session_key = &route_destination.dest_session;
            let Some(session_state) = state.users.get_mut(session_key) else {
                return ForwardSendOutcome::LocalRtc {
                    payload_bytes: None,
                };
            };
            let sender = LocalPacketDestination::new(
                route_destination.dest_transport_media_id,
                route_destination.dest_stream,
                route_destination.delivery_generation,
                route_destination.dest_mid,
                route_destination.dest_payload_type,
            );
            let vp8_payload = packet.local_vp8_payload();
            let Some(payload_bytes) =
                sender.send(session_state, &packet.local_send_packet(), vp8_payload)
            else {
                return ForwardSendOutcome::LocalRtc {
                    payload_bytes: None,
                };
            };
            session_state
                .egress_bitrate
                .record(packet.received_at(), payload_bytes);
            (payload_bytes, session_key.clone())
        };
        state.mark_session_dirty(&session_key);
        ForwardSendOutcome::LocalRtc {
            payload_bytes: Some(payload_bytes),
        }
    }
}

impl PacketSinkDestination {
    /// uses the sink-provided metric kind instead of exposing sink internals
    const fn metrics_kind(&self) -> RtpForwardDestinationKind {
        self.sink.forward_destination_kind()
    }

    /// records the source packet without mutating rtc session state
    fn send(&self, state: &PacketLoopState, packet: &ForwardedPacket) -> ForwardSendOutcome {
        let Some(src_key) = packet.src_key(state) else {
            return ForwardSendOutcome::SideEffect;
        };
        self.sink.record_packet(
            src_key,
            self.transport_media_id,
            packet.received_at(),
            packet.payload(),
        );
        ForwardSendOutcome::SideEffect
    }
}

impl RelayPacketDestination {
    /// enqueues a shared relay packet for another local worker
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
    match report.outcome {
        RelayEnqueueOutcome::Enqueued => RtcRelayEnqueueResult::IntraNodeEnqueued,
        RelayEnqueueOutcome::Overloaded => RtcRelayEnqueueResult::IntraNodeOverloaded,
        RelayEnqueueOutcome::Closed => RtcRelayEnqueueResult::IntraNodeClosed,
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
            .finish_non_exhaustive()
    }
}
