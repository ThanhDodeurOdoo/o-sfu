use str0m::RtcError;

use super::{
    demux::MediaRouteDestination,
    forwarded_packet::ForwardedPacket,
    local_forwarding::LocalPacketDestination,
    packet_loop::route_snapshot::{PacketLoopRouteSnapshot, RelayRouteRef},
    relay_registry::{RelayEnqueueOutcome, RelayTargetTransport},
    state::RtcBootstrapState,
};
use crate::runtime::{
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{RtpForwardDestinationKind, RtpRelayDropKind},
    packet_sink_registry::PacketSinkRouteRef,
};

#[derive(Debug, Clone)]
pub(super) struct PacketForward {
    pub(super) packet_idx: usize,
    pub(super) destination: ForwardingDestination,
}

#[derive(Debug, Clone)]
pub(super) enum ForwardingDestination {
    LocalRtc {
        session_key: TransportSessionKey,
        sender: LocalPacketDestination,
    },
    PacketSink {
        transport_media_id: TransportMediaId,
        route_ref: PacketSinkRouteRef,
    },
    Relay {
        transport_media_id: TransportMediaId,
        route_ref: RelayRouteRef,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ForwardSendOutcome {
    LocalRtc { payload_bytes: Option<usize> },
    SideEffect,
    OverloadedRelay,
}

impl PacketForward {
    pub(super) fn from_local_route_destination(
        packet_idx: usize,
        route_destination: &MediaRouteDestination,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::LocalRtc {
                session_key: route_destination.dest_session.clone(),
                sender: LocalPacketDestination::new(
                    route_destination.dest_transport_media_id,
                    route_destination.dest_mid,
                    route_destination.dest_payload_type,
                ),
            },
        }
    }

    pub(super) fn from_packet_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        route_ref: PacketSinkRouteRef,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::PacketSink {
                transport_media_id,
                route_ref,
            },
        }
    }

    pub(super) fn from_relay_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        route_ref: RelayRouteRef,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::Relay {
                transport_media_id,
                route_ref,
            },
        }
    }
}

impl ForwardingDestination {
    #[cfg(test)]
    pub(super) fn session_key(&self) -> Option<&TransportSessionKey> {
        match self {
            Self::LocalRtc { session_key, .. } => Some(session_key),
            Self::PacketSink { .. } | Self::Relay { .. } => None,
        }
    }

    pub(super) fn metrics_kind(&self) -> RtpForwardDestinationKind {
        match self {
            Self::LocalRtc { .. } => RtpForwardDestinationKind::LocalRtc,
            Self::PacketSink { route_ref, .. } => route_ref.forward_destination_kind(),
            Self::Relay { route_ref, .. } => relay_metrics_kind(*route_ref),
        }
    }

    pub(super) const fn relay_drop_kind(&self) -> Option<RtpRelayDropKind> {
        match self {
            Self::Relay { route_ref, .. } => Some(relay_drop_kind(*route_ref)),
            Self::LocalRtc { .. } | Self::PacketSink { .. } => None,
        }
    }

    pub(super) fn send(
        &self,
        state: &mut RtcBootstrapState,
        routes: &PacketLoopRouteSnapshot,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<ForwardSendOutcome, RtcError> {
        match self {
            Self::LocalRtc {
                session_key,
                sender,
            } => send_local_rtc_packet(state, session_key, sender, packet, is_last_destination),
            Self::PacketSink {
                transport_media_id,
                route_ref,
            } => Ok(send_packet_sink(
                routes,
                *transport_media_id,
                *route_ref,
                packet,
            )),
            Self::Relay {
                transport_media_id,
                route_ref,
            } => Ok(send_relay_packet(
                routes,
                *transport_media_id,
                *route_ref,
                packet,
            )),
        }
    }
}

fn send_local_rtc_packet(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    sender: &LocalPacketDestination,
    packet: &mut ForwardedPacket,
    is_last_destination: bool,
) -> Result<ForwardSendOutcome, RtcError> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Ok(ForwardSendOutcome::LocalRtc {
            payload_bytes: None,
        });
    };
    let payload_bytes = sender.send(
        session_state,
        packet.local_send_packet(),
        is_last_destination,
    )?;
    if let Some(payload_bytes) = payload_bytes {
        let _ = state.record_egress_bitrate(session_key, packet.received_at(), payload_bytes);
        state.packet_loop.mark_session_dirty(session_key);
    }
    Ok(ForwardSendOutcome::LocalRtc { payload_bytes })
}

fn send_packet_sink(
    routes: &PacketLoopRouteSnapshot,
    transport_media_id: TransportMediaId,
    route_ref: PacketSinkRouteRef,
    packet: &ForwardedPacket,
) -> ForwardSendOutcome {
    let Some(sink) = routes.packet_sink(route_ref) else {
        return ForwardSendOutcome::SideEffect;
    };
    sink.record_packet(
        packet.source_session_key(),
        transport_media_id,
        packet.received_at(),
        packet.payload(),
    );
    ForwardSendOutcome::SideEffect
}

fn send_relay_packet(
    routes: &PacketLoopRouteSnapshot,
    transport_media_id: TransportMediaId,
    route_ref: RelayRouteRef,
    packet: &ForwardedPacket,
) -> ForwardSendOutcome {
    match (route_ref, routes.relay_transport(route_ref)) {
        (RelayRouteRef::IntraNode(_), Some(RelayTargetTransport::IntraNodeMailbox(mailbox))) => {
            relay_send_outcome(mailbox.forward_packet(packet, transport_media_id))
        }
        (RelayRouteRef::InterNode(_), Some(RelayTargetTransport::InterNodeSender(sender))) => {
            relay_send_outcome(sender.forward_packet(packet, transport_media_id))
        }
        _ => ForwardSendOutcome::SideEffect,
    }
}

const fn relay_metrics_kind(route_ref: RelayRouteRef) -> RtpForwardDestinationKind {
    match route_ref {
        RelayRouteRef::IntraNode(_) => RtpForwardDestinationKind::IntraNodeRelay,
        RelayRouteRef::InterNode(_) => RtpForwardDestinationKind::InterNodeRelay,
    }
}

const fn relay_drop_kind(route_ref: RelayRouteRef) -> RtpRelayDropKind {
    match route_ref {
        RelayRouteRef::IntraNode(_) => RtpRelayDropKind::IntraNodeRelay,
        RelayRouteRef::InterNode(_) => RtpRelayDropKind::InterNodeRelay,
    }
}

fn relay_send_outcome(outcome: RelayEnqueueOutcome) -> ForwardSendOutcome {
    match outcome {
        RelayEnqueueOutcome::Overloaded => ForwardSendOutcome::OverloadedRelay,
        RelayEnqueueOutcome::Enqueued | RelayEnqueueOutcome::Closed => {
            ForwardSendOutcome::SideEffect
        }
    }
}
