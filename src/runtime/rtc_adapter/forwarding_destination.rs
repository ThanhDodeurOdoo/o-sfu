use str0m::RtcError;

use crate::runtime::transport_adapter::TransportSessionKey;

use super::{
    demux::MediaRouteDestination, forwarded_packet::ForwardedPacket,
    local_forwarding::LocalPacketDestination, state::RtcSessionState,
};

#[derive(Debug, Clone)]
pub(super) struct PacketForward {
    packet_idx: usize,
    destination: ForwardingDestination,
}

#[derive(Debug, Clone)]
pub(super) enum ForwardingDestination {
    LocalRtc(LocalRtcPacketDestination),
}

#[derive(Debug, Clone)]
pub(super) struct LocalRtcPacketDestination {
    session_key: TransportSessionKey,
    sender: LocalPacketDestination,
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
                LocalPacketDestination::new(route_destination.dest_mid),
            )),
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
    pub(super) fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::LocalRtc(destination) => destination.session_key(),
        }
    }

    pub(super) fn send(
        &self,
        session_state: &mut RtcSessionState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        match self {
            Self::LocalRtc(destination) => {
                destination.send(session_state, packet, is_last_destination)
            }
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

    fn session_key(&self) -> &TransportSessionKey {
        &self.session_key
    }

    fn send(
        &self,
        session_state: &mut RtcSessionState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        self.sender.send(
            session_state,
            packet.local_send_packet(),
            is_last_destination,
        )
    }
}

#[cfg(test)]
mod tests {
    use str0m::media::Mid;

    use super::*;
    use crate::runtime::transport_adapter::TransportMediaId;
    use crate::signaling::shared::SessionId;

    #[test]
    fn packet_forward_wraps_local_route_destinations_in_the_named_contract() {
        let dest_session = TransportSessionKey::new(11, 0, 12, SessionId::Integer(13));
        let route_destination = MediaRouteDestination {
            dest_session: dest_session.clone(),
            dest_transport_media_id: TransportMediaId::default(),
            dest_mid: Mid::from("aud-down"),
            active: true,
        };

        let forward = PacketForward::from_local_route_destination(4, &route_destination);

        assert_eq!(forward.packet_idx(), 4);
        match forward.destination() {
            ForwardingDestination::LocalRtc(destination) => {
                assert_eq!(destination.session_key(), &dest_session);
            }
        }
    }
}
