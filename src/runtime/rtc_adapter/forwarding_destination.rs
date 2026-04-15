use std::{fmt, sync::Arc};

use str0m::RtcError;

use crate::runtime::{
    recording::MediaPacketSink,
    transport_adapter::{TransportMediaId, TransportSessionKey},
};

use super::{
    demux::MediaRouteDestination, forwarded_packet::ForwardedPacket,
    local_forwarding::LocalPacketDestination, relay_registry::RelayPacketSink,
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
    Recording(RecordingPacketDestination),
    Relay(RelayPacketDestination),
}

#[derive(Debug, Clone)]
pub(super) struct LocalRtcPacketDestination {
    session_key: TransportSessionKey,
    sender: LocalPacketDestination,
}

#[derive(Clone)]
pub(super) struct RecordingPacketDestination {
    transport_media_id: TransportMediaId,
    sink: Arc<dyn MediaPacketSink>,
}

#[derive(Clone)]
pub(super) struct RelayPacketDestination {
    transport_media_id: TransportMediaId,
    sink: Arc<dyn RelayPacketSink>,
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

    pub(super) fn from_recording_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        sink: Arc<dyn MediaPacketSink>,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::Recording(RecordingPacketDestination {
                transport_media_id,
                sink,
            }),
        }
    }

    pub(super) fn from_relay_sink(
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        sink: Arc<dyn RelayPacketSink>,
    ) -> Self {
        Self {
            packet_idx,
            destination: ForwardingDestination::Relay(RelayPacketDestination {
                transport_media_id,
                sink,
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
            Self::Recording(_) | Self::Relay(_) => None,
        }
    }

    pub(super) fn send(
        &self,
        state: &mut RtcBootstrapState,
        packet: &mut ForwardedPacket,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        match self {
            Self::LocalRtc(destination) => destination.send(state, packet, is_last_destination),
            Self::Recording(destination) => Ok(destination.send(packet)),
            Self::Relay(destination) => Ok(destination.send(packet)),
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
    ) -> Result<Option<usize>, RtcError> {
        let Some(session_state) = state.sessions.get_mut(&self.session_key) else {
            return Ok(None);
        };
        self.sender.send(
            session_state,
            packet.local_send_packet(),
            is_last_destination,
        )
    }
}

impl RecordingPacketDestination {
    fn send(&self, packet: &ForwardedPacket) -> Option<usize> {
        self.sink.record_packet(
            packet.source_session_key(),
            self.transport_media_id,
            packet.received_at(),
            packet.payload().as_slice(),
        );
        None
    }
}

impl RelayPacketDestination {
    fn send(&self, packet: &ForwardedPacket) -> Option<usize> {
        self.sink.forward_packet(packet, self.transport_media_id);
        None
    }
}

impl fmt::Debug for RecordingPacketDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingPacketDestination")
            .field("transport_media_id", &self.transport_media_id)
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Instant;

    use str0m::media::Mid;

    use super::*;
    use crate::runtime::rtc_adapter::relay_registry::RelayPacketSink;
    use crate::runtime::transport_adapter::TransportMediaId;
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

    impl RelayPacketSink for CountingSink {
        fn forward_packet(
            &self,
            _packet: &ForwardedPacket,
            _source_transport_media_id: TransportMediaId,
        ) {
            self.packets.fetch_add(1, Ordering::Relaxed);
        }
    }

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
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::LocalRtc(destination)
                if destination.session_key == dest_session
        ));
    }

    #[test]
    fn packet_forward_wraps_recording_sinks_in_the_named_contract() {
        let sink = Arc::new(CountingSink::new());
        let forward = PacketForward::from_recording_sink(5, TransportMediaId::new(8), sink);

        assert_eq!(forward.packet_idx(), 5);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::Recording(destination)
                if destination.transport_media_id == TransportMediaId::new(8)
        ));
    }

    #[test]
    fn packet_forward_wraps_relay_sinks_in_the_named_contract() {
        let sink = Arc::new(CountingSink::new());
        let forward = PacketForward::from_relay_sink(6, TransportMediaId::new(9), sink);

        assert_eq!(forward.packet_idx(), 6);
        assert!(matches!(
            forward.destination(),
            ForwardingDestination::Relay(destination)
                if destination.transport_media_id == TransportMediaId::new(9)
        ));
    }
}
