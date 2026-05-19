use str0m::media::Mid;

use super::super::{
    demux::{MediaRouteDestination, MediaRouteEntry},
    media_registry::RegisteredMediaHandle,
    route_control::PacketLayerGate,
    slots::ConsumerStreamHandle,
    state::PacketLoopState,
};
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

pub(in crate::runtime::rtc_engine) struct MediaWorkerScenario<'a> {
    state: &'a mut PacketLoopState,
}

impl<'a> MediaWorkerScenario<'a> {
    pub fn new(state: &'a mut PacketLoopState) -> Self {
        Self { state }
    }

    pub fn source(&mut self, session_key: TransportSessionKey, mid: Mid) -> TransportMediaId {
        let transport_media_id = self
            .state
            .register_media_handle(RegisteredMediaHandle::Producer { session_key, mid });
        self.install_source_route(transport_media_id);
        transport_media_id
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub fn existing_source(&mut self, transport_media_id: TransportMediaId) -> TransportMediaId {
        self.install_source_route(transport_media_id);
        transport_media_id
    }

    pub fn destination(
        &mut self,
        source_transport_media_id: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
    ) -> TransportMediaId {
        self.destination_with_gate(
            source_transport_media_id,
            session_key,
            mid,
            PacketLayerGate::Open,
        )
    }

    pub fn destination_with_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
    ) -> TransportMediaId {
        self.install_destination(
            source_transport_media_id,
            session_key,
            mid,
            packet_gate,
            None,
        )
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub fn destination_with_pending_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
    ) -> TransportMediaId {
        self.install_destination(
            source_transport_media_id,
            session_key,
            mid,
            PacketLayerGate::Open,
            Some(packet_gate),
        )
    }

    fn install_source_route(&mut self, transport_media_id: TransportMediaId) {
        self.state
            .media_route_index
            .entry(transport_media_id)
            .and_modify(|route_entry| route_entry.source_active = true)
            .or_insert_with(|| MediaRouteEntry::new(true));
    }

    fn install_destination(
        &mut self,
        source_transport_media_id: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
        pending_packet_gate: Option<PacketLayerGate>,
    ) -> TransportMediaId {
        let transport_media_id =
            self.state
                .register_media_handle(RegisteredMediaHandle::Consumer {
                    session_key: session_key.clone(),
                    mid,
                    source_transport_media_id,
                });
        self.state
            .media_route_index
            .entry(source_transport_media_id)
            .or_insert_with(|| MediaRouteEntry::new(true))
            .push_destination(MediaRouteDestination {
                dest_session: session_key,
                dest_transport_media_id: transport_media_id,
                dest_stream: ConsumerStreamHandle::default(),
                dest_mid: mid,
                dest_payload_type: None,
                nackable: true,
                active: true,
                packet_gate,
                pending_packet_gate,
            });
        transport_media_id
    }
}
