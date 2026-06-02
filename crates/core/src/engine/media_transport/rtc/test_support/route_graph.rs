use str0m::media::Mid;

use super::super::{
    demux::MediaRouteDestination, media_registry::RegisteredMediaHandle,
    route_control::PacketLayerGate, slots::ConsumerStreamHandle, state::PacketLoopState,
};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey};

pub(in crate::engine::media_transport::rtc) struct MediaWorkerScenario<'a> {
    state: &'a mut PacketLoopState,
}

impl<'a> MediaWorkerScenario<'a> {
    pub fn new(state: &'a mut PacketLoopState) -> Self {
        Self { state }
    }

    pub fn source(&mut self, session_key: TransportSessionKey, mid: Mid) -> TransportMediaId {
        self.state
            .register_media_handle(RegisteredMediaHandle::Producer { session_key, mid })
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
        let bind_session_key = session_key.clone();
        let destination_index = self.state.routes.add_consumer_route(
            source_transport_media_id,
            MediaRouteDestination {
                dest_session: session_key,
                dest_transport_media_id: transport_media_id,
                dest_stream: ConsumerStreamHandle::default(),
                dest_mid: mid,
                dest_payload_type: None,
                nackable: true,
                active: true,
                packet_gate,
                pending_packet_gate,
            },
        );
        self.state.set_consumer_destination_index(
            &bind_session_key,
            mid,
            transport_media_id,
            source_transport_media_id,
            Some(destination_index),
        );
        transport_media_id
    }
}
