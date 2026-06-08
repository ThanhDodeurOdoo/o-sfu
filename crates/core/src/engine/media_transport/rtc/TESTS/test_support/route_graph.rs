use str0m::media::Mid;

use super::super::{
    media_registry::RegisteredMediaHandle, route_control::PacketLayerGate,
    slots::ConsumerStreamHandle, source_route::MediaRouteDestination, state::PacketLoopState,
};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey};

pub struct MediaWorkerScenario<'a> {
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
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
    ) -> TransportMediaId {
        self.destination_with_gate(src_media, session_key, mid, PacketLayerGate::Open)
    }

    pub fn destination_with_gate(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
    ) -> TransportMediaId {
        self.install_destination(src_media, session_key, mid, packet_gate, None)
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub fn destination_with_pending_gate(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
    ) -> TransportMediaId {
        self.install_destination(
            src_media,
            session_key,
            mid,
            PacketLayerGate::Open,
            Some(packet_gate),
        )
    }

    fn install_destination(
        &mut self,
        src_media: TransportMediaId,
        session_key: TransportSessionKey,
        mid: Mid,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) -> TransportMediaId {
        let transport_media_id =
            self.state
                .register_media_handle(RegisteredMediaHandle::Consumer {
                    session_key: session_key.clone(),
                    mid,
                    src_media,
                });
        let bind_session_key = session_key.clone();
        let dst_idx = self.state.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: session_key,
                dest_transport_media_id: transport_media_id,
                dest_stream: ConsumerStreamHandle::default(),
                dest_mid: mid,
                dest_payload_type: None,
                nackable: true,
                active: true,
                packet_gate,
                pending_gate,
            },
        );
        self.state.set_consumer_dst_idx(
            &bind_session_key,
            mid,
            transport_media_id,
            src_media,
            Some(dst_idx),
        );
        transport_media_id
    }
}
