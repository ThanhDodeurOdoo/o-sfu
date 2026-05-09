use str0m::media::{Mid, Pt};

use super::super::{
    demux::{MediaRouteDestination, MediaRouteEntry},
    media_registry::RegisteredMediaHandle,
    route_control::PacketLayerGate,
    state::RtcBootstrapState,
};
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

pub(in crate::runtime::rtc_engine) struct RouteSourceFixture {
    session_key: TransportSessionKey,
    mid: Mid,
    transport_media_id: Option<TransportMediaId>,
    active: bool,
}

impl RouteSourceFixture {
    pub(in crate::runtime::rtc_engine) fn new(session_key: TransportSessionKey, mid: Mid) -> Self {
        Self {
            session_key,
            mid,
            transport_media_id: None,
            active: true,
        }
    }

    pub(in crate::runtime::rtc_engine) fn existing(
        session_key: TransportSessionKey,
        mid: Mid,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            session_key,
            mid,
            transport_media_id: Some(transport_media_id),
            active: true,
        }
    }

    pub(in crate::runtime::rtc_engine) fn install(
        self,
        state: &mut RtcBootstrapState,
    ) -> TransportMediaId {
        let Self {
            session_key,
            mid,
            transport_media_id,
            active,
        } = self;
        let transport_media_id = transport_media_id.unwrap_or_else(|| {
            state.register_media_handle(RegisteredMediaHandle::Producer { session_key, mid })
        });
        state
            .packet_loop
            .media_route_index
            .entry(transport_media_id)
            .and_modify(|route_entry| route_entry.source_active = active)
            .or_insert_with(|| MediaRouteEntry {
                source_active: active,
                destinations: Vec::new(),
            });
        transport_media_id
    }
}

pub(in crate::runtime::rtc_engine) struct RouteDestinationFixture {
    session_key: TransportSessionKey,
    mid: Mid,
    payload_type: Option<Pt>,
    active: bool,
    packet_gate: PacketLayerGate,
    pending_packet_gate: Option<PacketLayerGate>,
}

impl RouteDestinationFixture {
    pub(in crate::runtime::rtc_engine) fn new(session_key: TransportSessionKey, mid: Mid) -> Self {
        Self {
            session_key,
            mid,
            payload_type: None,
            active: true,
            packet_gate: PacketLayerGate::Open,
            pending_packet_gate: None,
        }
    }

    pub(in crate::runtime::rtc_engine) fn packet_gate(
        mut self,
        packet_gate: PacketLayerGate,
    ) -> Self {
        self.packet_gate = packet_gate;
        self
    }

    pub(in crate::runtime::rtc_engine) fn install(
        self,
        state: &mut RtcBootstrapState,
        source_transport_media_id: TransportMediaId,
    ) -> TransportMediaId {
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: self.session_key.clone(),
            mid: self.mid,
            source_transport_media_id,
        });
        state
            .packet_loop
            .media_route_index
            .entry(source_transport_media_id)
            .or_insert_with(|| MediaRouteEntry {
                source_active: true,
                destinations: Vec::new(),
            })
            .destinations
            .push(MediaRouteDestination {
                dest_session: self.session_key,
                dest_transport_media_id: transport_media_id,
                dest_mid: self.mid,
                dest_payload_type: self.payload_type,
                active: self.active,
                packet_gate: self.packet_gate,
                pending_packet_gate: self.pending_packet_gate,
            });
        transport_media_id
    }
}
