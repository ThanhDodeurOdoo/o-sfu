use std::collections::BTreeMap;

use super::super::{
    relay_registry::{RelayTargetId, RelayTargetTransport},
    state::RtcBootstrapState,
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::TransportMediaId,
    packet_sink_registry::{
        PacketSinkRouteCache, PacketSinkRouteRef, RegisteredPacketSink, RoomPacketSinkRegistry,
    },
};

#[derive(Default)]
pub struct PacketLoopRouteSnapshot {
    relay_routes: BTreeMap<TransportMediaId, Vec<PacketLoopRelayRoute>>,
    relay_transports: Vec<RelayTargetTransport>,
    relay_generation: Option<u64>,
    packet_sinks: PacketSinkRouteCache,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::rtc_engine) struct PacketLoopRelayRoute {
    target_id: RelayTargetId,
    route_ref: RelayRouteRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_engine) enum RelayRouteRef {
    IntraNode(usize),
    InterNode(usize),
}

impl PacketLoopRouteSnapshot {
    pub(in crate::runtime::rtc_engine) fn refresh_from(
        &mut self,
        state: &RtcBootstrapState,
        packet_sink_registry: &RoomPacketSinkRegistry,
    ) {
        self.packet_sinks.refresh_from(packet_sink_registry);
        self.refresh_relay_routes(state);
    }

    pub(in crate::runtime::rtc_engine) fn packet_sink_route_for_room(
        &self,
        room_instance_id: RoomInstanceId,
    ) -> Option<PacketSinkRouteRef> {
        self.packet_sinks.route_for_room(room_instance_id)
    }

    pub(in crate::runtime::rtc_engine) fn packet_sink(
        &self,
        route_ref: PacketSinkRouteRef,
    ) -> Option<&RegisteredPacketSink> {
        self.packet_sinks.sink_for_route(route_ref)
    }

    pub(in crate::runtime::rtc_engine) fn relay_routes_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&[PacketLoopRelayRoute]> {
        self.relay_routes
            .get(&source_transport_media_id)
            .map(Vec::as_slice)
            .filter(|routes| !routes.is_empty())
    }

    pub(in crate::runtime::rtc_engine) fn relay_transport(
        &self,
        route_ref: RelayRouteRef,
    ) -> Option<&RelayTargetTransport> {
        let index = match route_ref {
            RelayRouteRef::IntraNode(index) | RelayRouteRef::InterNode(index) => index,
        };
        self.relay_transports.get(index)
    }

    fn refresh_relay_routes(&mut self, state: &RtcBootstrapState) {
        let relay_generation = state.packet_loop.relay_topology_generation();
        if self.relay_generation == Some(relay_generation) {
            return;
        }
        self.relay_generation = Some(relay_generation);
        self.relay_routes.clear();
        self.relay_transports.clear();
        for (source_transport_media_id, registration) in &state.packet_loop.relay_targets {
            for target in registration.active_targets_slice() {
                let route_ref = self.push_relay_transport(target.target().clone());
                self.relay_routes
                    .entry(*source_transport_media_id)
                    .or_default()
                    .push(PacketLoopRelayRoute::new(target.target_id(), route_ref));
            }
        }
    }

    fn push_relay_transport(&mut self, transport: RelayTargetTransport) -> RelayRouteRef {
        let index = self.relay_transports.len();
        let route_ref = match &transport {
            RelayTargetTransport::IntraNodeMailbox(_) => RelayRouteRef::IntraNode(index),
            RelayTargetTransport::InterNodeSender(_) => RelayRouteRef::InterNode(index),
        };
        self.relay_transports.push(transport);
        route_ref
    }
}

impl PacketLoopRelayRoute {
    fn new(target_id: RelayTargetId, route_ref: RelayRouteRef) -> Self {
        Self {
            target_id,
            route_ref,
        }
    }

    pub(in crate::runtime::rtc_engine) const fn target_id(self) -> RelayTargetId {
        self.target_id
    }

    pub(in crate::runtime::rtc_engine) const fn route_ref(self) -> RelayRouteRef {
        self.route_ref
    }
}
