use super::{
    ConsumerId, ConsumerRouteState, MediaKind, ProducerId, ProducerRouteState, TransportId,
};

/// media sink attached to a send transport inside the pure router
///
/// a consumer records the routing edge from one producer to one downstream
/// transport. receiver-local pause and producer-side pause are separate
/// route-control inputs because compatibility state reports both axes
///
/// [`Consumer::route_state`] is the receiver-local choice
/// [`Consumer::producer_route_state`] is the source shadow copied from the
/// producer by the router
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Consumer {
    id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    route_state: ConsumerRouteState,
    producer_route_state: ProducerRouteState,
}

impl Consumer {
    #[must_use]
    pub(super) fn new(
        id: ConsumerId,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
        route_state: ConsumerRouteState,
        producer_route_state: ProducerRouteState,
    ) -> Self {
        Self {
            id,
            producer_id,
            transport_id,
            media_kind,
            route_state,
            producer_route_state,
        }
    }

    #[cfg(any(test, feature = "test-support", kani))]
    #[must_use]
    pub fn id(&self) -> ConsumerId {
        self.id
    }

    #[must_use]
    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    #[must_use]
    pub fn transport_id(&self) -> TransportId {
        self.transport_id
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn route_state(&self) -> ConsumerRouteState {
        self.route_state
    }

    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn producer_route_state(&self) -> ProducerRouteState {
        self.producer_route_state
    }

    pub(super) fn set_route_state(&mut self, route_state: ConsumerRouteState) {
        self.route_state = route_state;
    }

    pub(super) fn set_producer_route_state(&mut self, producer_route_state: ProducerRouteState) {
        self.producer_route_state = producer_route_state;
    }
}
