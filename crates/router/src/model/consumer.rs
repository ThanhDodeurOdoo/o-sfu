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
    /// build an active consumer edge for an existing producer and send transport
    ///
    /// only [`Router::add_consumer`](super::Router::add_consumer) may make the
    /// edge visible because it has the current producer shadow
    #[must_use]
    pub fn new(
        id: ConsumerId,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
    ) -> Self {
        Self {
            id,
            producer_id,
            transport_id,
            media_kind,
            route_state: ConsumerRouteState::Active,
            producer_route_state: ProducerRouteState::Active,
        }
    }

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

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    /// receiver-local route state as a compatibility paused flag
    ///
    /// this does not include producer-side pause
    #[must_use]
    pub fn paused(&self) -> bool {
        self.route_state.is_paused()
    }

    /// producer shadow as a compatibility paused flag
    ///
    /// this does not describe the consumer's own subscription choice
    #[must_use]
    pub fn producer_paused(&self) -> bool {
        self.producer_route_state.is_paused()
    }

    #[must_use]
    pub fn route_state(&self) -> ConsumerRouteState {
        self.route_state
    }

    #[must_use]
    pub fn producer_route_state(&self) -> ProducerRouteState {
        self.producer_route_state
    }

    /// staged copy with a different receiver-local route state
    ///
    /// live router mutation must go through
    /// [`Router::set_consumer_route_state`](super::Router::set_consumer_route_state)
    #[must_use]
    pub fn with_route_state(mut self, route_state: ConsumerRouteState) -> Self {
        self.route_state = route_state;
        self
    }

    /// staged copy with a different producer shadow
    ///
    /// production code should let
    /// [`Router::add_consumer`](super::Router::add_consumer) and
    /// [`Router::set_producer_route_state`](super::Router::set_producer_route_state)
    /// manage producer shadowing
    #[must_use]
    pub fn with_producer_route_state(mut self, producer_route_state: ProducerRouteState) -> Self {
        self.producer_route_state = producer_route_state;
        self
    }

    pub(super) fn set_route_state(&mut self, route_state: ConsumerRouteState) {
        self.route_state = route_state;
    }

    pub(super) fn set_producer_route_state(&mut self, producer_route_state: ProducerRouteState) {
        self.producer_route_state = producer_route_state;
    }
}
