use super::{
    ConsumerId, ConsumerRouteState, MediaKind, ProducerId, ProducerRouteState, TransportId,
};

/// Media sink attached to a send transport inside the pure router.
///
/// A consumer records the routing edge from one producer to one downstream
/// transport. It stores two route-control inputs because the receiver's local
/// subscription choice and the producer's source pause have different owners.
///
/// `route_state` is the receiver-local choice. `producer_route_state` is the
/// source shadow copied from the producer by the router. Keeping both values
/// lets compatibility code report the same two pause axes without deriving one
/// from the other.
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
    /// Builds an active consumer edge for an existing producer and send transport.
    ///
    /// The producer shadow starts as active here because only the router can
    /// look up the current producer state safely. [`Router::add_consumer`](super::Router::add_consumer)
    /// updates the shadow before the consumer becomes visible.
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

    /// Returns the receiver-local route state as a compatibility paused flag.
    ///
    /// This does not include producer-side pause. Callers that need the source
    /// shadow should use [`Consumer::producer_paused`] or
    /// [`Consumer::producer_route_state`].
    #[must_use]
    pub fn paused(&self) -> bool {
        self.route_state.is_paused()
    }

    /// Returns the producer shadow as a compatibility paused flag.
    ///
    /// This value changes when the source producer route state changes. It does
    /// not describe the consumer's own subscription choice.
    #[must_use]
    pub fn producer_paused(&self) -> bool {
        self.producer_route_state.is_paused()
    }

    /// Returns the route state selected by this consumer's owner.
    #[must_use]
    pub fn route_state(&self) -> ConsumerRouteState {
        self.route_state
    }

    /// Returns the producer route state currently shadowed on this consumer.
    #[must_use]
    pub fn producer_route_state(&self) -> ProducerRouteState {
        self.producer_route_state
    }

    /// Returns a copy of this consumer with a different receiver-local route state.
    ///
    /// This builder is for fixtures and staged values. Mutating a live router
    /// consumer should go through
    /// [`Router::set_consumer_route_state`](super::Router::set_consumer_route_state) so the
    /// router remains the only owner of indexed state changes.
    #[must_use]
    pub fn with_route_state(mut self, route_state: ConsumerRouteState) -> Self {
        self.route_state = route_state;
        self
    }

    /// Returns a copy of this consumer with a different producer shadow.
    ///
    /// This is mainly for fixtures that construct staged values. Production code should let
    /// [`Router::add_consumer`](super::Router::add_consumer) and
    /// [`Router::set_producer_route_state`](super::Router::set_producer_route_state)
    /// manage producer shadowing.
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
