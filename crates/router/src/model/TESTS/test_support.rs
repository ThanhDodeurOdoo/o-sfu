//! read-only router state inspection for proofs and subsystem tests
//!
//! this module is the stable inspection boundary for router tests
//! it exposes detached snapshots and invariant predicates, not mutation helpers
//! that keeps
//! test code from becoming a second router facade while still letting proofs
//! state exact topology obligations

use super::{
    ConsumerId, ConsumerRouteState, MediaKind, ProducerId, ProducerRouteState, Router, RouterId,
    RouterObserver, SessionId, SessionState, TransportDirection, TransportId,
};

/// read-only view over one detached reverse relation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationSnapshot<'a, K, V> {
    /// owner-to-dependent entries copied from the router index
    entries: &'a [(K, Vec<V>)],
}

impl<'a, K, V> RelationSnapshot<'a, K, V>
where
    K: Copy + Eq,
    V: Copy + Eq,
{
    fn new(entries: &'a [(K, Vec<V>)]) -> Self {
        Self { entries }
    }

    /// count relation owner keys in this snapshot
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// report whether one owner key has any indexed dependents
    #[must_use]
    pub fn contains_key(&self, key: K) -> bool {
        self.entries
            .iter()
            .any(|(relation_key, _)| *relation_key == key)
    }

    /// report exact relation membership
    #[must_use]
    pub fn contains(&self, key: K, value: V) -> bool {
        self.entries
            .iter()
            .find(|(relation_key, _)| *relation_key == key)
            .is_some_and(|(_, values)| values.contains(&value))
    }
}

/// detached router read model for assertions
///
/// snapshots copy the primary maps and reverse indexes at one point in time
/// they deliberately expose predicate methods instead of tuple fields so tests
/// express router facts without depending on storage layout
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterStateSnapshot {
    /// router identity copied from the inspected router
    id: RouterId,
    /// session ids plus lifecycle state at snapshot time
    sessions: Vec<(SessionId, SessionState)>,
    /// transport ownership plus direction at snapshot time
    transports: Vec<(TransportId, SessionId, TransportDirection)>,
    /// producer ownership, media kind and source route state at snapshot time
    producers: Vec<(ProducerId, TransportId, MediaKind, ProducerRouteState)>,
    /// consumer ownership, media kind and both route-state axes at snapshot time
    consumers: Vec<(
        ConsumerId,
        ProducerId,
        TransportId,
        MediaKind,
        ConsumerRouteState,
        ProducerRouteState,
    )>,
    /// session-to-transport reverse relation at snapshot time
    session_transports: Vec<(SessionId, Vec<TransportId>)>,
    /// transport-to-producer reverse relation at snapshot time
    transport_producers: Vec<(TransportId, Vec<ProducerId>)>,
    /// transport-to-consumer reverse relation at snapshot time
    transport_consumers: Vec<(TransportId, Vec<ConsumerId>)>,
    /// producer-to-consumer reverse relation at snapshot time
    producer_consumers: Vec<(ProducerId, Vec<ConsumerId>)>,
}

impl RouterStateSnapshot {
    /// count live sessions captured in the primary session map
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// count live transports captured in the primary transport map
    #[must_use]
    pub fn transport_count(&self) -> usize {
        self.transports.len()
    }

    /// count live producers captured in the primary producer map
    #[must_use]
    pub fn producer_count(&self) -> usize {
        self.producers.len()
    }

    /// count live consumers captured in the primary consumer map
    #[must_use]
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// report whether the primary session map contained the session
    #[must_use]
    pub fn contains_session(&self, session_id: SessionId) -> bool {
        self.sessions.iter().any(|(id, _)| *id == session_id)
    }

    /// report whether the primary transport map contained the transport
    #[must_use]
    pub fn contains_transport(&self, transport_id: TransportId) -> bool {
        self.transport(transport_id).is_some()
    }

    /// report whether the primary producer map contained the producer
    #[must_use]
    pub fn contains_producer(&self, producer_id: ProducerId) -> bool {
        self.producer(producer_id).is_some()
    }

    /// report whether the primary consumer map contained the consumer
    #[must_use]
    pub fn contains_consumer(&self, consumer_id: ConsumerId) -> bool {
        self.consumer(consumer_id).is_some()
    }

    /// assert the captured source-side route state for a producer
    #[must_use]
    pub fn producer_route_state_matches(
        &self,
        producer_id: ProducerId,
        route_state: ProducerRouteState,
    ) -> bool {
        self.producer(producer_id)
            .is_some_and(|(_, _, _, producer_route_state)| producer_route_state == route_state)
    }

    /// assert both route-state axes stored on a consumer
    ///
    /// the first route state is receiver-local
    /// the second is the copied
    /// producer shadow that should track source-side pause changes
    #[must_use]
    pub fn consumer_route_matches(
        &self,
        consumer_id: ConsumerId,
        route_state: ConsumerRouteState,
        producer_route_state: ProducerRouteState,
    ) -> bool {
        self.consumer(consumer_id)
            .is_some_and(|(_, _, _, _, route, producer_route)| {
                route == route_state && producer_route == producer_route_state
            })
    }

    /// return the session-to-transport relation snapshot
    #[must_use]
    pub fn session_transports(&self) -> RelationSnapshot<'_, SessionId, TransportId> {
        RelationSnapshot::new(&self.session_transports)
    }

    /// return the transport-to-producer relation snapshot
    #[must_use]
    pub fn transport_producers(&self) -> RelationSnapshot<'_, TransportId, ProducerId> {
        RelationSnapshot::new(&self.transport_producers)
    }

    /// return the transport-to-consumer relation snapshot
    #[must_use]
    pub fn transport_consumers(&self) -> RelationSnapshot<'_, TransportId, ConsumerId> {
        RelationSnapshot::new(&self.transport_consumers)
    }

    /// return the producer-to-consumer relation snapshot
    #[must_use]
    pub fn producer_consumers(&self) -> RelationSnapshot<'_, ProducerId, ConsumerId> {
        RelationSnapshot::new(&self.producer_consumers)
    }

    fn transport(
        &self,
        transport_id: TransportId,
    ) -> Option<(TransportId, SessionId, TransportDirection)> {
        self.transports
            .iter()
            .copied()
            .find(|(id, _, _)| *id == transport_id)
    }

    fn producer(
        &self,
        producer_id: ProducerId,
    ) -> Option<(ProducerId, TransportId, MediaKind, ProducerRouteState)> {
        self.producers
            .iter()
            .copied()
            .find(|(id, _, _, _)| *id == producer_id)
    }

    fn consumer(
        &self,
        consumer_id: ConsumerId,
    ) -> Option<(
        ConsumerId,
        ProducerId,
        TransportId,
        MediaKind,
        ConsumerRouteState,
        ProducerRouteState,
    )> {
        self.consumers
            .iter()
            .copied()
            .find(|(id, _, _, _, _, _)| *id == consumer_id)
    }
}

/// copy router primary maps and reverse indexes into detached test data
///
/// the snapshot must not be used as a mutation surface
/// it is a read model for
/// assertions that need to compare primary topology and reverse-index topology
/// after a router transition
#[must_use]
pub fn router_state_snapshot<O: RouterObserver>(router: &Router<O>) -> RouterStateSnapshot {
    let index_snapshot = router.indexes.snapshot();
    RouterStateSnapshot {
        id: router.id,
        sessions: router
            .sessions
            .values()
            .map(|session| (session.id(), session.state()))
            .collect(),
        transports: router
            .transports
            .values()
            .map(|transport| {
                (
                    transport.id(),
                    transport.session_id(),
                    transport.direction(),
                )
            })
            .collect(),
        producers: router
            .producers
            .values()
            .map(|producer| {
                (
                    producer.id(),
                    producer.transport_id(),
                    producer.media_kind(),
                    producer.route_state(),
                )
            })
            .collect(),
        consumers: router
            .consumers
            .values()
            .map(|consumer| {
                (
                    consumer.id(),
                    consumer.producer_id(),
                    consumer.transport_id(),
                    consumer.media_kind(),
                    consumer.route_state(),
                    consumer.producer_route_state(),
                )
            })
            .collect(),
        session_transports: index_snapshot.session_transports,
        transport_producers: index_snapshot.transport_producers,
        transport_consumers: index_snapshot.transport_consumers,
        producer_consumers: index_snapshot.producer_consumers,
    }
}

/// verify the full router consistency contract used by tests and proofs
///
/// this predicate is broader than public API validation
/// it checks facts that should be impossible to violate through normal transitions,
/// including exact reverse indexes, transport direction rules, consumer media
/// kind matches and producer route-state shadows
///
/// a `false` result means the router's internal topology has diverged
/// callers should not treat it as a recoverable application error
#[must_use]
pub fn router_satisfies_invariants<O: RouterObserver>(router: &Router<O>) -> bool {
    references_are_valid(router)
        && reverse_indices_are_exact(router)
        && transport_directions_are_valid(router)
        && consumer_media_matches_producer(router)
        && consumer_route_shadows_producer(router)
}

/// validate that every stored owner id points to a live primary entity
fn references_are_valid<O: RouterObserver>(router: &Router<O>) -> bool {
    for transport in router.transports.values() {
        if !router.sessions.contains_key(&transport.session_id()) {
            return false;
        }
    }

    for producer in router.producers.values() {
        if !router.transports.contains_key(&producer.transport_id()) {
            return false;
        }
    }

    for consumer in router.consumers.values() {
        if !router.transports.contains_key(&consumer.transport_id())
            || !router.producers.contains_key(&consumer.producer_id())
        {
            return false;
        }
    }

    true
}

/// validate that every reverse relation is exact in both directions
fn reverse_indices_are_exact<O: RouterObserver>(router: &Router<O>) -> bool {
    router.indexes.session_transports_are_exact(
        router.transports.values(),
        |session_id| router.sessions.contains_key(&session_id),
        |session_id, transport_id| {
            let Some(transport) = router.transports.get(&transport_id) else {
                return false;
            };
            transport.session_id() == session_id
        },
    ) && router.indexes.transport_producers_are_exact(
        router.producers.values(),
        |transport_id| router.transports.contains_key(&transport_id),
        |transport_id, producer_id| {
            let Some(producer) = router.producers.get(&producer_id) else {
                return false;
            };
            producer.transport_id() == transport_id
        },
    ) && router.indexes.transport_consumers_are_exact(
        router.consumers.values(),
        |transport_id| router.transports.contains_key(&transport_id),
        |transport_id, consumer_id| {
            let Some(consumer) = router.consumers.get(&consumer_id) else {
                return false;
            };
            consumer.transport_id() == transport_id
        },
    ) && router.indexes.producer_consumers_are_exact(
        router.consumers.values(),
        |producer_id| router.producers.contains_key(&producer_id),
        |producer_id, consumer_id| {
            let Some(consumer) = router.consumers.get(&consumer_id) else {
                return false;
            };
            consumer.producer_id() == producer_id
        },
    )
}

/// validate that producers and consumers live on direction-compatible transports
fn transport_directions_are_valid<O: RouterObserver>(router: &Router<O>) -> bool {
    for producer in router.producers.values() {
        let Some(transport) = router.transports.get(&producer.transport_id()) else {
            return false;
        };
        if transport.direction() != TransportDirection::Receive {
            return false;
        }
    }

    for consumer in router.consumers.values() {
        let Some(transport) = router.transports.get(&consumer.transport_id()) else {
            return false;
        };
        if transport.direction() != TransportDirection::Send {
            return false;
        }
    }

    true
}

/// validate that each consumer keeps the same media kind as its producer
fn consumer_media_matches_producer<O: RouterObserver>(router: &Router<O>) -> bool {
    for consumer in router.consumers.values() {
        let Some(producer) = router.producers.get(&consumer.producer_id()) else {
            return false;
        };
        if consumer.media_kind() != producer.media_kind() {
            return false;
        }
    }

    true
}

/// validate that each consumer shadow matches its producer route state
fn consumer_route_shadows_producer<O: RouterObserver>(router: &Router<O>) -> bool {
    for consumer in router.consumers.values() {
        let Some(producer) = router.producers.get(&consumer.producer_id()) else {
            return false;
        };
        if consumer.producer_route_state() != producer.route_state() {
            return false;
        }
    }

    true
}

/// proof-only accessors for storage-shaped router facts
///
/// these helpers are not exposed in normal test-support builds
/// the Kani harnesses need small predicates over individual map entries so they
/// can state bounded exactness properties without copying an entire snapshot
/// into each assertion
#[cfg(kani)]
pub mod proof {
    pub use super::super::{relation_index::RelationProofView, topology::test_support::proof::*};
    use super::{super::NoopRouterObserver, *};

    /// cfg-gated proof view over primary maps and reverse relations
    pub struct RouterProofView<'a, O: RouterObserver = NoopRouterObserver> {
        router: &'a Router<O>,
    }

    impl<'a, O: RouterObserver> RouterProofView<'a, O> {
        /// borrow router state for proof assertions
        #[must_use]
        pub fn new(router: &'a Router<O>) -> Self {
            Self { router }
        }

        /// report whether the primary session map contains a session
        #[must_use]
        pub fn contains_session(&self, session_id: SessionId) -> bool {
            self.router.sessions.contains_key(&session_id)
        }

        /// report whether the primary transport map contains a transport
        #[must_use]
        pub fn contains_transport(&self, transport_id: TransportId) -> bool {
            self.router.transports.contains_key(&transport_id)
        }

        /// report whether the primary producer map contains a producer
        #[must_use]
        pub fn contains_producer(&self, producer_id: ProducerId) -> bool {
            self.router.producers.contains_key(&producer_id)
        }

        /// report whether the primary consumer map contains a consumer
        #[must_use]
        pub fn contains_consumer(&self, consumer_id: ConsumerId) -> bool {
            self.router.consumers.contains_key(&consumer_id)
        }

        /// count live transports in the primary map
        #[must_use]
        pub fn transport_count(&self) -> usize {
            self.router.transports.len()
        }

        /// count live producers in the primary map
        #[must_use]
        pub fn producer_count(&self) -> usize {
            self.router.producers.len()
        }

        /// count live consumers in the primary map
        #[must_use]
        pub fn consumer_count(&self) -> usize {
            self.router.consumers.len()
        }

        /// return the session-to-transport relation proof view
        #[must_use]
        pub fn session_transports(&self) -> RelationProofView<'_, SessionId, TransportId> {
            self.router.indexes.session_transports()
        }

        /// return the transport-to-producer relation proof view
        #[must_use]
        pub fn transport_producers(&self) -> RelationProofView<'_, TransportId, ProducerId> {
            self.router.indexes.transport_producers()
        }

        /// return the transport-to-consumer relation proof view
        #[must_use]
        pub fn transport_consumers(&self) -> RelationProofView<'_, TransportId, ConsumerId> {
            self.router.indexes.transport_consumers()
        }

        /// return the producer-to-consumer relation proof view
        #[must_use]
        pub fn producer_consumers(&self) -> RelationProofView<'_, ProducerId, ConsumerId> {
            self.router.indexes.producer_consumers()
        }

        /// assert a transport's captured owner and direction from primary storage
        #[must_use]
        pub fn transport_matches(
            &self,
            transport_id: TransportId,
            session_id: SessionId,
            direction: TransportDirection,
        ) -> bool {
            let Some(transport) = self.router.transports.get(&transport_id) else {
                return false;
            };
            transport.session_id() == session_id && transport.direction() == direction
        }

        /// assert a producer's captured receive transport and media kind
        #[must_use]
        pub fn producer_origin_matches(
            &self,
            producer_id: ProducerId,
            transport_id: TransportId,
            media_kind: MediaKind,
        ) -> bool {
            let Some(producer) = self.router.producers.get(&producer_id) else {
                return false;
            };
            producer.transport_id() == transport_id && producer.media_kind() == media_kind
        }

        /// assert a consumer's captured source producer, send transport and media kind
        #[must_use]
        pub fn consumer_origin_matches(
            &self,
            consumer_id: ConsumerId,
            producer_id: ProducerId,
            transport_id: TransportId,
            media_kind: MediaKind,
        ) -> bool {
            let Some(consumer) = self.router.consumers.get(&consumer_id) else {
                return false;
            };
            consumer.producer_id() == producer_id
                && consumer.transport_id() == transport_id
                && consumer.media_kind() == media_kind
        }

        /// assert that a consumer's producer shadow matches the current source state
        #[must_use]
        pub fn consumer_shadows_producer(&self, consumer_id: ConsumerId) -> bool {
            let Some(consumer) = self.router.consumers.get(&consumer_id) else {
                return false;
            };
            let Some(producer) = self.router.producers.get(&consumer.producer_id()) else {
                return false;
            };
            consumer.producer_route_state() == producer.route_state()
        }

        /// assert the receiver-local route state and producer-shadow route state
        #[must_use]
        pub fn consumer_route_matches(
            &self,
            consumer_id: ConsumerId,
            route_state: ConsumerRouteState,
            producer_route_state: ProducerRouteState,
        ) -> bool {
            let Some(consumer) = self.router.consumers.get(&consumer_id) else {
                return false;
            };
            consumer.route_state() == route_state
                && consumer.producer_route_state() == producer_route_state
        }
    }
}
