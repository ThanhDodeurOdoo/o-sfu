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
    /// return the router id observed by this detached snapshot
    #[must_use]
    pub fn id(&self) -> RouterId {
        self.id
    }

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
        self.session_state(session_id).is_some()
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

    /// return the captured lifecycle state for one session
    ///
    /// this is intentionally based on the primary session map, not the reverse
    /// index
    /// tests that need relation facts should use the relation accessors
    /// separately
    #[must_use]
    pub fn session_state(&self, session_id: SessionId) -> Option<SessionState> {
        self.sessions
            .iter()
            .find_map(|(id, state)| (*id == session_id).then_some(*state))
    }

    /// assert the captured transport owner and direction in one predicate
    ///
    /// this keeps tests focused on the domain relation they care about instead
    /// of unpacking the snapshot tuple shape
    #[must_use]
    pub fn transport_matches(
        &self,
        transport_id: TransportId,
        session_id: SessionId,
        direction: TransportDirection,
    ) -> bool {
        self.transport(transport_id)
            .is_some_and(|(_, owner, transport_direction)| {
                owner == session_id && transport_direction == direction
            })
    }

    /// assert the captured producer transport owner and media kind
    #[must_use]
    pub fn producer_origin_matches(
        &self,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
    ) -> bool {
        self.producer(producer_id)
            .is_some_and(|(_, owner, kind, _)| owner == transport_id && kind == media_kind)
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

    /// assert the captured consumer source, owner transport and media kind
    #[must_use]
    pub fn consumer_origin_matches(
        &self,
        consumer_id: ConsumerId,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
    ) -> bool {
        self.consumer(consumer_id)
            .is_some_and(|(_, source, owner, kind, _, _)| {
                source == producer_id && owner == transport_id && kind == media_kind
            })
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

    /// verify that a consumer's producer shadow still mirrors its source producer
    ///
    /// this checks one consumer at snapshot time
    /// use `router_satisfies_invariants`
    /// when the whole router must be checked
    #[must_use]
    pub fn consumer_shadows_producer(&self, consumer_id: ConsumerId) -> bool {
        let Some((_, producer_id, _, _, _, producer_route_state)) = self.consumer(consumer_id)
        else {
            return false;
        };
        self.producer(producer_id)
            .is_some_and(|(_, _, _, route_state)| producer_route_state == route_state)
    }

    /// count session owner entries in the reverse index snapshot
    #[must_use]
    pub fn session_transport_index_count(&self) -> usize {
        self.session_transports.len()
    }

    /// count transport owner entries in the producer reverse index snapshot
    #[must_use]
    pub fn transport_producer_index_count(&self) -> usize {
        self.transport_producers.len()
    }

    /// count transport owner entries in the consumer reverse index snapshot
    #[must_use]
    pub fn transport_consumer_index_count(&self) -> usize {
        self.transport_consumers.len()
    }

    /// count producer owner entries in the consumer reverse index snapshot
    #[must_use]
    pub fn producer_consumer_index_count(&self) -> usize {
        self.producer_consumers.len()
    }

    /// count transports indexed for one session
    #[must_use]
    pub fn session_transport_count(&self, session_id: SessionId) -> usize {
        Self::relation_count(&self.session_transports, session_id)
    }

    /// count producers indexed for one transport
    #[must_use]
    pub fn transport_producer_count(&self, transport_id: TransportId) -> usize {
        Self::relation_count(&self.transport_producers, transport_id)
    }

    /// count consumers indexed for one transport
    #[must_use]
    pub fn transport_consumer_count(&self, transport_id: TransportId) -> usize {
        Self::relation_count(&self.transport_consumers, transport_id)
    }

    /// count consumers indexed for one producer
    #[must_use]
    pub fn producer_consumer_count(&self, producer_id: ProducerId) -> usize {
        Self::relation_count(&self.producer_consumers, producer_id)
    }

    /// report whether one session has any transport relation entry
    #[must_use]
    pub fn has_session_transport_index(&self, session_id: SessionId) -> bool {
        Self::relation_has_key(&self.session_transports, session_id)
    }

    /// report whether one transport has any producer relation entry
    #[must_use]
    pub fn has_transport_producer_index(&self, transport_id: TransportId) -> bool {
        Self::relation_has_key(&self.transport_producers, transport_id)
    }

    /// report whether one transport has any consumer relation entry
    #[must_use]
    pub fn has_transport_consumer_index(&self, transport_id: TransportId) -> bool {
        Self::relation_has_key(&self.transport_consumers, transport_id)
    }

    /// report whether one producer has any consumer relation entry
    #[must_use]
    pub fn has_producer_consumer_index(&self, producer_id: ProducerId) -> bool {
        Self::relation_has_key(&self.producer_consumers, producer_id)
    }

    /// report exact session-to-transport membership in the snapshot
    #[must_use]
    pub fn has_session_transport(&self, session_id: SessionId, transport_id: TransportId) -> bool {
        Self::relation_contains(&self.session_transports, session_id, transport_id)
    }

    /// report exact transport-to-producer membership in the snapshot
    #[must_use]
    pub fn has_transport_producer(
        &self,
        transport_id: TransportId,
        producer_id: ProducerId,
    ) -> bool {
        Self::relation_contains(&self.transport_producers, transport_id, producer_id)
    }

    /// report exact transport-to-consumer membership in the snapshot
    #[must_use]
    pub fn has_transport_consumer(
        &self,
        transport_id: TransportId,
        consumer_id: ConsumerId,
    ) -> bool {
        Self::relation_contains(&self.transport_consumers, transport_id, consumer_id)
    }

    /// report exact producer-to-consumer membership in the snapshot
    #[must_use]
    pub fn has_producer_consumer(&self, producer_id: ProducerId, consumer_id: ConsumerId) -> bool {
        Self::relation_contains(&self.producer_consumers, producer_id, consumer_id)
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

    fn relation_count<K, V>(relations: &[(K, Vec<V>)], key: K) -> usize
    where
        K: Copy + Eq,
    {
        relations
            .iter()
            .find_map(|(relation_key, values)| (*relation_key == key).then_some(values.len()))
            .unwrap_or(0)
    }

    fn relation_has_key<K, V>(relations: &[(K, Vec<V>)], key: K) -> bool
    where
        K: Copy + Eq,
    {
        relations
            .iter()
            .any(|(relation_key, _)| *relation_key == key)
    }

    fn relation_contains<K, V>(relations: &[(K, Vec<V>)], key: K, value: V) -> bool
    where
        K: Copy + Eq,
        V: Copy + Eq,
    {
        relations
            .iter()
            .find(|(relation_key, _)| *relation_key == key)
            .is_some_and(|(_, values)| values.contains(&value))
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
/// this predicate is intentionally broader than public API validation
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
        && consumer_pause_shadows_producer(router)
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
fn consumer_pause_shadows_producer<O: RouterObserver>(router: &Router<O>) -> bool {
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
/// these helpers are intentionally not exposed in normal test-support builds
/// the Kani harnesses need small predicates over individual map entries so they
/// can state bounded exactness properties without copying an entire snapshot
/// into each assertion
#[cfg(kani)]
pub mod proof {
    use super::*;

    /// report whether the primary session map contains a session
    #[must_use]
    pub fn router_contains_session<O: RouterObserver>(
        router: &Router<O>,
        session_id: SessionId,
    ) -> bool {
        router.sessions.contains_key(&session_id)
    }

    /// report whether the primary transport map contains a transport
    #[must_use]
    pub fn router_contains_transport<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
    ) -> bool {
        router.transports.contains_key(&transport_id)
    }

    /// report whether the primary producer map contains a producer
    #[must_use]
    pub fn router_contains_producer<O: RouterObserver>(
        router: &Router<O>,
        producer_id: ProducerId,
    ) -> bool {
        router.producers.contains_key(&producer_id)
    }

    /// report whether the primary consumer map contains a consumer
    #[must_use]
    pub fn router_contains_consumer<O: RouterObserver>(
        router: &Router<O>,
        consumer_id: ConsumerId,
    ) -> bool {
        router.consumers.contains_key(&consumer_id)
    }

    /// count live transports in the primary map
    #[must_use]
    pub fn router_transport_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.transports.len()
    }

    /// count live producers in the primary map
    #[must_use]
    pub fn router_producer_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.producers.len()
    }

    /// count live consumers in the primary map
    #[must_use]
    pub fn router_consumer_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.consumers.len()
    }

    /// count session owner entries in the transport reverse index
    #[must_use]
    pub fn router_session_transport_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.indexes.session_transport_index_count()
    }

    /// count transport owner entries in the producer reverse index
    #[must_use]
    pub fn router_transport_producer_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.indexes.transport_producer_index_count()
    }

    /// count transport owner entries in the consumer reverse index
    #[must_use]
    pub fn router_transport_consumer_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.indexes.transport_consumer_index_count()
    }

    /// count producer owner entries in the consumer reverse index
    #[must_use]
    pub fn router_producer_consumer_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
        router.indexes.producer_consumer_index_count()
    }

    /// count transports indexed under one session
    #[must_use]
    pub fn router_session_transport_count<O: RouterObserver>(
        router: &Router<O>,
        session_id: SessionId,
    ) -> usize {
        router.indexes.session_transport_count(session_id)
    }

    /// count producers indexed under one receive transport
    #[must_use]
    pub fn router_transport_producer_count<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
    ) -> usize {
        router.indexes.transport_producer_count(transport_id)
    }

    /// count consumers indexed under one send transport
    #[must_use]
    pub fn router_transport_consumer_count<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
    ) -> usize {
        router.indexes.transport_consumer_count(transport_id)
    }

    /// count consumers indexed under one producer
    #[must_use]
    pub fn router_producer_consumer_count<O: RouterObserver>(
        router: &Router<O>,
        producer_id: ProducerId,
    ) -> usize {
        router.indexes.producer_consumer_count(producer_id)
    }

    /// report exact session-to-transport reverse-index membership
    #[must_use]
    pub fn router_has_session_transport<O: RouterObserver>(
        router: &Router<O>,
        session_id: SessionId,
        transport_id: TransportId,
    ) -> bool {
        router
            .indexes
            .has_session_transport(session_id, transport_id)
    }

    /// report exact transport-to-producer reverse-index membership
    #[must_use]
    pub fn router_has_transport_producer<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
        producer_id: ProducerId,
    ) -> bool {
        router
            .indexes
            .has_transport_producer(transport_id, producer_id)
    }

    /// report exact transport-to-consumer reverse-index membership
    #[must_use]
    pub fn router_has_transport_consumer<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
        consumer_id: ConsumerId,
    ) -> bool {
        router
            .indexes
            .has_transport_consumer(transport_id, consumer_id)
    }

    /// report exact producer-to-consumer reverse-index membership
    #[must_use]
    pub fn router_has_producer_consumer<O: RouterObserver>(
        router: &Router<O>,
        producer_id: ProducerId,
        consumer_id: ConsumerId,
    ) -> bool {
        router
            .indexes
            .has_producer_consumer(producer_id, consumer_id)
    }

    /// report whether a session has any transport reverse-index entry
    #[must_use]
    pub fn router_has_session_transport_index<O: RouterObserver>(
        router: &Router<O>,
        session_id: SessionId,
    ) -> bool {
        router.indexes.has_session_transport_index(session_id)
    }

    /// report whether a transport has any producer reverse-index entry
    #[must_use]
    pub fn router_has_transport_producer_index<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
    ) -> bool {
        router.indexes.has_transport_producer_index(transport_id)
    }

    /// report whether a transport has any consumer reverse-index entry
    #[must_use]
    pub fn router_has_transport_consumer_index<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
    ) -> bool {
        router.indexes.has_transport_consumer_index(transport_id)
    }

    /// report whether a producer has any consumer reverse-index entry
    #[must_use]
    pub fn router_has_producer_consumer_index<O: RouterObserver>(
        router: &Router<O>,
        producer_id: ProducerId,
    ) -> bool {
        router.indexes.has_producer_consumer_index(producer_id)
    }

    /// assert a transport's captured owner and direction from primary storage
    #[must_use]
    pub fn router_transport_matches<O: RouterObserver>(
        router: &Router<O>,
        transport_id: TransportId,
        session_id: SessionId,
        direction: TransportDirection,
    ) -> bool {
        let Some(transport) = router.transports.get(&transport_id) else {
            return false;
        };
        transport.session_id() == session_id && transport.direction() == direction
    }

    /// assert a producer's captured receive transport and media kind
    #[must_use]
    pub fn router_producer_origin_matches<O: RouterObserver>(
        router: &Router<O>,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
    ) -> bool {
        let Some(producer) = router.producers.get(&producer_id) else {
            return false;
        };
        producer.transport_id() == transport_id && producer.media_kind() == media_kind
    }

    /// assert a consumer's captured source producer, send transport and media kind
    #[must_use]
    pub fn router_consumer_origin_matches<O: RouterObserver>(
        router: &Router<O>,
        consumer_id: ConsumerId,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
    ) -> bool {
        let Some(consumer) = router.consumers.get(&consumer_id) else {
            return false;
        };
        consumer.producer_id() == producer_id
            && consumer.transport_id() == transport_id
            && consumer.media_kind() == media_kind
    }

    /// assert that a consumer's producer shadow matches the current source state
    #[must_use]
    pub fn router_consumer_shadows_producer<O: RouterObserver>(
        router: &Router<O>,
        consumer_id: ConsumerId,
    ) -> bool {
        let Some(consumer) = router.consumers.get(&consumer_id) else {
            return false;
        };
        let Some(producer) = router.producers.get(&consumer.producer_id()) else {
            return false;
        };
        consumer.producer_route_state() == producer.route_state()
    }

    /// assert the receiver-local route state and producer-shadow route state
    #[must_use]
    pub fn router_consumer_route_matches<O: RouterObserver>(
        router: &Router<O>,
        consumer_id: ConsumerId,
        route_state: ConsumerRouteState,
        producer_route_state: ProducerRouteState,
    ) -> bool {
        let Some(consumer) = router.consumers.get(&consumer_id) else {
            return false;
        };
        consumer.route_state() == route_state
            && consumer.producer_route_state() == producer_route_state
    }
}
