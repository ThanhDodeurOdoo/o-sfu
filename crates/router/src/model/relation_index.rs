//! reverse-relation ownership for router teardown plus invariant checks
//!
//! router mutations keep primary entity maps in the router module
//! this module owns the reverse lookup shape used to find dependents during
//! teardown
//!
//! every relation here mirrors a primary-map ownership edge
//! empty owner sets are removed immediately so a present index key always means
//! at least one live dependent exists
//! tests and Kani proofs use the same owner for exactness checks, so proof
//! storage details do not leak back into production mutation code

#[cfg(any(test, feature = "test-support", kani))]
use super::{Consumer, Producer, Transport};
use super::{
    ConsumerId, ProducerId, SessionId, TransportId,
    proof_storage::{BTreeMap, BTreeSet},
};

/// reverse index that keeps one owner-to-dependent relation exact
///
/// the type owns the no-empty-set invariant for a single relation
/// callers choose lifecycle operations such as `insert`, `remove` or `take`
/// while this type chooses the storage operations needed by runtime and proof
/// builds
#[derive(Debug, Clone)]
pub(super) struct RelationIndex<K, V> {
    /// owner-to-dependent entries with no empty value sets
    entries: BTreeMap<K, BTreeSet<V>>,
}

impl<K, V> RelationIndex<K, V>
where
    K: Copy + Ord,
    V: Copy + Ord,
{
    /// create an empty reverse relation
    ///
    /// callers should build relation entries only after the matching primary
    /// entity has been accepted by the router
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// record one dependent for an existing owner
    ///
    /// insertion is idempotent for duplicate `(owner, dependent)` pairs
    /// normal builds use the standard `BTreeMap` entry API
    /// the Kani build avoids the entry facade because the bounded proof map
    /// exposes a smaller mutation surface
    fn insert(&mut self, key: K, value: V) {
        #[cfg(not(kani))]
        {
            self.entries.entry(key).or_default().insert(value);
        }

        #[cfg(kani)]
        {
            if let Some(values) = self.entries.get_mut(&key) {
                values.insert(value);
                return;
            }

            let mut values = BTreeSet::new();
            values.insert(value);
            self.entries.insert(key, values);
        }
    }

    /// remove one dependent while preserving the no-empty-set invariant
    ///
    /// missing owners or missing dependents are treated as no-ops
    /// teardown helpers use that property so cascading removal can safely call
    /// detach paths after an earlier relation already drained part of the graph
    fn remove(&mut self, key: K, value: V) {
        #[cfg(not(kani))]
        let should_remove_key = self.entries.get_mut(&key).is_some_and(|values| {
            values.remove(&value);
            values.is_empty()
        });

        #[cfg(kani)]
        let should_remove_key = if let Some(values) = self.entries.get_mut(&key) {
            values.remove(&value);
            values.is_empty()
        } else {
            false
        };

        if should_remove_key {
            self.entries.remove(&key);
        }
    }

    /// remove one owner relation and return all dependents owned by it
    ///
    /// this is the main teardown primitive
    /// returning an empty set for a missing owner keeps repeated cleanup
    /// idempotent while still letting callers walk only the dependents that were
    /// indexed
    fn take(&mut self, key: K) -> BTreeSet<V> {
        self.entries.remove(&key).unwrap_or_default()
    }

    /// return a detached dependent list for read-then-mutate transitions
    ///
    /// producer route-state propagation needs to iterate consumers while mutating
    /// the consumer map
    /// normal builds return a `Vec` snapshot for borrow separation
    /// the Kani build can return the bounded set directly because proof storage
    /// is copy based
    #[cfg(not(kani))]
    fn values_snapshot(&self, key: K) -> Vec<V> {
        self.entries
            .get(&key)
            .map_or_else(Vec::new, |values| values.iter().copied().collect())
    }

    #[cfg(kani)]
    fn values_snapshot(&self, key: K) -> BTreeSet<V> {
        self.entries.get(&key).copied().unwrap_or_default()
    }

    /// expose exact relation membership to tests and proof predicates
    ///
    /// production teardown uses `remove` or `take` instead so callers do not
    /// branch on the storage layout
    #[cfg(any(test, feature = "test-support", kani))]
    fn contains(&self, key: K, value: V) -> bool {
        self.entries
            .get(&key)
            .is_some_and(|values| values.contains(&value))
    }

    /// copy the full relation without exposing the backing map type
    ///
    /// this is the normal test-support bridge
    /// snapshots can assert topology
    /// facts without gaining mutation access or relying on proof-storage types
    #[cfg(any(test, feature = "test-support", kani))]
    fn snapshot(&self) -> Vec<(K, Vec<V>)> {
        self.entries
            .iter()
            .map(|(key, values)| (*key, values.iter().copied().collect()))
            .collect()
    }

    /// prove that this reverse relation exactly mirrors one primary-map view
    ///
    /// `key_exists` validates that every owner key is still live,
    /// `relation_matches` validates each stored dependent edge and
    /// `item_relation` derives the required reverse edge from the primary item
    ///
    /// exactness is bidirectional
    /// every indexed edge must match a live primary
    /// item and every primary item must be present in the index
    #[cfg(any(test, feature = "test-support", kani))]
    fn is_exact_for<Item, Items, KeyExists, RelationMatches, ItemRelation>(
        &self,
        items: Items,
        key_exists: KeyExists,
        relation_matches: RelationMatches,
        item_relation: ItemRelation,
    ) -> bool
    where
        Items: IntoIterator<Item = Item>,
        KeyExists: Fn(K) -> bool,
        RelationMatches: Fn(K, V) -> bool,
        ItemRelation: Fn(Item) -> (K, V),
    {
        for (key, values) in &self.entries {
            if !key_exists(*key) || values.is_empty() {
                return false;
            }

            for value in values {
                if !relation_matches(*key, *value) {
                    return false;
                }
            }
        }

        for item in items {
            let (key, value) = item_relation(item);
            if !self.contains(key, value) {
                return false;
            }
        }

        true
    }
}

#[cfg(kani)]
/// borrowed proof view over one reverse relation
pub struct RelationProofView<'a, K, V> {
    relation: &'a RelationIndex<K, V>,
}

#[cfg(kani)]
impl<'a, K, V> RelationProofView<'a, K, V>
where
    K: Copy + Ord,
    V: Copy + Ord,
{
    fn new(relation: &'a RelationIndex<K, V>) -> Self {
        Self { relation }
    }

    /// count relation owner keys
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.relation.entries.len()
    }

    /// count dependents indexed under one owner key
    #[must_use]
    pub fn count(&self, key: K) -> usize {
        self.relation
            .entries
            .get(&key)
            .map_or(0, |values| values.len())
    }

    /// report whether one owner key has any indexed dependents
    #[must_use]
    pub fn contains_key(&self, key: K) -> bool {
        self.relation.entries.contains_key(&key)
    }

    /// report exact relation membership
    #[must_use]
    pub fn contains(&self, key: K, value: V) -> bool {
        self.relation.contains(key, value)
    }
}

#[cfg(any(test, feature = "test-support", kani))]
/// detached copy of every reverse router relation
///
/// tests use this shape to inspect router topology without observing the
/// production map type or mutating reverse indexes directly
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RouterIndexSnapshot {
    /// session-to-transport ownership at the time the snapshot was taken
    pub(super) session_transports: Vec<(SessionId, Vec<TransportId>)>,
    /// receive-transport-to-producer ownership at the time the snapshot was taken
    pub(super) transport_producers: Vec<(TransportId, Vec<ProducerId>)>,
    /// send-transport-to-consumer ownership at the time the snapshot was taken
    pub(super) transport_consumers: Vec<(TransportId, Vec<ConsumerId>)>,
    /// producer-to-consumer dependency ownership at the time the snapshot was taken
    pub(super) producer_consumers: Vec<(ProducerId, Vec<ConsumerId>)>,
}

/// router-owned reverse indexes for all topology dependency edges
///
/// `Router` is the only production owner of this type
/// callers mutate it through
/// lifecycle-shaped methods such as `add_transport`, `take_transport_consumers`
/// or `remove_consumer` rather than by choosing a concrete map
/// that keeps
/// teardown rules, proof-storage details and invariant exactness checks in one
/// local boundary
#[derive(Debug, Clone)]
pub(super) struct RouterIndexes {
    /// live transports grouped by their owning session
    session_transports: RelationIndex<SessionId, TransportId>,
    /// live producers grouped by their receive transport
    transport_producers: RelationIndex<TransportId, ProducerId>,
    /// live consumers grouped by their send transport
    transport_consumers: RelationIndex<TransportId, ConsumerId>,
    /// live consumers grouped by the producer they consume
    producer_consumers: RelationIndex<ProducerId, ConsumerId>,
}

impl RouterIndexes {
    /// create empty reverse indexes for a new router
    pub(super) fn new() -> Self {
        Self {
            session_transports: RelationIndex::new(),
            transport_producers: RelationIndex::new(),
            transport_consumers: RelationIndex::new(),
            producer_consumers: RelationIndex::new(),
        }
    }

    /// attach a transport to the session that owns it
    pub(super) fn add_transport(&mut self, session_id: SessionId, transport_id: TransportId) {
        self.session_transports.insert(session_id, transport_id);
    }

    /// detach a transport from its session without touching its media dependents
    ///
    /// callers use this when the transport primary record is already gone or is
    /// being removed by a wider teardown cascade
    pub(super) fn remove_transport_from_session(
        &mut self,
        session_id: SessionId,
        transport_id: TransportId,
    ) {
        self.session_transports.remove(session_id, transport_id);
    }

    /// drain all transports owned by a session
    ///
    /// session teardown consumes this set before recursively removing each transport
    /// after this call the session has no reverse-index entry
    pub(super) fn take_session_transports(
        &mut self,
        session_id: SessionId,
    ) -> BTreeSet<TransportId> {
        self.session_transports.take(session_id)
    }

    /// attach a producer to the receive transport that owns it
    pub(super) fn add_producer(&mut self, transport_id: TransportId, producer_id: ProducerId) {
        self.transport_producers.insert(transport_id, producer_id);
    }

    /// detach a producer from its receive transport
    ///
    /// dependent consumers are owned by the producer relation and must be drained
    /// separately by the producer teardown path
    pub(super) fn remove_producer_from_transport(
        &mut self,
        transport_id: TransportId,
        producer_id: ProducerId,
    ) {
        self.transport_producers.remove(transport_id, producer_id);
    }

    /// drain all producers owned by one transport
    ///
    /// transport teardown consumes this set after consumers on the same transport
    /// have been detached
    pub(super) fn take_transport_producers(
        &mut self,
        transport_id: TransportId,
    ) -> BTreeSet<ProducerId> {
        self.transport_producers.take(transport_id)
    }

    /// attach a consumer to both reverse ownership edges it participates in
    ///
    /// every consumer is owned by a send transport and depends on a producer
    /// both relations must be updated together or teardown exactness breaks
    pub(super) fn add_consumer(
        &mut self,
        transport_id: TransportId,
        producer_id: ProducerId,
        consumer_id: ConsumerId,
    ) {
        self.transport_consumers.insert(transport_id, consumer_id);
        self.producer_consumers.insert(producer_id, consumer_id);
    }

    /// remove a consumer from both reverse ownership edges
    ///
    /// this is idempotent so producer teardown and transport teardown can both
    /// funnel through the same consumer detach path
    pub(super) fn remove_consumer(
        &mut self,
        transport_id: TransportId,
        producer_id: ProducerId,
        consumer_id: ConsumerId,
    ) {
        self.transport_consumers.remove(transport_id, consumer_id);
        self.producer_consumers.remove(producer_id, consumer_id);
    }

    /// drain consumers owned by a send transport
    ///
    /// transport teardown uses this before producer cleanup so receiver-local
    /// routes disappear even when their source producer remains live elsewhere
    pub(super) fn take_transport_consumers(
        &mut self,
        transport_id: TransportId,
    ) -> BTreeSet<ConsumerId> {
        self.transport_consumers.take(transport_id)
    }

    /// drain consumers that depend on a producer
    ///
    /// producer teardown uses this to remove all receiver routes before emitting
    /// the producer lifecycle event
    pub(super) fn take_producer_consumers(
        &mut self,
        producer_id: ProducerId,
    ) -> BTreeSet<ConsumerId> {
        self.producer_consumers.take(producer_id)
    }

    /// snapshot consumers that depend on one producer for route-state updates
    ///
    /// this intentionally does not drain the relation
    /// it only gives the router a detached id list so consumer state can be
    /// updated while the index remains intact
    pub(super) fn producer_consumers_for_update(
        &self,
        producer_id: ProducerId,
    ) -> impl IntoIterator<Item = ConsumerId> {
        self.producer_consumers.values_snapshot(producer_id)
    }

    /// return a borrowed proof view over session transports
    #[cfg(kani)]
    pub(super) fn session_transports(&self) -> RelationProofView<'_, SessionId, TransportId> {
        RelationProofView::new(&self.session_transports)
    }

    /// return a borrowed proof view over transport producers
    #[cfg(kani)]
    pub(super) fn transport_producers(&self) -> RelationProofView<'_, TransportId, ProducerId> {
        RelationProofView::new(&self.transport_producers)
    }

    /// return a borrowed proof view over transport consumers
    #[cfg(kani)]
    pub(super) fn transport_consumers(&self) -> RelationProofView<'_, TransportId, ConsumerId> {
        RelationProofView::new(&self.transport_consumers)
    }

    /// return a borrowed proof view over producer consumers
    #[cfg(kani)]
    pub(super) fn producer_consumers(&self) -> RelationProofView<'_, ProducerId, ConsumerId> {
        RelationProofView::new(&self.producer_consumers)
    }

    /// create a detached copy of every reverse relation for tests
    #[cfg(any(test, feature = "test-support", kani))]
    pub(super) fn snapshot(&self) -> RouterIndexSnapshot {
        RouterIndexSnapshot {
            session_transports: self.session_transports.snapshot(),
            transport_producers: self.transport_producers.snapshot(),
            transport_consumers: self.transport_consumers.snapshot(),
            producer_consumers: self.producer_consumers.snapshot(),
        }
    }

    /// verify that session transport indexes mirror the transport primary map
    #[cfg(any(test, feature = "test-support", kani))]
    pub(super) fn session_transports_are_exact<'a, SessionExists, TransportMatches>(
        &self,
        transports: impl IntoIterator<Item = &'a Transport>,
        session_exists: SessionExists,
        transport_matches: TransportMatches,
    ) -> bool
    where
        SessionExists: Fn(SessionId) -> bool,
        TransportMatches: Fn(SessionId, TransportId) -> bool,
    {
        self.session_transports.is_exact_for(
            transports,
            session_exists,
            transport_matches,
            |transport| (transport.session_id(), transport.id()),
        )
    }

    /// verify that transport producer indexes mirror the producer primary map
    #[cfg(any(test, feature = "test-support", kani))]
    pub(super) fn transport_producers_are_exact<'a, TransportExists, ProducerMatches>(
        &self,
        producers: impl IntoIterator<Item = &'a Producer>,
        transport_exists: TransportExists,
        producer_matches: ProducerMatches,
    ) -> bool
    where
        TransportExists: Fn(TransportId) -> bool,
        ProducerMatches: Fn(TransportId, ProducerId) -> bool,
    {
        self.transport_producers.is_exact_for(
            producers,
            transport_exists,
            producer_matches,
            |producer| (producer.transport_id(), producer.id()),
        )
    }

    /// verify that transport consumer indexes mirror the consumer primary map
    #[cfg(any(test, feature = "test-support", kani))]
    pub(super) fn transport_consumers_are_exact<'a, TransportExists, ConsumerMatches>(
        &self,
        consumers: impl IntoIterator<Item = &'a Consumer>,
        transport_exists: TransportExists,
        consumer_matches: ConsumerMatches,
    ) -> bool
    where
        TransportExists: Fn(TransportId) -> bool,
        ConsumerMatches: Fn(TransportId, ConsumerId) -> bool,
    {
        self.transport_consumers.is_exact_for(
            consumers,
            transport_exists,
            consumer_matches,
            |consumer| (consumer.transport_id(), consumer.id()),
        )
    }

    /// verify that producer consumer indexes mirror the consumer primary map
    #[cfg(any(test, feature = "test-support", kani))]
    pub(super) fn producer_consumers_are_exact<'a, ProducerExists, ConsumerMatches>(
        &self,
        consumers: impl IntoIterator<Item = &'a Consumer>,
        producer_exists: ProducerExists,
        consumer_matches: ConsumerMatches,
    ) -> bool
    where
        ProducerExists: Fn(ProducerId) -> bool,
        ConsumerMatches: Fn(ProducerId, ConsumerId) -> bool,
    {
        self.producer_consumers.is_exact_for(
            consumers,
            producer_exists,
            consumer_matches,
            |consumer| (consumer.producer_id(), consumer.id()),
        )
    }
}
