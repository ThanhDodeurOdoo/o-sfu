use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::{
    super::ids::ProducerRuntimeId,
    relay::{RelayRouteEffect, RoomRelayRoutes},
};
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::TransportMediaId,
    room::topology::{RoutedConsumerId, RoutedProducerId},
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId, SourceEncodingId,
        UserStreamId,
    },
};

/// Room-owned media graph and reverse indexes.
///
/// Source, producer and consumer stores live together because their teardown
/// rules are one graph. Callers update them through graph operations so indexes
/// cannot drift from the owning stores.
#[derive(Debug, Default)]
pub struct RoomMediaGraph {
    pub sources: BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
    pub source_ids_by_owner_stream: BTreeMap<SourceKey, PublishedSourceId>,
    pub source_ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    pub producer_id_by_source_id: BTreeMap<PublishedSourceId, ProducerRuntimeId>,
    pub producer_ids_by_owner: BTreeMap<UserId, BTreeSet<ProducerRuntimeId>>,
    pub producers: BTreeMap<ProducerRuntimeId, PublishedProducer>,
    pub source_transport_media_index: BTreeMap<TransportMediaId, SourceTransportMediaIndexEntry>,
    pub consumer_source_selections: BTreeMap<ConsumerKey, ConsumerSourceSelection>,
    pub consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    pub pending_consumer_bootstraps: BTreeSet<ConsumerKey>,
    pub consumer_keys_by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    pub consumer_keys_by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
    pub relay_routes: RoomRelayRoutes,
}

/// Uniquely identifies one consumer's desired or realized route to a source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumerKey {
    pub consumer_user_id: UserId,
    pub source_id: PublishedSourceId,
}

impl ConsumerKey {
    pub fn new(consumer_user_id: &UserId, source_id: PublishedSourceId) -> Self {
        Self {
            consumer_user_id: consumer_user_id.clone(),
            source_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceKey {
    owner_user_id: UserId,
    stream_id: UserStreamId,
}

#[derive(Debug, Clone)]
pub struct PublishedProducer {
    pub source_id: PublishedSourceId,
    pub owner_user_id: UserId,
    pub owner_connection_id: ConnectionId,
    pub stream_id: UserStreamId,
    pub media_kind: MediaKind,
    pub consumable_rtp_parameters: o_sfu_router::MediaStream,
    pub routed_producer_id: RoutedProducerId,
    pub transport_media_id: Option<TransportMediaId>,
    pub active: bool,
}

#[derive(Debug)]
pub struct PublishedSourceInstall {
    pub source_key: SourceKey,
    pub source_descriptor: PublishedSourceDescriptor,
    pub source_encoding_ids: Vec<SourceEncodingId>,
    pub producer_id: ProducerRuntimeId,
    pub producer: PublishedProducer,
    pub transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTransportMediaIndexEntry {
    pub source_id: PublishedSourceId,
    pub encoding_ids: Vec<SourceEncodingId>,
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    stream_id: UserStreamId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerState {
    pub routed_consumer_id: RoutedConsumerId,
    pub consumer_connection_id: ConnectionId,
    pub source_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
    pub consumer_media: TransportMediaId,
}

#[derive(Debug, Clone)]
pub struct ConsumerRouteView<'a> {
    pub consumer_user_id: UserId,
    pub state: ConsumerState,
    pub source: &'a PublishedSourceDescriptor,
    pub producer: &'a PublishedProducer,
    pub selection: Option<ConsumerSourceSelection>,
}

impl ConsumerRouteView<'_> {
    pub fn selection_or_open(&self, active: bool) -> ConsumerSourceSelection {
        self.selection
            .unwrap_or_else(|| ConsumerSourceSelection::open(active))
    }

    pub fn transport_ref(&self) -> ConsumerRouteTransportRef {
        ConsumerRouteTransportRef::from_parts(
            self.consumer_user_id.clone(),
            self.state.consumer_connection_id,
            self.state.consumer_media,
            self.source.owner().user_id().clone(),
            self.state.source_connection_id,
            self.state.source_media,
        )
    }

    pub fn matches_transport_ref(&self, route: &ConsumerRouteTransportRef) -> bool {
        self.consumer_user_id == *route.consumer_user_id()
            && self.state.consumer_connection_id == route.consumer_connection_id()
            && self.state.consumer_media == route.consumer_media()
            && self.source.owner().user_id() == route.source_user_id()
            && self.state.source_connection_id == route.source_connection_id()
            && self.state.source_media == route.source_media()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PendingConsumerRouteView<'a> {
    pub source: &'a PublishedSourceDescriptor,
    pub producer: Option<&'a PublishedProducer>,
    pub selection: Option<ConsumerSourceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRouteTransportRef {
    consumer_user_id: UserId,
    consumer_connection_id: ConnectionId,
    consumer_media: TransportMediaId,
    source_user_id: UserId,
    source_connection_id: ConnectionId,
    source_media: TransportMediaId,
}

impl ConsumerRouteTransportRef {
    pub fn from_parts(
        consumer_user_id: UserId,
        consumer_connection_id: ConnectionId,
        consumer_media: TransportMediaId,
        source_user_id: UserId,
        source_connection_id: ConnectionId,
        source_media: TransportMediaId,
    ) -> Self {
        Self {
            consumer_user_id,
            consumer_connection_id,
            consumer_media,
            source_user_id,
            source_connection_id,
            source_media,
        }
    }

    pub fn consumer_user_id(&self) -> &UserId {
        &self.consumer_user_id
    }

    pub const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_media
    }

    pub fn source_user_id(&self) -> &UserId {
        &self.source_user_id
    }

    pub const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub const fn source_media(&self) -> TransportMediaId {
        self.source_media
    }
}

impl SourceTransportMediaIndexEntry {
    pub fn new(
        source_id: PublishedSourceId,
        encoding_ids: Vec<SourceEncodingId>,
        owner_user_id: UserId,
        owner_connection_id: ConnectionId,
        stream_id: UserStreamId,
    ) -> Self {
        Self {
            source_id,
            encoding_ids,
            owner_user_id,
            owner_connection_id,
            stream_id,
        }
    }

    pub fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept for test-only inspection of the ownership index"
        )
    )]
    pub const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub const fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }
}

impl SourceKey {
    pub fn new(owner_user_id: &UserId, stream_id: &UserStreamId) -> Self {
        Self {
            owner_user_id: owner_user_id.clone(),
            stream_id: stream_id.clone(),
        }
    }
}

impl RoomMediaGraph {
    pub fn install_source(&mut self, install: PublishedSourceInstall) {
        let PublishedSourceInstall {
            source_key,
            source_descriptor,
            source_encoding_ids,
            producer_id,
            producer,
            transport_media_id,
        } = install;
        let source_id = source_descriptor.source_id();
        let owner_user_id = producer.owner_user_id.clone();
        let owner_connection_id = producer.owner_connection_id;
        let stream_id = producer.stream_id.clone();
        self.producers.insert(producer_id, producer);
        self.sources.insert(source_id, source_descriptor);
        self.source_ids_by_owner_stream
            .insert(source_key, source_id);
        self.producer_id_by_source_id.insert(source_id, producer_id);
        self.register_source_owner(&owner_user_id, source_id);
        self.register_producer_owner(&owner_user_id, producer_id);
        self.source_transport_media_index.insert(
            transport_media_id,
            SourceTransportMediaIndexEntry::new(
                source_id,
                source_encoding_ids,
                owner_user_id,
                owner_connection_id,
                stream_id,
            ),
        );
    }

    pub fn register_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        self.source_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(source_id);
    }

    pub fn unregister_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        remove_from_index_set(&mut self.source_ids_by_owner, user_id, &source_id);
    }

    pub fn register_producer_owner(&mut self, user_id: &UserId, producer_id: ProducerRuntimeId) {
        self.producer_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(producer_id);
    }

    pub fn unregister_producer_owner(&mut self, user_id: &UserId, producer_id: ProducerRuntimeId) {
        remove_from_index_set(&mut self.producer_ids_by_owner, user_id, &producer_id);
    }

    pub fn register_consumer_key(&mut self, key: &ConsumerKey) {
        self.consumer_keys_by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.consumer_keys_by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
    }

    pub fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.consumer_source_selections
            .entry(key.clone())
            .and_modify(|selection| selection.set_active(active))
            .or_insert_with(|| ConsumerSourceSelection::open(active));
        self.register_consumer_key(key);
    }

    pub fn ensure_consumer_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.consumer_source_selections
            .entry(key.clone())
            .or_insert(selection);
        self.register_consumer_key(key);
    }

    pub fn reserve_consumer_bootstrap(&mut self, key: ConsumerKey) {
        self.pending_consumer_bootstraps.insert(key);
    }

    pub fn remove_pending_consumer_bootstrap(&mut self, key: &ConsumerKey) {
        self.pending_consumer_bootstraps.remove(key);
        self.prune_consumer_key_indexes_if_unused(key);
    }

    pub fn commit_consumer(
        &mut self,
        key: ConsumerKey,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        if self.consumer_index.contains_key(&key) {
            return false;
        }
        self.ensure_consumer_source_selection(&key, selection);
        self.consumer_index.insert(key, state);
        true
    }

    pub fn live_consumer_routes(&self) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.consumer_index
            .iter()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, *state))
    }

    pub fn consumer_route_for_key(
        &self,
        key: &ConsumerKey,
        state: ConsumerState,
    ) -> Option<ConsumerRouteView<'_>> {
        let source = self.sources.get(&key.source_id)?;
        let producer = self.producer_for_source(key.source_id)?;
        Some(ConsumerRouteView {
            consumer_user_id: key.consumer_user_id.clone(),
            state,
            source,
            producer,
            selection: self.consumer_source_selections.get(key).copied(),
        })
    }

    pub fn committed_consumer_route_for_key(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let state = *self.consumer_index.get(key)?;
        self.consumer_route_for_key(key, state)
    }

    pub fn pending_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PendingConsumerRouteView<'_>> {
        self.pending_consumer_bootstraps
            .iter()
            .filter(|key| {
                key.consumer_user_id == *user_id && !self.consumer_index.contains_key(*key)
            })
            .filter_map(|key| {
                let source = self.sources.get(&key.source_id)?;
                Some(PendingConsumerRouteView {
                    source,
                    producer: self.producer_for_source(key.source_id),
                    selection: self.consumer_source_selections.get(key).copied(),
                })
            })
    }

    pub fn producer_for_source(&self, source_id: PublishedSourceId) -> Option<&PublishedProducer> {
        self.producer_id_by_source_id
            .get(&source_id)
            .and_then(|producer_id| self.producers.get(producer_id))
    }

    pub fn prune_consumer_key_indexes_if_unused(&mut self, key: &ConsumerKey) {
        if self.consumer_index.contains_key(key)
            || self.pending_consumer_bootstraps.contains(key)
            || self.consumer_source_selections.contains_key(key)
        {
            return;
        }
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
    }

    pub fn remove_consumer_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        self.consumer_index.remove(key);
        self.pending_consumer_bootstraps.remove(key);
        self.consumer_source_selections.remove(key);
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
        self.relay_routes
            .release_consumer_key(&key.consumer_user_id, key.source_id)
    }

    pub fn consumer_keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_user
            .get(user_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn consumer_keys_for_source(&self, source_id: PublishedSourceId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_source
            .get(&source_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn producer_ids_for_user(&self, user_id: &UserId) -> Vec<ProducerRuntimeId> {
        self.producer_ids_by_owner
            .get(user_id)
            .map(|producer_ids| producer_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn producer_id_for_source_key(&self, source_key: &SourceKey) -> Option<ProducerRuntimeId> {
        let source_id = self.source_ids_by_owner_stream.get(source_key)?;
        self.producer_id_by_source_id.get(source_id).copied()
    }

    pub fn remove_source(
        &mut self,
        source_id: PublishedSourceId,
    ) -> Option<(PublishedProducer, Vec<RelayRouteEffect>)> {
        let source = self.sources.remove(&source_id)?;
        let consumer_keys = self.consumer_keys_for_source(source_id);
        let mut relay_effects = Vec::new();
        for key in consumer_keys {
            relay_effects.extend(self.remove_consumer_key_state(&key));
        }
        let source_key = SourceKey::new(source.owner().user_id(), source.stream_id());
        self.source_ids_by_owner_stream.remove(&source_key);
        self.unregister_source_owner(source.owner().user_id(), source_id);
        let producer_id = self.producer_id_by_source_id.remove(&source_id)?;
        let producer = self.producers.remove(&producer_id)?;
        self.unregister_producer_owner(&producer.owner_user_id, producer_id);
        if let Some(transport_media_id) = producer.transport_media_id {
            self.source_transport_media_index
                .remove(&transport_media_id);
        }
        Some((producer, relay_effects))
    }

    pub fn take_source_ids_for_owner(&mut self, user_id: &UserId) -> BTreeSet<PublishedSourceId> {
        self.source_ids_by_owner.remove(user_id).unwrap_or_default()
    }

    pub fn take_consumer_keys_for_user(&mut self, user_id: &UserId) -> BTreeSet<ConsumerKey> {
        self.consumer_keys_by_user
            .remove(user_id)
            .unwrap_or_default()
    }

    pub fn consumer_bootstrap_exists(&self, consumer_key: &ConsumerKey) -> bool {
        self.consumer_index.contains_key(consumer_key)
            || self.pending_consumer_bootstraps.contains(consumer_key)
    }
}

fn remove_from_index_set<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K, value: &V)
where
    K: Ord,
    V: Ord,
{
    let Some(values) = index.get_mut(key) else {
        return;
    };
    values.remove(value);
    if values.is_empty() {
        index.remove(key);
    }
}
