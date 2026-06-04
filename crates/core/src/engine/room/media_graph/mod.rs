use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use self::{route_graph::RouteGraph, source_index::SourceIndex};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{RelayRouteActivity, TransportMediaId},
    room::routing::{RoutedConsumerId, RoutedProducerId},
    source_model::{
        ActiveSpeakerGroup, ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
        UserStreamId,
    },
};

mod ids;
mod producer;
mod route_graph;
mod source_index;
mod subscription;

#[cfg(test)]
mod route_graph_tests;
#[cfg(test)]
mod tests;

pub use self::subscription::{ConsumerRouteState, RemoteTrackSetup};
pub(super) use self::{
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    producer::ValidatedPublish,
    route_graph::{RelayRouteEffect, RelayRouteKey, ResolvedRelayRouteEffect},
    subscription::{
        ConsumerRouteUpdate, ConsumerSetupCommit, ConsumerSetupOrigin, ConsumerSetupPlan,
        ConsumerSetupTarget,
    },
};

#[derive(Debug, Default)]
pub(super) struct RoomMediaGraph {
    sources: SourceIndex,
    routes: RouteGraph,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ConsumerKey {
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
pub(super) struct SourceKey {
    owner_user_id: UserId,
    stream_id: UserStreamId,
}

#[derive(Debug, Clone)]
pub(super) struct PublishedProducer {
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
pub(super) struct PublishedSourceInstall {
    pub source_descriptor: PublishedSourceDescriptor,
    pub producer_id: ProducerRuntimeId,
    pub producer: PublishedProducer,
    pub transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceTransportMediaIndexEntry {
    source: PublishedSourceId,
    owner: UserId,
    stream: UserStreamId,
}

#[allow(
    clippy::struct_field_names,
    reason = "postfix _id is intentional because the fields are all identity values"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProducerRouteTarget {
    source_id: PublishedSourceId,
    producer_id: ProducerRuntimeId,
    owner_connection_id: ConnectionId,
    routed_producer_id: RoutedProducerId,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TransportMediaRemoval {
    user: UserId,
    connection: ConnectionId,
    transport_media: TransportMediaId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConsumerState {
    pub routed_consumer_id: RoutedConsumerId,
    pub consumer_connection_id: ConnectionId,
    pub source_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
    pub consumer_media: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(super) struct ConsumerRouteView<'a> {
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
pub(super) struct PendingConsumerRouteView<'a> {
    pub source: &'a PublishedSourceDescriptor,
    pub producer: Option<&'a PublishedProducer>,
    pub selection: Option<ConsumerSourceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConsumerRouteTransportRef {
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
    pub fn new(source: PublishedSourceId, owner: UserId, stream: UserStreamId) -> Self {
        Self {
            source,
            owner,
            stream,
        }
    }

    pub fn owner_user_id(&self) -> &UserId {
        &self.owner
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.source
    }

    pub const fn stream_id(&self) -> &UserStreamId {
        &self.stream
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
    pub fn publication_count(&self) -> usize {
        self.sources.publication_count()
    }

    pub fn subscription_count(&self) -> usize {
        self.routes.subscription_count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn producer_count(&self) -> usize {
        self.sources.producer_count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn consumer_count(&self) -> usize {
        self.routes.count()
    }

    pub fn sources(&self) -> impl Iterator<Item = &PublishedSourceDescriptor> {
        self.sources.sources()
    }

    pub fn producers(&self) -> impl Iterator<Item = (ProducerRuntimeId, &PublishedProducer)> {
        self.sources.producers()
    }

    pub fn active_producer_stream_owners(&self) -> impl Iterator<Item = (&UserStreamId, &UserId)> {
        self.sources.active_producer_stream_owners()
    }

    pub fn source(&self, source_id: PublishedSourceId) -> Option<&PublishedSourceDescriptor> {
        self.sources.source(source_id)
    }

    pub fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.sources.transport_media_entry(transport_media_id)
    }

    pub fn producer_stream_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserStreamId> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.stream_id().clone())
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.sources.first_published_transport_media_id()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        self.sources
            .producer_transport_media_id(user_id, connection_id, stream_id)
    }

    pub fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.sources.id_for_owner_stream(owner_user_id, stream_id)
    }

    pub fn has_source_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.source_id_for_owner_stream(owner_user_id, stream_id)
            .is_some()
    }

    pub fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        self.sources
            .producer_route_target(owner_user_id, owner_connection_id, stream_id)
    }

    pub fn producer_for_route_target(
        &self,
        target: &ProducerRouteTarget,
        current_connection_id: Option<ConnectionId>,
    ) -> Option<&PublishedProducer> {
        self.sources
            .producer_for_route_target(target, current_connection_id)
    }

    pub fn set_producer_active(&mut self, target: &ProducerRouteTarget, active: bool) -> bool {
        self.sources.set_producer_active(target, active)
    }

    pub fn publications_for_user_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> impl Iterator<Item = (&PublishedSourceDescriptor, &PublishedProducer)> {
        self.sources
            .publications_for_user_connection(user_id, connection_id)
    }

    pub fn owner_has_promotable_source_in_group(
        &self,
        owner_user_id: &UserId,
        group: ActiveSpeakerGroup,
    ) -> bool {
        self.sources
            .owner_has_promotable_source_in_group(owner_user_id, group)
    }

    pub fn install_source(&mut self, install: PublishedSourceInstall) {
        self.sources.install_source(install);
    }

    pub fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.routes.set_selection(key, active);
    }

    pub fn consumer_source_selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.routes.selection(key)
    }

    pub fn ensure_consumer_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.routes.ensure_selection(key, selection);
    }

    pub fn update_consumer_source_selection(
        &mut self,
        route: &ConsumerRouteTransportRef,
        source_id: PublishedSourceId,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        let key = ConsumerKey::new(route.consumer_user_id(), source_id);
        let Some(current_route) = self.committed_consumer_route_for_key(&key) else {
            return false;
        };
        if !current_route.matches_transport_ref(route) {
            return false;
        }
        update(self.routes.selection_mut_or_open(key));
        true
    }

    pub fn reserve_consumer_setup(&mut self, key: ConsumerKey) {
        self.routes.reserve_consumer_setup(key);
    }

    pub fn release_consumer_setup(&mut self, key: &ConsumerKey) {
        self.routes.release_consumer_setup(key);
    }

    pub fn commit_consumer(
        &mut self,
        key: ConsumerKey,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        self.routes.commit(key, state, selection)
    }

    #[cfg(test)]
    pub fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.routes.consumer_state(key)
    }

    pub fn committed_consumer_transport_entries(
        &self,
    ) -> impl Iterator<Item = (UserId, ConnectionId)> + '_ {
        self.routes.committed_consumer_transport_entries()
    }

    pub fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.routes.pending_consumer_user_ids()
    }

    pub fn live_consumer_routes(&self) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.routes
            .committed_entries()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, state))
    }

    pub fn consumer_route_for_key(
        &self,
        key: &ConsumerKey,
        state: ConsumerState,
    ) -> Option<ConsumerRouteView<'_>> {
        let source = self.sources.source(key.source_id)?;
        let producer = self.producer_for_source(key.source_id)?;
        Some(ConsumerRouteView {
            consumer_user_id: key.consumer_user_id.clone(),
            state,
            source,
            producer,
            selection: self.routes.selection(key),
        })
    }

    pub fn committed_consumer_route_for_key(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let state = self.routes.consumer_state(key)?;
        self.consumer_route_for_key(key, state)
    }

    pub fn pending_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PendingConsumerRouteView<'_>> {
        self.routes
            .pending_keys_for_user(user_id)
            .filter_map(|key| {
                let source = self.sources.source(key.source_id)?;
                Some(PendingConsumerRouteView {
                    source,
                    producer: self.producer_for_source(key.source_id),
                    selection: self.routes.selection(key),
                })
            })
    }

    pub fn producer_for_source(&self, source_id: PublishedSourceId) -> Option<&PublishedProducer> {
        self.sources.producer_for_source(source_id)
    }

    pub fn producer(&self, producer_id: ProducerRuntimeId) -> Option<&PublishedProducer> {
        self.sources.producer(producer_id)
    }

    pub fn remove_consumer_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        self.routes.remove_key_state(key)
    }

    pub fn remove_user_media(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        let mut relay_effects = Vec::new();
        let source_ids = self.sources.ids_for_owner(user_id).collect::<Vec<_>>();
        for source_id in source_ids {
            if let Some((_producer, effects)) = self.remove_source(source_id) {
                relay_effects.extend(effects);
            }
        }
        for key in self.routes.keys_for_user(user_id) {
            relay_effects.extend(self.remove_consumer_key_state(&key));
        }
        relay_effects
    }

    pub fn consumer_keys_for_source(&self, source_id: PublishedSourceId) -> Vec<ConsumerKey> {
        self.routes.keys_for_source(source_id)
    }

    pub fn routed_consumer_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<RoutedConsumerId> {
        self.routes.routed_consumer_ids_for_source(source_id)
    }

    pub fn routed_consumer_ids_affected_by_user(&self, user_id: &UserId) -> Vec<RoutedConsumerId> {
        let mut consumer_ids = self
            .routes
            .routed_consumer_ids_for_keys(self.consumer_keys_affected_by_user(user_id));
        consumer_ids.sort_unstable();
        consumer_ids.dedup();
        consumer_ids
    }

    fn consumer_keys_affected_by_user(&self, user_id: &UserId) -> BTreeSet<ConsumerKey> {
        self.routes
            .affected_keys_for_user(user_id, self.sources.ids_for_owner(user_id))
    }

    pub fn remove_source(
        &mut self,
        source_id: PublishedSourceId,
    ) -> Option<(PublishedProducer, Vec<RelayRouteEffect>)> {
        self.sources.source(source_id)?;
        let consumer_keys = self.consumer_keys_for_source(source_id);
        let mut relay_effects = Vec::new();
        for key in consumer_keys {
            relay_effects.extend(self.remove_consumer_key_state(&key));
        }
        let producer = self.sources.remove_source(source_id)?;
        Some((producer, relay_effects))
    }

    pub fn transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = self
            .sources
            .producer_transport_removals_for_users(departing_user_ids);
        removals.extend(self.consumer_transport_removals_for_users(departing_user_ids));
        removals
    }

    pub fn transport_removals_for_producer_target(
        &self,
        user_id: &UserId,
        producer_target: &ProducerRouteTarget,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = vec![TransportMediaRemoval::new(
            user_id.clone(),
            producer_target.owner_connection_id,
            producer_target.transport_media_id,
        )];
        removals.extend(
            self.routes
                .transport_removals_for_source(producer_target.source_id),
        );
        removals
    }

    fn consumer_transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let keys = departing_user_ids
            .iter()
            .flat_map(|user_id| self.consumer_keys_affected_by_user(user_id))
            .collect::<BTreeSet<_>>();

        self.routes.transport_removals_for_keys(keys)
    }

    pub fn reserve_relay_consumer(
        &mut self,
        target: &ConsumerSetupTarget,
        source_connection_id: ConnectionId,
        source_transport_media_id: TransportMediaId,
        target_media_worker_id: MediaWorkerId,
        active: bool,
    ) -> Vec<RelayRouteEffect> {
        self.routes.reserve_relay(
            target,
            source_connection_id,
            source_transport_media_id,
            target_media_worker_id,
            active,
        )
    }

    pub fn set_relay_consumer_active(
        &mut self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
        source_id: PublishedSourceId,
        activity: RelayRouteActivity,
    ) -> Vec<RelayRouteEffect> {
        self.routes.set_relay_active(
            consumer_user_id,
            consumer_connection_id,
            source_id,
            activity,
        )
    }

    pub fn release_pending_relay_target(
        &mut self,
        target: &ConsumerSetupTarget,
    ) -> Vec<RelayRouteEffect> {
        self.routes.release_target(target)
    }

    pub fn has_consumer_setup_or_route(&self, consumer_key: &ConsumerKey) -> bool {
        self.routes.has_consumer_setup_or_route(consumer_key)
    }

    pub fn contains_consumer(&self, key: &ConsumerKey) -> bool {
        self.routes.contains(key)
    }
}

impl ProducerRouteTarget {
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub const fn routed_producer_id(&self) -> RoutedProducerId {
        self.routed_producer_id
    }

    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    pub const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    fn matches_producer(&self, producer: &PublishedProducer) -> bool {
        producer.source_id == self.source_id
            && producer.owner_connection_id == self.owner_connection_id
            && producer.routed_producer_id == self.routed_producer_id
            && producer.transport_media_id == Some(self.transport_media_id)
    }
}

impl TransportMediaRemoval {
    pub fn new(user: UserId, connection: ConnectionId, transport_media: TransportMediaId) -> Self {
        Self {
            user,
            connection,
            transport_media,
        }
    }

    pub fn user(&self) -> &UserId {
        &self.user
    }

    pub const fn connection(&self) -> ConnectionId {
        self.connection
    }

    pub const fn transport_media(&self) -> TransportMediaId {
        self.transport_media
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
