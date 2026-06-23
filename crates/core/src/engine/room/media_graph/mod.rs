use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{
    MediaKind, rtp,
    topology::{RoutedConsumerId, RoutedProducerId},
};

use self::{route_graph::RouteGraph, source_index::SourceIndex};
use crate::engine::{
    ConnectionId, UserId,
    media_transport::{TransportConsumerRoute, TransportMediaId},
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId, UserStreamId,
    },
};

mod consumer_setup;
mod ids;
mod producer;
mod route_graph;
mod source_index;
mod subscription;
mod topology;

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(test)]
#[path = "TESTS/route_graph.rs"]
mod route_graph_tests;

pub use self::consumer_setup::RemoteTrackSetup;
#[cfg(any(test, feature = "testing-transport"))]
pub use self::subscription::ConsumerRouteState;
pub(super) use self::{
    consumer_setup::{
        ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget, PendingConsumerSetup,
    },
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    producer::{
        ProducerActivityCommit, PublishCommit, PublishIntentPlan, UnpublishCommit, ValidatedPublish,
    },
    route_graph::{RelayRouteEffect, RelayRouteKey, ResolvedRelayRouteEffect},
    subscription::{ReceiverRouteActivity, ReceiverRouteCommit, ReceiverRouteWork},
    topology::{
        CommittedTransportReceipt, MediaTopologyEffects, RoomTopology, SessionPlacementCommit,
        SessionPlacementRejection,
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
    pub consumable_rtp_parameters: rtp::MediaStream,
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
    pub source: PublishedSourceId,
    pub owner: UserId,
    pub stream: UserStreamId,
}

#[allow(
    clippy::struct_field_names,
    reason = "postfix _id is intentional because the fields are all identity values"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProducerRouteTarget {
    pub source_id: PublishedSourceId,
    producer_id: ProducerRuntimeId,
    pub owner_connection_id: ConnectionId,
    pub routed_producer_id: RoutedProducerId,
    pub transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TransportMediaRemoval {
    pub user: UserId,
    pub connection: ConnectionId,
    pub transport_media: TransportMediaId,
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

#[derive(Debug, Clone, Copy)]
pub(super) struct SourceView<'a> {
    pub source: &'a PublishedSourceDescriptor,
    pub producer: &'a PublishedProducer,
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
        self.consumer_user_id == route.consumer_user_id
            && self.state.consumer_connection_id == route.consumer_connection_id
            && self.state.consumer_media == route.consumer_media
            && self.source.owner().user_id() == &route.source_user_id
            && self.state.source_connection_id == route.source_connection_id
            && self.state.source_media == route.source_media
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PendingConsumerRouteView<'a> {
    pub source: &'a PublishedSourceDescriptor,
    pub producer: &'a PublishedProducer,
    pub selection: Option<ConsumerSourceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConsumerRouteTransportRef {
    pub consumer_user_id: UserId,
    pub consumer_connection_id: ConnectionId,
    pub consumer_media: TransportMediaId,
    pub source_user_id: UserId,
    pub source_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRouteTarget {
    transport_ref: ConsumerRouteTransportRef,
    transport_route: TransportConsumerRoute,
    stream_id: UserStreamId,
    kind: MediaKind,
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
}

impl ConsumerRouteTarget {
    fn new(
        transport_ref: ConsumerRouteTransportRef,
        transport_route: TransportConsumerRoute,
        stream_id: UserStreamId,
        kind: MediaKind,
    ) -> Self {
        Self {
            transport_ref,
            transport_route,
            stream_id,
            kind,
        }
    }

    pub const fn transport_route(&self) -> &TransportConsumerRoute {
        &self.transport_route
    }

    pub const fn consumer_media_id(&self) -> TransportMediaId {
        self.transport_ref.consumer_media
    }

    pub fn producer_user_id(&self) -> &UserId {
        &self.transport_ref.source_user_id
    }

    pub const fn source_media_id(&self) -> TransportMediaId {
        self.transport_ref.source_media
    }

    pub fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    pub fn request_keyframe_after_activity(&self, active: bool) -> bool {
        active && self.kind == MediaKind::Video
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
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn producer_count(&self) -> usize {
        self.sources.producer_count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn consumer_count(&self) -> usize {
        self.routes.count()
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

    #[cfg(test)]
    pub fn ensure_consumer_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.routes.ensure_selection(key, selection);
    }

    #[cfg(test)]
    pub fn commit_consumer(
        &mut self,
        key: ConsumerKey,
        state: ConsumerState,
        selection: ConsumerSourceSelection,
    ) -> bool {
        let Some(reservation) = self.routes.reserve_consumer_setup(key, selection) else {
            return false;
        };
        self.routes.commit(&reservation, state, selection)
    }

    #[cfg(test)]
    pub fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.routes.consumer_state(key)
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

    pub fn consumer_keys_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> impl Iterator<Item = &ConsumerKey> {
        self.routes.keys_for_source(source_id)
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
        let consumer_keys = self
            .consumer_keys_for_source(source_id)
            .cloned()
            .collect::<Vec<_>>();
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

    pub fn transport_removals_for_user(&self, user_id: &UserId) -> Vec<TransportMediaRemoval> {
        self.transport_removals_for_users(&BTreeSet::from([user_id.clone()]))
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
}

impl ProducerRouteTarget {
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
