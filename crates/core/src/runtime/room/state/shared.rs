use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities, MediaKind};

use super::{
    super::{
        RoomAdmissionPolicy, RoomMediaCounts, RoomUserPermissions,
        outbound::OutboundSender,
        placement::RoomPlacementUsageSnapshot,
        topology::{RoomRouterStateFactory, RoomTopology, RoutedConsumerId, RoutedProducerId},
        user_negotiation::UserNegotiation,
    },
    ids::ProducerRuntimeId,
    layout::UserLayout,
    media::relay::{RelayRouteEffect, RoomRelayRoutes},
    presence::UserPresence,
};
use crate::runtime::{
    ConnectionId, RecordingState, UserId,
    media_transport::TransportMediaId,
    router_events::RoomRouterEventSink,
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId, SourceEncodingId,
        SourceSubscriptionIntent, UserStreamId,
    },
};

/// Core mutable state for a single SFU room (room).
///
/// Owns all user, producer, and consumer bookkeeping. Every mutation returns
/// an `*Outcome` value that carries deferred side-effects (fan-out messages,
/// kicked senders). The caller is responsible for calling `.emit()` on outcomes
/// after releasing any lock on this state so the critical section stays pure
/// and non-blocking.
///
/// The two-phase patterns (`prepare_*` / `commit_*`) allow async transport work
/// to happen between phases without holding the state lock.
#[derive(Debug)]
pub(in crate::runtime::room) struct RoomState {
    pub(super) admission_policy: RoomAdmissionPolicy,
    pub(super) users: BTreeMap<UserId, ActiveUser>,
    /// Monotonically increasing: each join, including re-joins, gets a fresh id
    /// so stale async callbacks from a previous connection are rejected.
    pub(super) next_connection_id: u64,
    pub(super) next_source_id: u64,
    pub(super) next_source_encoding_id: u64,
    pub(super) next_producer_id: u64,
    pub(super) next_consumer_id: u64,
    pub(super) recording_state: RecordingState,
    pub(super) media: RoomMediaGraph,
    /// Shadow of user/producer/consumer state inside the pure router core.
    pub(super) topology: RoomTopology,
}

/// room-owned media graph and reverse indexes
///
/// source, producer and consumer stores live together because their teardown
/// rules are one graph
/// callers should update them through narrow room-state methods so indexes
/// cannot drift from the owning stores
#[derive(Debug, Default)]
pub(in crate::runtime::room) struct RoomMediaGraph {
    pub(super) sources: BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
    pub(super) source_ids_by_owner_stream: BTreeMap<SourceKey, PublishedSourceId>,
    pub(super) source_ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    pub(super) producer_id_by_source_id: BTreeMap<PublishedSourceId, ProducerRuntimeId>,
    pub(super) producer_ids_by_owner: BTreeMap<UserId, BTreeSet<ProducerRuntimeId>>,
    pub(super) producers: BTreeMap<ProducerRuntimeId, PublishedProducer>,
    pub(super) source_transport_media_index:
        BTreeMap<TransportMediaId, SourceTransportMediaIndexEntry>,
    pub(super) consumer_source_selections: BTreeMap<ConsumerKey, ConsumerSourceSelection>,
    pub(super) consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    pub(super) pending_consumer_bootstraps: BTreeSet<ConsumerKey>,
    pub(super) consumer_keys_by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    pub(super) consumer_keys_by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
    pub(super) relay_routes: RoomRelayRoutes,
}

/// Uniquely identifies one consumer's desired or realized route to a source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime::room) struct ConsumerKey {
    pub(super) consumer_user_id: UserId,
    pub(super) source_id: PublishedSourceId,
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

#[derive(Debug)]
pub(in crate::runtime::room) struct ActiveUser {
    #[allow(
        dead_code,
        reason = "stored for future user display and recording metadata"
    )]
    pub(super) label: Option<String>,
    #[allow(dead_code, reason = "stored for future permission-gated actions")]
    pub(super) permissions: RoomUserPermissions,
    pub(super) presence: UserPresence,
    pub(super) layout: UserLayout,
    pub(super) negotiation: UserNegotiation,
    pub(super) desired_source_subscriptions:
        BTreeMap<UserId, BTreeMap<UserStreamId, SourceSubscriptionIntent>>,
    pub(super) parsed_client_rtp_capabilities: Option<RouterRtpCapabilities>,
    pub(super) connection_id: ConnectionId,
    pub(super) sender: OutboundSender,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PublishedProducer {
    pub(super) source_id: PublishedSourceId,
    pub(super) owner_user_id: UserId,
    pub(super) owner_connection_id: ConnectionId,
    pub(super) stream_id: UserStreamId,
    pub(super) media_kind: MediaKind,
    pub(super) consumable_rtp_parameters: o_sfu_router::MediaStream,
    pub(super) routed_producer_id: RoutedProducerId,
    pub(super) transport_media_id: Option<TransportMediaId>,
    pub(super) active: bool,
}

#[derive(Debug)]
pub(super) struct PublishedSourceInstall {
    pub(super) source_key: SourceKey,
    pub(super) source_descriptor: PublishedSourceDescriptor,
    pub(super) source_encoding_ids: Vec<SourceEncodingId>,
    pub(super) producer_id: ProducerRuntimeId,
    pub(super) producer: PublishedProducer,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct SourceTransportMediaIndexEntry {
    pub(super) source_id: PublishedSourceId,
    pub(super) encoding_ids: Vec<SourceEncodingId>,
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    stream_id: UserStreamId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::room) struct ConsumerState {
    pub(super) routed_consumer_id: RoutedConsumerId,
    pub(super) consumer_connection_id: ConnectionId,
    pub(super) source_connection_id: ConnectionId,
    pub(super) source_media: TransportMediaId,
    pub(super) consumer_media: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ConsumerRouteView<'a> {
    pub(super) consumer_user_id: UserId,
    pub(super) state: ConsumerState,
    pub(super) source: &'a PublishedSourceDescriptor,
    pub(super) producer: &'a PublishedProducer,
    pub(super) selection: Option<ConsumerSourceSelection>,
}

impl ConsumerRouteView<'_> {
    pub(super) fn selection_or_open(&self, active: bool) -> ConsumerSourceSelection {
        self.selection
            .unwrap_or_else(|| ConsumerSourceSelection::open(active))
    }

    pub(super) fn transport_ref(&self) -> ConsumerRouteTransportRef {
        ConsumerRouteTransportRef::from_parts(
            self.consumer_user_id.clone(),
            self.state.consumer_connection_id,
            self.state.consumer_media,
            self.source.owner().user_id().clone(),
            self.state.source_connection_id,
            self.state.source_media,
        )
    }

    pub(super) fn matches_transport_ref(&self, route: &ConsumerRouteTransportRef) -> bool {
        self.consumer_user_id == *route.consumer_user_id()
            && self.state.consumer_connection_id == route.consumer_connection_id()
            && self.state.consumer_media == route.consumer_media()
            && self.source.owner().user_id() == route.source_user_id()
            && self.state.source_connection_id == route.source_connection_id()
            && self.state.source_media == route.source_media()
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct PendingConsumerRouteView<'a> {
    pub(super) source: &'a PublishedSourceDescriptor,
    pub(super) producer: Option<&'a PublishedProducer>,
    pub(super) selection: Option<ConsumerSourceSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct ConsumerRouteTransportRef {
    consumer_user_id: UserId,
    consumer_connection_id: ConnectionId,
    consumer_media: TransportMediaId,
    source_user_id: UserId,
    source_connection_id: ConnectionId,
    source_media: TransportMediaId,
}

impl ConsumerRouteTransportRef {
    pub(super) fn from_parts(
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct TransportMediaRemoval {
    pub user: UserId,
    pub connection: ConnectionId,
    pub transport_media: TransportMediaId,
}

impl RoomState {
    pub fn new(
        runtime_context: &super::super::RoomRuntimeContext,
        admission_policy: RoomAdmissionPolicy,
        router_rtp_capabilities: MediaCapabilities,
        router_event_sink: Arc<dyn RoomRouterEventSink>,
    ) -> Self {
        Self {
            admission_policy,
            users: BTreeMap::new(),
            next_connection_id: 0,
            next_source_id: 1,
            next_source_encoding_id: 1,
            next_producer_id: 1,
            next_consumer_id: 1,
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            media: RoomMediaGraph::default(),
            topology: RoomTopology::new_with_router_state_factory(
                runtime_context.local_routers().clone(),
                router_rtp_capabilities,
                &RoomRouterStateFactory::new(router_event_sink),
            ),
        }
    }

    pub fn collect_consumer_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut keys = BTreeSet::new();
        for user_id in departing_user_ids {
            if let Some(user_keys) = self.media.consumer_keys_by_user.get(user_id) {
                keys.extend(user_keys.iter().cloned());
            }
            if let Some(source_ids) = self.media.source_ids_by_owner.get(user_id) {
                for source_id in source_ids {
                    if let Some(source_keys) = self.media.consumer_keys_by_source.get(source_id) {
                        keys.extend(source_keys.iter().cloned());
                    }
                }
            }
        }
        keys.into_iter()
            .filter_map(|key| {
                let consumer_state = self.media.consumer_index.get(&key)?;
                Some(TransportMediaRemoval {
                    user: key.consumer_user_id,
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            })
            .collect()
    }

    pub fn collect_producer_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        departing_user_ids
            .iter()
            .filter_map(|user_id| self.media.producer_ids_by_owner.get(user_id))
            .flat_map(|producer_ids| producer_ids.iter())
            .filter_map(|producer_id| {
                let producer = self.media.producers.get(producer_id)?;
                let transport_media = producer.transport_media_id?;
                Some(TransportMediaRemoval {
                    user: producer.owner_user_id.clone(),
                    connection: producer.owner_connection_id,
                    transport_media,
                })
            })
            .collect()
    }

    pub fn collect_user_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = self.collect_producer_transport_removals(departing_user_ids);
        removals.extend(self.collect_consumer_transport_removals(departing_user_ids));
        removals
    }

    pub fn purge_user_media_state(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        let mut relay_effects = Vec::new();
        let source_ids = self
            .media
            .take_source_ids_for_owner(user_id)
            .into_iter()
            .collect::<Vec<_>>();
        for source_id in source_ids {
            if let Some((_producer, effects)) = self.media.remove_source_registry_entry(source_id) {
                relay_effects.extend(effects);
            }
        }
        let consumer_keys = self
            .media
            .take_consumer_keys_for_user(user_id)
            .into_iter()
            .collect::<Vec<_>>();
        for key in consumer_keys {
            relay_effects.extend(self.media.remove_consumer_key_state(&key));
        }
        relay_effects
    }

    pub fn user_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<&ActiveUser> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        Some(user)
    }

    pub fn user_mut_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<&mut ActiveUser> {
        let user = self.users.get_mut(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        Some(user)
    }

    pub fn recording_state(&self) -> RecordingState {
        self.recording_state.clone()
    }

    pub fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.topology.rtp_capabilities().clone()
    }

    pub fn transport_user_entries(&self) -> Vec<(UserId, ConnectionId)> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), user.connection_id))
            .collect()
    }

    pub fn transport_consumer_entries(&self) -> Vec<(UserId, ConnectionId)> {
        let mut entries = self
            .media
            .consumer_index
            .iter()
            .map(|(key, state)| (key.consumer_user_id.clone(), state.consumer_connection_id))
            .collect::<Vec<_>>();
        entries.extend(
            self.media
                .pending_consumer_bootstraps
                .iter()
                .filter_map(|key| {
                    self.users
                        .get(&key.consumer_user_id)
                        .map(|user| (key.consumer_user_id.clone(), user.connection_id))
                }),
        );
        entries
    }

    pub fn placement_usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        RoomPlacementUsageSnapshot::new(
            self.topology.primary_router_id(),
            self.topology.has_assigned_local_placements(),
            self.topology.local_placements(),
        )
    }

    pub fn user_connection_id(&self, user_id: &UserId) -> Option<ConnectionId> {
        self.users.get(user_id).map(|user| user.connection_id)
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    pub(super) fn current_live_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.media.live_consumer_routes().filter(|route| {
            self.user_connection_id(&route.consumer_user_id)
                .is_some_and(|connection_id| connection_id == route.state.consumer_connection_id)
        })
    }

    pub fn publication_count(&self) -> usize {
        self.media.sources.len()
    }

    pub fn subscription_count(&self) -> usize {
        self.media
            .consumer_index
            .len()
            .saturating_add(self.media.pending_consumer_bootstraps.len())
    }

    pub fn media_counts(&self) -> RoomMediaCounts {
        RoomMediaCounts {
            publications: self.publication_count(),
            subscriptions: self.subscription_count(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

impl TransportMediaRemoval {
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

impl SourceTransportMediaIndexEntry {
    pub(super) fn new(
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

    pub(super) fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    pub(super) const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept for test-only inspection of the ownership index"
        )
    )]
    pub(super) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub(super) const fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }
}

impl SourceKey {
    pub(super) fn new(owner_user_id: &UserId, stream_id: &UserStreamId) -> Self {
        Self {
            owner_user_id: owner_user_id.clone(),
            stream_id: stream_id.clone(),
        }
    }
}

impl RoomMediaGraph {
    pub(super) fn install_published_source(&mut self, install: PublishedSourceInstall) {
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

    pub(super) fn register_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        self.source_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(source_id);
    }

    pub(super) fn unregister_source_owner(
        &mut self,
        user_id: &UserId,
        source_id: PublishedSourceId,
    ) {
        remove_from_index_set(&mut self.source_ids_by_owner, user_id, &source_id);
    }

    pub(super) fn register_producer_owner(
        &mut self,
        user_id: &UserId,
        producer_id: ProducerRuntimeId,
    ) {
        self.producer_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(producer_id);
    }

    pub(super) fn unregister_producer_owner(
        &mut self,
        user_id: &UserId,
        producer_id: ProducerRuntimeId,
    ) {
        remove_from_index_set(&mut self.producer_ids_by_owner, user_id, &producer_id);
    }

    pub(super) fn register_consumer_key(&mut self, key: &ConsumerKey) {
        self.consumer_keys_by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.consumer_keys_by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
    }

    pub(super) fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.consumer_source_selections
            .entry(key.clone())
            .and_modify(|selection| selection.set_active(active))
            .or_insert_with(|| ConsumerSourceSelection::open(active));
        self.register_consumer_key(key);
    }

    pub(super) fn ensure_consumer_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.consumer_source_selections
            .entry(key.clone())
            .or_insert(selection);
        self.register_consumer_key(key);
    }

    pub(super) fn reserve_pending_consumer_bootstrap(&mut self, key: ConsumerKey) {
        self.pending_consumer_bootstraps.insert(key);
    }

    pub(super) fn remove_pending_consumer_bootstrap(&mut self, key: &ConsumerKey) {
        self.pending_consumer_bootstraps.remove(key);
        self.prune_consumer_key_indexes_if_unused(key);
    }

    pub(super) fn insert_consumer_route(&mut self, key: ConsumerKey, state: ConsumerState) {
        self.consumer_index.insert(key, state);
    }

    pub(super) fn live_consumer_routes(&self) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.consumer_index
            .iter()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, *state))
    }

    pub(super) fn consumer_route_for_key(
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

    pub(super) fn committed_consumer_route_for_key(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let state = *self.consumer_index.get(key)?;
        self.consumer_route_for_key(key, state)
    }

    pub(super) fn pending_consumer_routes_for_user(
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

    pub(super) fn producer_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedProducer> {
        self.producer_id_by_source_id
            .get(&source_id)
            .and_then(|producer_id| self.producers.get(producer_id))
    }

    pub(super) fn prune_consumer_key_indexes_if_unused(&mut self, key: &ConsumerKey) {
        if self.consumer_index.contains_key(key)
            || self.pending_consumer_bootstraps.contains(key)
            || self.consumer_source_selections.contains_key(key)
        {
            return;
        }
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
    }

    pub(super) fn remove_consumer_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        self.consumer_index.remove(key);
        self.pending_consumer_bootstraps.remove(key);
        self.consumer_source_selections.remove(key);
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
        self.relay_routes
            .release_consumer_key(&key.consumer_user_id, key.source_id)
    }

    pub(super) fn consumer_keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_user
            .get(user_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn consumer_keys_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<ConsumerKey> {
        self.consumer_keys_by_source
            .get(&source_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn producer_ids_for_user(&self, user_id: &UserId) -> Vec<ProducerRuntimeId> {
        self.producer_ids_by_owner
            .get(user_id)
            .map(|producer_ids| producer_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(super) fn producer_id_for_source_key(
        &self,
        source_key: &SourceKey,
    ) -> Option<ProducerRuntimeId> {
        let source_id = self.source_ids_by_owner_stream.get(source_key)?;
        self.producer_id_by_source_id.get(source_id).copied()
    }

    pub(super) fn remove_source_registry_entry(
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

    pub(super) fn take_source_ids_for_owner(
        &mut self,
        user_id: &UserId,
    ) -> BTreeSet<PublishedSourceId> {
        self.source_ids_by_owner.remove(user_id).unwrap_or_default()
    }

    pub(super) fn take_consumer_keys_for_user(
        &mut self,
        user_id: &UserId,
    ) -> BTreeSet<ConsumerKey> {
        self.consumer_keys_by_user
            .remove(user_id)
            .unwrap_or_default()
    }

    pub(super) fn consumer_bootstrap_exists(&self, consumer_key: &ConsumerKey) -> bool {
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
