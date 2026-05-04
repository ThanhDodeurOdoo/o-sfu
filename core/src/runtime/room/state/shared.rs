use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities, MediaKind};

use super::{
    super::{
        RoomAdmissionPolicy, RoomUserPermissions,
        outbound::OutboundSender,
        topology::{RoomRouterObserverFactory, RoomTopology, RoutedConsumerId, RoutedProducerId},
        user_negotiation::UserNegotiation,
    },
    ids::ProducerRuntimeId,
    layout::UserLayout,
    presence::UserPresence,
};
use crate::{
    RoomShardingPolicy,
    runtime::{
        ConnectionId, RecordingState, UserId,
        media_transport::TransportMediaId,
        recording::RecordingService,
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
            SourceEncodingId, SourceSubscriptionIntent, UserStreamId,
        },
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
    /// Source graph keyed by stable room-domain source id.
    pub(super) sources: BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
    /// Source lookup keyed by publisher user and orchestration stream id.
    pub(super) source_ids_by_owner_stream: BTreeMap<SourceKey, PublishedSourceId>,
    /// Published source ids owned by each room user.
    ///
    /// This is an ownership index for local teardown. It is updated in the
    /// same state transition as `sources` and prevents a departing user from
    /// scanning unrelated source entries.
    pub(super) source_ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    /// Current routed producer realization keyed by source id.
    pub(super) producer_id_by_source_id: BTreeMap<PublishedSourceId, ProducerRuntimeId>,
    /// Runtime producer ids owned by each room user.
    ///
    /// Producer media cleanup uses this index before state mutation so
    /// replacement joins and disconnect bursts only inspect the departing
    /// user's producers.
    pub(super) producer_ids_by_owner: BTreeMap<UserId, BTreeSet<ProducerRuntimeId>>,
    /// Keyed by typed runtime producer id. Compatibility wire ids are rendered at the edge.
    pub(super) producers: BTreeMap<ProducerRuntimeId, PublishedProducer>,
    /// Source ownership and encoding metadata keyed by transport-owned media ids.
    pub(super) source_transport_media_index:
        BTreeMap<TransportMediaId, SourceTransportMediaIndexEntry>,
    /// Desired per-consumer source state keyed above transport realization.
    pub(super) consumer_source_selections: BTreeMap<ConsumerKey, ConsumerSourceSelection>,
    /// Concrete routed consumer media currently realizing a source selection.
    pub(super) consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    pub(super) pending_consumer_bootstraps: BTreeSet<ConsumerKey>,
    /// Consumer keys grouped by receiver and by source.
    ///
    /// These are ownership facts for all room-owned consumer key stores:
    /// desired source selections, pending bootstraps, and realized consumers.
    /// A key stays indexed while any of those stores contains it.
    pub(super) consumer_keys_by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    pub(super) consumer_keys_by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
    /// Shadow of user/producer/consumer state inside the pure router core.
    pub(super) topology: RoomTopology,
}

/// Uniquely identifies one consumer's desired or realized route to a source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime::room) struct ConsumerKey {
    pub(super) consumer_user_id: UserId,
    pub(super) source_id: PublishedSourceId,
}

impl ConsumerKey {
    pub(in crate::runtime::room) fn new(
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct TransportMediaRemoval {
    pub(in crate::runtime::room) user: UserId,
    pub(in crate::runtime::room) connection: ConnectionId,
    pub(in crate::runtime::room) transport_media: TransportMediaId,
}

impl RoomState {
    pub(in crate::runtime::room) fn new(
        runtime_context: &super::super::RoomRuntimeContext,
        admission_policy: RoomAdmissionPolicy,
        router_rtp_capabilities: MediaCapabilities,
        room_sharding_policy: RoomShardingPolicy,
        recording_service: Arc<RecordingService>,
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
            sources: BTreeMap::new(),
            source_ids_by_owner_stream: BTreeMap::new(),
            source_ids_by_owner: BTreeMap::new(),
            producer_id_by_source_id: BTreeMap::new(),
            producer_ids_by_owner: BTreeMap::new(),
            producers: BTreeMap::new(),
            source_transport_media_index: BTreeMap::new(),
            consumer_source_selections: BTreeMap::new(),
            consumer_index: BTreeMap::new(),
            pending_consumer_bootstraps: BTreeSet::new(),
            consumer_keys_by_user: BTreeMap::new(),
            consumer_keys_by_source: BTreeMap::new(),
            topology: RoomTopology::new_with_recording_observer_factory(
                runtime_context.local_routers().clone(),
                room_sharding_policy,
                router_rtp_capabilities,
                &RoomRouterObserverFactory::new(recording_service),
            ),
        }
    }

    pub(in crate::runtime::room) fn collect_consumer_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut keys = BTreeSet::new();
        for user_id in departing_user_ids {
            if let Some(user_keys) = self.consumer_keys_by_user.get(user_id) {
                keys.extend(user_keys.iter().cloned());
            }
            if let Some(source_ids) = self.source_ids_by_owner.get(user_id) {
                for source_id in source_ids {
                    if let Some(source_keys) = self.consumer_keys_by_source.get(source_id) {
                        keys.extend(source_keys.iter().cloned());
                    }
                }
            }
        }
        keys.into_iter()
            .filter_map(|key| {
                let consumer_state = self.consumer_index.get(&key)?;
                Some(TransportMediaRemoval {
                    user: key.consumer_user_id,
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            })
            .collect()
    }

    pub(in crate::runtime::room) fn collect_producer_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        departing_user_ids
            .iter()
            .filter_map(|user_id| self.producer_ids_by_owner.get(user_id))
            .flat_map(|producer_ids| producer_ids.iter())
            .filter_map(|producer_id| {
                let producer = self.producers.get(producer_id)?;
                let transport_media = producer.transport_media_id?;
                Some(TransportMediaRemoval {
                    user: producer.owner_user_id.clone(),
                    connection: producer.owner_connection_id,
                    transport_media,
                })
            })
            .collect()
    }

    pub(in crate::runtime::room) fn collect_user_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = self.collect_producer_transport_removals(departing_user_ids);
        removals.extend(self.collect_consumer_transport_removals(departing_user_ids));
        removals
    }

    pub(in crate::runtime::room) fn purge_user_media_state(&mut self, user_id: &UserId) {
        let source_ids = self
            .source_ids_by_owner
            .remove(user_id)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        for source_id in source_ids {
            self.remove_source_registry_entry(source_id);
        }
        let consumer_keys = self
            .consumer_keys_by_user
            .remove(user_id)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        for key in consumer_keys {
            self.remove_consumer_key_state(&key);
        }
    }

    pub(in crate::runtime::room) fn user_for_connection(
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

    pub(in crate::runtime::room) fn user_mut_for_connection(
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

    pub(in crate::runtime::room) fn recording_state(&self) -> RecordingState {
        self.recording_state.clone()
    }

    pub(in crate::runtime::room) fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.topology.rtp_capabilities().clone()
    }

    pub(in crate::runtime::room) fn transport_user_entries(&self) -> Vec<(UserId, ConnectionId)> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), user.connection_id))
            .collect()
    }

    pub(in crate::runtime::room) fn user_connection_id(
        &self,
        user_id: &UserId,
    ) -> Option<ConnectionId> {
        self.users.get(user_id).map(|user| user.connection_id)
    }

    pub(in crate::runtime::room) fn user_count(&self) -> usize {
        self.users.len()
    }

    pub(in crate::runtime::room) fn publication_count(&self) -> usize {
        self.sources.len()
    }

    pub(in crate::runtime::room) fn subscription_count(&self) -> usize {
        self.consumer_index
            .len()
            .saturating_add(self.pending_consumer_bootstraps.len())
    }

    pub(in crate::runtime::room) fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

impl TransportMediaRemoval {
    pub(in crate::runtime::room) fn user(&self) -> &UserId {
        &self.user
    }

    pub(in crate::runtime::room) const fn connection(&self) -> ConnectionId {
        self.connection
    }

    pub(in crate::runtime::room) const fn transport_media(&self) -> TransportMediaId {
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

impl RoomState {
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

    pub(super) fn remove_consumer_key_state(&mut self, key: &ConsumerKey) {
        self.consumer_index.remove(key);
        self.pending_consumer_bootstraps.remove(key);
        self.consumer_source_selections.remove(key);
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
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
    ) -> Option<PublishedProducer> {
        let source = self.sources.remove(&source_id)?;
        let consumer_keys = self.consumer_keys_for_source(source_id);
        for key in consumer_keys {
            self.remove_consumer_key_state(&key);
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
        Some(producer)
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
