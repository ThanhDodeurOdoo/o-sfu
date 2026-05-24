//! the graph is synchronous room state because callers hold the room state lock while
//! they ask for muattions or read views, collect returned effect inputs then run
//! transport work after the lock is released
//!
//! ownership sketch:
//!
//! ```text
//! owner user + stream -> source id -> source descriptor
//! source id -> producer id -> published producer
//! consumer user + source id -> selection / pending bootstrap / consumer state
//! consumer route -> relay route owners
//! ```
//!
//! query methods return borrowed views or small copied ids while mutation methods
//! update primary stores and reverse indexes together, so callers should not
//! rebuild joins across graph maps outside this module

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::{
    super::ids::ProducerRuntimeId,
    relay::{RelayRouteEffect, RoomRelayRoutes},
    subscription::PendingConsumerBootstrapTarget,
};
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::{RelayRouteActivity, TransportMediaId},
    room::topology::{RoutedConsumerId, RoutedProducerId},
    source_model::{
        ActiveSpeakerGroup, ActiveSpeakerSourceRole, ConsumerSourceSelection,
        PublishedSourceDescriptor, PublishedSourceId, SourceEncodingId, UserStreamId,
    },
};

/// media graph for one room
///
/// sources, producers, consumers, selections, pending bootstrap reservations
/// and relay owners form one lifecycle graph, keeping their primary stores and
/// reverse indexes behind this type lets user replacement, explicit unpublish
/// and disconnect cleanup remove all dependent media with one owner
///
/// all methods are cold-path room-state work because packet forwarding never calls
/// this graph directly
#[derive(Debug, Default)]
pub struct RoomMediaGraph {
    sources: BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
    source_ids_by_owner_stream: BTreeMap<SourceKey, PublishedSourceId>,
    source_ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    producer_id_by_source_id: BTreeMap<PublishedSourceId, ProducerRuntimeId>,
    producer_ids_by_owner: BTreeMap<UserId, BTreeSet<ProducerRuntimeId>>,
    producers: BTreeMap<ProducerRuntimeId, PublishedProducer>,
    source_transport_media_index: BTreeMap<TransportMediaId, SourceTransportMediaIndexEntry>,
    consumer_source_selections: BTreeMap<ConsumerKey, ConsumerSourceSelection>,
    consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    pending_consumer_bootstraps: BTreeSet<ConsumerKey>,
    consumer_keys_by_user: BTreeMap<UserId, BTreeSet<ConsumerKey>>,
    consumer_keys_by_source: BTreeMap<PublishedSourceId, BTreeSet<ConsumerKey>>,
    relay_routes: RoomRelayRoutes,
}

/// stable key for one receiver's relationship to one published source
///
/// the key is used before and after a consumer route is transport-backed, it
/// can point at stored receiver intent, a pending bootstrap reservation or a
/// committed consumer route
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumerKey {
    pub consumer_user_id: UserId,
    pub source_id: PublishedSourceId,
}

impl ConsumerKey {
    /// builds the graph key for one receiver and one source
    pub fn new(consumer_user_id: &UserId, source_id: PublishedSourceId) -> Self {
        Self {
            consumer_user_id: consumer_user_id.clone(),
            source_id,
        }
    }
}

/// source lookup key scoped by the publisher and the caller-provided stream id
///
/// this prevents producer workflows from depending on product stream labels
/// the graph stores the normalized source id once a publish commit succeeds
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceKey {
    owner_user_id: UserId,
    stream_id: UserStreamId,
}

/// committed producer realization for a published source
///
/// this is the graph-local join between the source model, room topology and
/// transport media ownership, workflow modules read it to plan bootstraps or
/// activity updates, but all index maintenance stays in [`RoomMediaGraph`]
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

/// all graph state needed to atomically commit a publish
///
/// callers construct this only after transport media exists and router producer
/// mirroring has succeeded, [`RoomMediaGraph::install_source`] consumes it so
/// the source descriptor, producer, owner indexes and transport-media index are
/// installed as one graph update
#[derive(Debug)]
pub struct PublishedSourceInstall {
    pub source_key: SourceKey,
    pub source_descriptor: PublishedSourceDescriptor,
    pub source_encoding_ids: Vec<SourceEncodingId>,
    pub producer_id: ProducerRuntimeId,
    pub producer: PublishedProducer,
    pub transport_media_id: TransportMediaId,
}

/// reverse lookup from transport media to source ownership
///
/// diagnostics, incoming bitrate aggregation and source-policy refreshes start
/// from transport observations, this entry lets those cold-path readers resolve
/// back to source identity without walking every producer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTransportMediaIndexEntry {
    source_id: PublishedSourceId,
    encoding_ids: Vec<SourceEncodingId>,
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    stream_id: UserStreamId,
}

#[allow(
    clippy::struct_field_names,
    reason = "postfix _id is intentional because the fields are all identity values"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
/// stale-callback guard for a live producer route
///
/// callers resolve this imediately before a producer activity change or
/// unpublish, later mutation methods compare the handle against current graph
/// ownership, so callbacks from replaced sockets become no-ops instead of
/// mutating a newer producer that reused the same user and stream
pub(in crate::runtime::room) struct ProducerRouteTarget {
    source_id: PublishedSourceId,
    producer_id: ProducerRuntimeId,
    owner_connection_id: ConnectionId,
    routed_producer_id: RoutedProducerId,
    transport_media_id: TransportMediaId,
}

/// transport media that must be cleaned after room state stops owning it
///
/// the graph returns these while it still has authoritative source and consumer
/// ownership, async cleanup can then target the exact user connection and
/// transport media id after the state lock has been released
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct TransportMediaRemoval {
    user: UserId,
    connection: ConnectionId,
    transport_media: TransportMediaId,
}

/// committed consumer route attached to transport media
///
/// the pure router id and transport ids are stored together because stale
/// packet-gate, keyframe and diagnostics updates must validate both topology
/// and transport ownership before changing source selection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerState {
    pub routed_consumer_id: RoutedConsumerId,
    pub consumer_connection_id: ConnectionId,
    pub source_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
    pub consumer_media: TransportMediaId,
}

/// borrowed view over a committed consumer route
///
/// read-side code uses this instead of joining source, producer, selection and
/// consumer maps itself, the view is only valid for the borrow of the graph
#[derive(Debug, Clone)]
pub struct ConsumerRouteView<'a> {
    pub consumer_user_id: UserId,
    pub state: ConsumerState,
    pub source: &'a PublishedSourceDescriptor,
    pub producer: &'a PublishedProducer,
    pub selection: Option<ConsumerSourceSelection>,
}

impl ConsumerRouteView<'_> {
    /// returns the stored selector or an open selector with the supplied active state
    pub fn selection_or_open(&self, active: bool) -> ConsumerSourceSelection {
        self.selection
            .unwrap_or_else(|| ConsumerSourceSelection::open(active))
    }

    /// copies the transport identity needed to revalidate async policy effects
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

    /// checks whether this borrowed route still matches an async transport handle
    pub fn matches_transport_ref(&self, route: &ConsumerRouteTransportRef) -> bool {
        self.consumer_user_id == *route.consumer_user_id()
            && self.state.consumer_connection_id == route.consumer_connection_id()
            && self.state.consumer_media == route.consumer_media()
            && self.source.owner().user_id() == route.source_user_id()
            && self.state.source_connection_id == route.source_connection_id()
            && self.state.source_media == route.source_media()
    }
}

/// borrowed view over a reserved consumer bootstrap
///
/// diagnostics need to show a pending subscription before transport has
/// returned a consumer media id, this view exposes the source and any still-live
/// producer without pretending the consumer route is committed
#[derive(Debug, Clone, Copy)]
pub struct PendingConsumerRouteView<'a> {
    pub source: &'a PublishedSourceDescriptor,
    pub producer: Option<&'a PublishedProducer>,
    pub selection: Option<ConsumerSourceSelection>,
}

/// transport identity for one committed consumer route
///
/// effect code carries this value across an async transport boundary then asks
/// the graph to revalidate it before committing packet-gate or policy state
/// it deliberately carries transport identity rather than diagnostics or fanout
/// payloads
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
    /// builds a route identity from both receiver and source transport sides
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

    /// receiver user that owns the consumer media
    pub fn consumer_user_id(&self) -> &UserId {
        &self.consumer_user_id
    }

    /// receiver connection that owns the consumer media
    pub const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    /// receiver-side transport media id for the consumer route
    pub const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_media
    }

    /// source user that owns the producer media
    pub fn source_user_id(&self) -> &UserId {
        &self.source_user_id
    }

    /// source connection that owns the producer media
    pub const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    /// source-side transport media id feeding this consumer route
    pub const fn source_media(&self) -> TransportMediaId {
        self.source_media
    }
}

impl SourceTransportMediaIndexEntry {
    /// creates the transport-media ownership record installed with a source
    ///
    /// callers should pass the complete encoding id list from the committed
    /// descriptor, diagnostics and test inspectors rely on this entry matching
    /// the source descriptor owned by the graph
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

    /// source owner resolved from the transport media id
    pub fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    /// published source resolved from the transport media id
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    /// source encoding ids mirrored into this transport-media lookup
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn encoding_ids(&self) -> &[SourceEncodingId] {
        &self.encoding_ids
    }

    /// source connection resolved from the transport media id
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

    /// caller stream id resolved from the transport media id
    pub const fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }
}

impl SourceKey {
    /// builds the owner-scoped stream lookup key used during publish replacement
    pub fn new(owner_user_id: &UserId, stream_id: &UserStreamId) -> Self {
        Self {
            owner_user_id: owner_user_id.clone(),
            stream_id: stream_id.clone(),
        }
    }
}

impl RoomMediaGraph {
    /// current committed source count
    ///
    /// this is the room publication count used for metrics snapshots and
    /// lifecycle deltas
    pub fn publication_count(&self) -> usize {
        self.sources.len()
    }

    /// current committed plus reserved subscription count
    ///
    /// pending bootstraps count because room orchestration has already accepted
    /// the subscription intent and reserved cleanup ownership for the consumer
    pub fn subscription_count(&self) -> usize {
        self.consumer_index
            .len()
            .saturating_add(self.pending_consumer_bootstraps.len())
    }

    /// number of committed producers for transport-backed test assertions
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn producer_count(&self) -> usize {
        self.producers.len()
    }

    /// number of committed consumers for transport-backed test assertions
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn consumer_count(&self) -> usize {
        self.consumer_index.len()
    }

    /// borrowed source inventory for diagnostics and policy input assembly
    ///
    /// callers must keep this as a read-only projection because source
    /// ownership and teardown stay behind graph mutation methods
    pub fn sources(&self) -> impl Iterator<Item = &PublishedSourceDescriptor> {
        self.sources.values()
    }

    /// borrowed producer inventory for bootstrap planning
    ///
    /// this returns the graph-local producer id beside the producer so callers
    /// can snapshot the exact producer they will later revalidate during
    /// consumer bootstrap commit
    pub fn producers(&self) -> impl Iterator<Item = (ProducerRuntimeId, &PublishedProducer)> {
        self.producers
            .iter()
            .map(|(producer_id, producer)| (*producer_id, producer))
    }

    /// active producer owners grouped by stream for `/v1/stats`
    ///
    /// the query only exposes stream and owner identity because callers do not
    /// need source or transport ids to build compatibility user counts
    pub fn active_producer_stream_owners(&self) -> impl Iterator<Item = (&UserStreamId, &UserId)> {
        self.producers
            .values()
            .filter(|producer| producer.active)
            .map(|producer| (&producer.stream_id, &producer.owner_user_id))
    }

    /// resolves a source id to the current descriptor
    ///
    /// a missing source means the route or diagnostic snapshot is stale
    pub fn source(&self, source_id: PublishedSourceId) -> Option<&PublishedSourceDescriptor> {
        self.sources.get(&source_id)
    }

    /// resolves transport media observations back to source ownership
    ///
    /// this is a cold-path lookup used by diagnostics and receiver policy
    /// refreshes after the transport layer reports media-scoped facts
    pub fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.source_transport_media_index.get(&transport_media_id)
    }

    /// returns the stream id associated with an incoming producer media handle
    ///
    /// this is the compatibility-facing lookup used when transport events are
    /// keyed by `TransportMediaId` but room outputs need caller stream ids
    pub fn producer_stream_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserStreamId> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.stream_id().clone())
    }

    /// first producer transport media id exposed for test transport probes
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.producers
            .values()
            .find_map(|producer| producer.transport_media_id)
    }

    /// producer transport media for one owner stream and connection
    ///
    /// a mismatched connection returns `None` so tests model the same stale
    /// callback guard as runtime code
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        let producer_id = self.producer_id_for_source_key(&SourceKey::new(user_id, stream_id))?;
        let producer = self.producers.get(&producer_id)?;
        (producer.owner_connection_id == connection_id).then_some(producer.transport_media_id?)
    }

    /// resolves a publisher-owned stream to the canonical source id
    pub fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.source_ids_by_owner_stream
            .get(&SourceKey::new(owner_user_id, stream_id))
            .copied()
    }

    /// checks whether a publisher-owned stream is currently published
    pub fn has_source_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.source_id_for_owner_stream(owner_user_id, stream_id)
            .is_some()
    }

    /// resolves one currently owned producer into a stale-safe mutation target
    ///
    /// callers should obtain the target immediately before changing producer
    /// activity or unpublishing, the target is later checked against the graph
    /// again before any mutation is accepted
    pub fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        let producer_id =
            self.producer_id_for_source_key(&SourceKey::new(owner_user_id, stream_id))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id != owner_connection_id {
            return None;
        }
        let transport_media_id = producer.transport_media_id?;
        Some(ProducerRouteTarget {
            source_id: producer.source_id,
            producer_id,
            owner_connection_id: producer.owner_connection_id,
            routed_producer_id: producer.routed_producer_id,
            transport_media_id,
        })
    }

    /// revalidates a producer target against current graph ownership
    ///
    /// this returns `None` when the user was replaced, the source was removed
    /// or the transport media id no longer belongs to the resolved producer
    pub fn producer_for_route_target(
        &self,
        target: &ProducerRouteTarget,
        current_connection_id: Option<ConnectionId>,
    ) -> Option<&PublishedProducer> {
        let producer = self.producers.get(&target.producer_id)?;
        if !target.matches_producer(producer)
            || Some(producer.owner_connection_id) != current_connection_id
        {
            return None;
        }
        Some(producer)
    }

    /// commits producer activity after router state accepted the same change
    ///
    /// the target must still match the current graph producer, a stale target
    /// returns `false` so outward fanout cannot get ahead of lasting media
    /// state
    pub fn set_producer_active(&mut self, target: &ProducerRouteTarget, active: bool) -> bool {
        let Some(producer) = self.producers.get_mut(&target.producer_id) else {
            return false;
        };
        if !target.matches_producer(producer) {
            return false;
        }
        producer.active = active;
        true
    }

    /// borrowed publications for one current user connection
    ///
    /// diagnostics use this to show only publications that still belong to the
    /// live connection being inspected
    pub fn publications_for_user_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> impl Iterator<Item = (&PublishedSourceDescriptor, &PublishedProducer)> {
        self.producers.values().filter_map(move |producer| {
            if producer.owner_user_id != *user_id || producer.owner_connection_id != connection_id {
                return None;
            }
            let source = self.sources.get(&producer.source_id)?;
            Some((source, producer))
        })
    }

    /// checks whether an active-speaker detector can promote the same owner
    ///
    /// transport reports detector media ids, room policy still needs to know
    /// whether that owner has a promotable source in the same active-speaker
    /// group before changing featured layout state
    pub fn owner_has_promotable_source_in_group(
        &self,
        owner_user_id: &UserId,
        group: ActiveSpeakerGroup,
    ) -> bool {
        self.source_ids_by_owner
            .get(owner_user_id)
            .into_iter()
            .flat_map(BTreeSet::iter)
            .filter_map(|source_id| self.sources.get(source_id))
            .any(|source| {
                source.policy().active_speaker().is_some_and(|policy| {
                    policy.group() == group && policy.role() == ActiveSpeakerSourceRole::Promotable
                })
            })
    }

    /// atomically installs a published source and all producer-side indexes
    ///
    /// callers must already have a current room user, a routed producer and a
    /// transport media id, after this method returns, source lookup, producer
    /// lookup, owner teardown and transport-media diagnostics all see the same
    /// graph state
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

    fn register_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        self.source_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(source_id);
    }

    fn unregister_source_owner(&mut self, user_id: &UserId, source_id: PublishedSourceId) {
        remove_from_index_set(&mut self.source_ids_by_owner, user_id, &source_id);
    }

    fn register_producer_owner(&mut self, user_id: &UserId, producer_id: ProducerRuntimeId) {
        self.producer_ids_by_owner
            .entry(user_id.clone())
            .or_default()
            .insert(producer_id);
    }

    fn unregister_producer_owner(&mut self, user_id: &UserId, producer_id: ProducerRuntimeId) {
        remove_from_index_set(&mut self.producer_ids_by_owner, user_id, &producer_id);
    }

    fn register_consumer_key(&mut self, key: &ConsumerKey) {
        self.consumer_keys_by_user
            .entry(key.consumer_user_id.clone())
            .or_default()
            .insert(key.clone());
        self.consumer_keys_by_source
            .entry(key.source_id)
            .or_default()
            .insert(key.clone());
    }

    /// stores receiver intent for a source and keeps consumer reverse indexes live
    ///
    /// this can be called before a transport consumer exists, the selection is
    /// retained so a later publish, late join or recovery bootstrap can inherit
    /// the user's desired active state
    pub fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.consumer_source_selections
            .entry(key.clone())
            .and_modify(|selection| selection.set_active(active))
            .or_insert_with(|| ConsumerSourceSelection::open(active));
        self.register_consumer_key(key);
    }

    /// reads the stored receiver selection for one source
    ///
    /// absence means no receiver-specific choice has been stored yet, callers
    /// may fall back to the effective subscription intent for that user
    pub fn consumer_source_selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.consumer_source_selections.get(key).copied()
    }

    /// ensures a receiver selection exists for the bootstrap state.
    ///
    /// selections without committed consumers can only come from stored
    /// subscription intent, so bootstrap planning may replace them with the
    /// computed initial policy state. Committed routes keep their current
    /// selector and budget state until source policy updates them.
    pub fn ensure_consumer_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        if self.consumer_index.contains_key(key) {
            self.consumer_source_selections
                .entry(key.clone())
                .or_insert(selection);
        } else {
            self.consumer_source_selections
                .insert(key.clone(), selection);
        }
        self.register_consumer_key(key);
    }

    /// mutates a consumer selection only when the transport route is still current
    ///
    /// async source-policy effects call this after transport packet gates have
    /// been applied, route identity is checked again so replaced users and
    /// removed sources cannot commit stale selectors
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
        let selection = self
            .consumer_source_selections
            .entry(key)
            .or_insert_with(|| ConsumerSourceSelection::open(true));
        update(selection);
        true
    }

    /// reserves cleanup ownership for a consumer bootstrap in flight
    ///
    /// the route is not committed yet, but the graph records the consumer key
    /// so replacement and source teardown can release the pending work if the
    /// async transport step fails or becomes stale
    pub fn reserve_consumer_bootstrap(&mut self, key: ConsumerKey) {
        self.register_consumer_key(&key);
        self.pending_consumer_bootstraps.insert(key);
    }

    /// removes a pending bootstrap reservation after transport work finishes
    ///
    /// the consumer key indexes are pruned only when no committed route or
    /// stored selection still references the key
    pub fn remove_pending_consumer_bootstrap(&mut self, key: &ConsumerKey) {
        self.pending_consumer_bootstraps.remove(key);
        self.prune_consumer_key_indexes_if_unused(key);
    }

    /// commits a transport-backed consumer route
    ///
    /// returns `false` if a route for the same consumer and source already
    /// exists, the caller remains responsible for cleaning up any transport
    /// media it created for a rejected commit
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

    /// resolves a committed consumer route without exposing the backing map
    ///
    /// callers use this for tests and explicit cleanup planning where a copied
    /// route state is enough
    pub fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.consumer_index.get(key).copied()
    }

    /// committed consumer sessions as transport cleanup candidates
    ///
    /// room shutdown uses this shape because transport cleanup is scoped by
    /// user connection before specific media cleanup runs
    pub fn committed_consumer_transport_entries(
        &self,
    ) -> impl Iterator<Item = (UserId, ConnectionId)> + '_ {
        self.consumer_index
            .iter()
            .map(|(key, state)| (key.consumer_user_id.clone(), state.consumer_connection_id))
    }

    /// users with pending consumer bootstrap reservations
    ///
    /// this lets room transport cleanup include users whose consumer route is
    /// not yet committed but whose bootstrap already reserved graph ownership
    pub fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.pending_consumer_bootstraps
            .iter()
            .map(|key| &key.consumer_user_id)
    }

    /// borrowed live route views for diagnostics and policy planning
    ///
    /// the iterator joins source, producer, selection and consumer state inside
    /// the graph so callers cannot accidentally observe mismatched indexes
    pub fn live_consumer_routes(&self) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.consumer_index
            .iter()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, *state))
    }

    /// joins one consumer key and route state into a borrowed route view
    ///
    /// this returns `None` when the source or producer side vanished before a
    /// stale consumer state could be pruned
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

    /// resolves one committed consumer key into its current route view
    ///
    /// callers use this before committing async receiver effects so the graph
    /// can reject keys whose route disappeared or whose producer side changed
    pub fn committed_consumer_route_for_key(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let state = *self.consumer_index.get(key)?;
        self.consumer_route_for_key(key, state)
    }

    /// pending consumer bootstraps for one user as diagnostic route views
    ///
    /// committed routes are skipped because they are exposed through
    /// [`RoomMediaGraph::live_consumer_routes`]
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

    /// resolves the current producer attached to a source
    ///
    /// source removal and stale async callbacks can make this lookup fail even
    /// while a caller still holds an older source id
    pub fn producer_for_source(&self, source_id: PublishedSourceId) -> Option<&PublishedProducer> {
        self.producer_id_by_source_id
            .get(&source_id)
            .and_then(|producer_id| self.producers.get(producer_id))
    }

    /// resolves a graph-local producer id into the current producer state
    pub fn producer(&self, producer_id: ProducerRuntimeId) -> Option<&PublishedProducer> {
        self.producers.get(&producer_id)
    }

    /// drops reverse consumer-key indexes when no owner state still uses the key
    ///
    /// callers invoke this after removing one optional state slot such as a
    /// pending bootstrap so later user or source teardown sees only live keys
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

    /// removes all state attached to one consumer key
    ///
    /// this clears committed route state, pending bootstrap state, receiver
    /// selection state and reverse indexes before releasing matching relay
    /// ownership
    pub fn remove_consumer_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        self.consumer_index.remove(key);
        self.pending_consumer_bootstraps.remove(key);
        self.consumer_source_selections.remove(key);
        remove_from_index_set(&mut self.consumer_keys_by_user, &key.consumer_user_id, key);
        remove_from_index_set(&mut self.consumer_keys_by_source, &key.source_id, key);
        self.relay_routes
            .release_consumer_key(&key.consumer_user_id, key.source_id)
    }

    /// removes every publisher and subscriber edge owned by one user
    ///
    /// user replacement and disconnect cleanup use this as their single graph
    /// entry point so producer routes, consumer routes, selections and relay
    /// ownership are removed together
    pub fn remove_user_media(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        let mut relay_effects = Vec::new();
        let source_ids = self
            .source_ids_by_owner
            .get(user_id)
            .cloned()
            .unwrap_or_default();
        for source_id in source_ids {
            if let Some((_producer, effects)) = self.remove_source(source_id) {
                relay_effects.extend(effects);
            }
        }
        let consumer_keys = self
            .consumer_keys_by_user
            .get(user_id)
            .cloned()
            .unwrap_or_default();
        for key in consumer_keys {
            relay_effects.extend(self.remove_consumer_key_state(&key));
        }
        relay_effects
    }

    /// consumer keys owned by a receiver user
    ///
    /// the returned vector is detached from the graph so callers can remove
    /// keys while iterating without borrowing conflicts
    pub fn consumer_keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_user
            .get(user_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// consumer keys that point at one published source
    ///
    /// the returned vector gives teardown code a stable snapshot before it
    /// starts mutating the graph
    pub fn consumer_keys_for_source(&self, source_id: PublishedSourceId) -> Vec<ConsumerKey> {
        self.consumer_keys_by_source
            .get(&source_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub fn producer_ids_for_user(&self, user_id: &UserId) -> Vec<ProducerRuntimeId> {
        self.producer_ids_by_owner
            .get(user_id)
            .map(|producer_ids| producer_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// routed consumer ids owned by a receiver user
    ///
    /// only committed routes are returned because pending bootstraps do not
    /// have router consumer ids yet
    pub fn routed_consumer_ids_for_user(&self, user_id: &UserId) -> Vec<RoutedConsumerId> {
        self.consumer_keys_for_user(user_id)
            .into_iter()
            .filter_map(|key| self.consumer_index.get(&key))
            .map(|consumer_state| consumer_state.routed_consumer_id)
            .collect()
    }

    /// routed consumer ids that receive from one published source
    ///
    /// source teardown passes this snapshot to topology before the media graph
    /// removes the source-owned consumer edges
    pub fn routed_consumer_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<RoutedConsumerId> {
        self.consumer_keys_for_source(source_id)
            .into_iter()
            .filter_map(|key| self.consumer_index.get(&key))
            .map(|consumer_state| consumer_state.routed_consumer_id)
            .collect()
    }

    /// routed consumers affected when one room user leaves or is replaced
    ///
    /// the result includes consumers owned by the departing receiver plus
    /// consumers attached to sources owned by the departing publisher
    pub fn routed_consumer_ids_affected_by_user(&self, user_id: &UserId) -> Vec<RoutedConsumerId> {
        let mut consumer_ids = self.routed_consumer_ids_for_user(user_id);
        if let Some(source_ids) = self.source_ids_by_owner.get(user_id) {
            for source_id in source_ids {
                consumer_ids.extend(self.routed_consumer_ids_for_source(*source_id));
            }
        }
        consumer_ids.sort_unstable();
        consumer_ids.dedup();
        consumer_ids
    }

    fn producer_id_for_source_key(&self, source_key: &SourceKey) -> Option<ProducerRuntimeId> {
        let source_id = self.source_ids_by_owner_stream.get(source_key)?;
        self.producer_id_by_source_id.get(source_id).copied()
    }

    /// removes one published source and every graph edge depending on it
    ///
    /// the returned producer tells callers which transport producer to close
    /// after the room lock is released, while relay effects describe any relay
    /// route ownership that must be reconciled outside the graph
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

    /// transport media removals needed before a set of users leaves the room
    ///
    /// producer media owned by departing users and consumer media that either
    /// belongs to them or receives from them are returned as one cleanup batch
    pub fn transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = self.producer_transport_removals_for_users(departing_user_ids);
        removals.extend(self.consumer_transport_removals_for_users(departing_user_ids));
        removals
    }

    /// producer transport media owned by departing users
    fn producer_transport_removals_for_users(
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

    /// consumer transport media affected by departing receivers or publishers
    ///
    /// publisher departures include consumers of the departed user's sources
    /// because those receiver-side transport consumers must be closed as well
    fn consumer_transport_removals_for_users(
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

    /// reserves a relay consumer target while bootstrap transport work is in flight
    ///
    /// relay ownership lives in the media graph so source teardown and receiver
    /// teardown can release the same target even if bootstrap has not committed
    pub fn reserve_relay_consumer(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        source_connection_id: ConnectionId,
        source_transport_media_id: TransportMediaId,
        target_media_worker_id: usize,
        active: bool,
    ) -> Vec<RelayRouteEffect> {
        self.relay_routes.reserve_consumer(
            target,
            source_connection_id,
            source_transport_media_id,
            target_media_worker_id,
            active,
        )
    }

    /// updates relay activity for a committed consumer route
    ///
    /// this forwards the graph-validated route identity into the relay table and
    /// returns the external effects required to open or close relay consumers
    pub fn set_relay_consumer_active(
        &mut self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
        source_id: PublishedSourceId,
        activity: RelayRouteActivity,
    ) -> Vec<RelayRouteEffect> {
        self.relay_routes.set_consumer_active(
            consumer_user_id,
            consumer_connection_id,
            source_id,
            activity,
        )
    }

    /// releases a relay reservation that never became a committed consumer
    pub fn release_pending_relay_target(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Vec<RelayRouteEffect> {
        self.relay_routes.release_target(target)
    }

    /// checks whether a consumer key already has committed or pending bootstrap state
    ///
    /// subscription bootstrap uses this to avoid creating duplicate transport
    /// work for the same receiver and source
    pub fn consumer_bootstrap_exists(&self, consumer_key: &ConsumerKey) -> bool {
        self.consumer_index.contains_key(consumer_key)
            || self.pending_consumer_bootstraps.contains(consumer_key)
    }

    /// test helper for source primary-store membership
    #[cfg(test)]
    pub fn contains_source(&self, source_id: PublishedSourceId) -> bool {
        self.sources.contains_key(&source_id)
    }

    /// checks whether a consumer key has a committed route
    pub fn contains_consumer(&self, key: &ConsumerKey) -> bool {
        self.consumer_index.contains_key(key)
    }

    /// test helper for pending bootstrap membership
    #[cfg(test)]
    pub fn contains_pending_consumer_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.pending_consumer_bootstraps.contains(key)
    }

    /// test helper for stored receiver selection membership
    #[cfg(test)]
    pub fn contains_consumer_source_selection(&self, key: &ConsumerKey) -> bool {
        self.consumer_source_selections.contains_key(key)
    }

    /// test helper proving all source and producer stores are empty
    #[cfg(test)]
    pub fn source_indexes_are_empty(&self) -> bool {
        self.sources.is_empty()
            && self.source_ids_by_owner_stream.is_empty()
            && self.source_ids_by_owner.is_empty()
            && self.producer_id_by_source_id.is_empty()
            && self.producer_ids_by_owner.is_empty()
            && self.producers.is_empty()
    }

    /// test helper proving one publisher has no source owner index entry
    #[cfg(test)]
    pub fn owner_source_index_is_empty(&self, user_id: &UserId) -> bool {
        !self.source_ids_by_owner.contains_key(user_id)
    }

    /// test helper proving one publisher has no producer owner index entry
    #[cfg(test)]
    pub fn owner_producer_index_is_empty(&self, user_id: &UserId) -> bool {
        !self.producer_ids_by_owner.contains_key(user_id)
    }
}

impl ProducerRouteTarget {
    /// source guarded by this stale-callback target
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    /// routed producer guarded by this stale-callback target
    pub const fn routed_producer_id(&self) -> RoutedProducerId {
        self.routed_producer_id
    }

    /// transport media guarded by this stale-callback target
    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    /// publisher connection guarded by this stale-callback target
    pub const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    /// verifies that a current producer still matches the captured target
    fn matches_producer(&self, producer: &PublishedProducer) -> bool {
        producer.source_id == self.source_id
            && producer.owner_connection_id == self.owner_connection_id
            && producer.routed_producer_id == self.routed_producer_id
            && producer.transport_media_id == Some(self.transport_media_id)
    }
}

impl TransportMediaRemoval {
    /// builds a transport media cleanup item for work after the room lock drops
    pub fn new(user: UserId, connection: ConnectionId, transport_media: TransportMediaId) -> Self {
        Self {
            user,
            connection,
            transport_media,
        }
    }

    /// user that owns the transport media to remove
    pub fn user(&self) -> &UserId {
        &self.user
    }

    /// connection that owns the transport media to remove
    pub const fn connection(&self) -> ConnectionId {
        self.connection
    }

    /// transport media id that should be removed
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
