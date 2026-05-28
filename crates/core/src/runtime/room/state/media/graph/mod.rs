//! the graph is synchronous room state because callers hold the room state lock while
//! they ask for mutations or read views, collect returned effect inputs then run
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
        ActiveSpeakerGroup, ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
        SourceEncodingId, UserStreamId,
    },
};

mod consumer_index;
mod source_index;

use consumer_index::ConsumerIndex;
use source_index::SourceIndex;

#[cfg(test)]
mod test_support;

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
    sources: SourceIndex,
    consumers: ConsumerIndex,
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
/// activity updates, but all index maintenance stays behind the graph facade
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
/// callers resolve this immediately before a producer activity change or
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
        self.sources.publication_count()
    }

    /// current committed plus reserved subscription count
    ///
    /// pending bootstraps count because room orchestration has already accepted
    /// the subscription intent and reserved cleanup ownership for the consumer
    pub fn subscription_count(&self) -> usize {
        self.consumers.subscription_count()
    }

    /// number of committed producers for transport-backed test assertions
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn producer_count(&self) -> usize {
        self.sources.producer_count()
    }

    /// number of committed consumers for transport-backed test assertions
    #[cfg(any(test, feature = "testing-transport"))]
    pub fn consumer_count(&self) -> usize {
        self.consumers.consumer_count()
    }

    /// borrowed source inventory for diagnostics and policy input assembly
    ///
    /// callers must keep this as a read-only projection because source
    /// ownership and teardown stay behind graph mutation methods
    pub fn sources(&self) -> impl Iterator<Item = &PublishedSourceDescriptor> {
        self.sources.sources()
    }

    /// borrowed producer inventory for bootstrap planning
    ///
    /// this returns the graph-local producer id beside the producer so callers
    /// can snapshot the exact producer they will later revalidate during
    /// consumer bootstrap commit
    pub fn producers(&self) -> impl Iterator<Item = (ProducerRuntimeId, &PublishedProducer)> {
        self.sources.producers()
    }

    /// active producer owners grouped by stream for `/v1/stats`
    ///
    /// the query only exposes stream and owner identity because callers do not
    /// need source or transport ids to build compatibility user counts
    pub fn active_producer_stream_owners(&self) -> impl Iterator<Item = (&UserStreamId, &UserId)> {
        self.sources.active_producer_stream_owners()
    }

    /// resolves a source id to the current descriptor
    ///
    /// a missing source means the route or diagnostic snapshot is stale
    pub fn source(&self, source_id: PublishedSourceId) -> Option<&PublishedSourceDescriptor> {
        self.sources.source(source_id)
    }

    /// resolves transport media observations back to source ownership
    ///
    /// this is a cold-path lookup used by diagnostics and receiver policy
    /// refreshes after the transport layer reports media-scoped facts
    pub fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.sources
            .source_transport_media_entry(transport_media_id)
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
        self.sources.first_published_transport_media_id()
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
        self.sources
            .producer_transport_media_id(user_id, connection_id, stream_id)
    }

    /// resolves a publisher-owned stream to the canonical source id
    pub fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.sources
            .source_id_for_owner_stream(owner_user_id, stream_id)
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
        self.sources
            .producer_route_target(owner_user_id, owner_connection_id, stream_id)
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
        self.sources
            .producer_for_route_target(target, current_connection_id)
    }

    /// commits producer activity after router state accepted the same change
    ///
    /// the target must still match the current graph producer, a stale target
    /// returns `false` so outward fanout cannot get ahead of lasting media
    /// state
    pub fn set_producer_active(&mut self, target: &ProducerRouteTarget, active: bool) -> bool {
        self.sources.set_producer_active(target, active)
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
        self.sources
            .publications_for_user_connection(user_id, connection_id)
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
        self.sources
            .owner_has_promotable_source_in_group(owner_user_id, group)
    }

    /// atomically installs a published source and all producer-side indexes
    ///
    /// callers must already have a current room user, a routed producer and a
    /// transport media id, after this method returns, source lookup, producer
    /// lookup, owner teardown and transport-media diagnostics all see the same
    /// graph state
    pub fn install_source(&mut self, install: PublishedSourceInstall) {
        self.sources.install_source(install);
    }

    /// stores receiver intent for a source and keeps consumer reverse indexes live
    ///
    /// this can be called before a transport consumer exists, the selection is
    /// retained so a later publish, late join or recovery bootstrap can inherit
    /// the user's desired active state
    pub fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.consumers.set_source_selection(key, active);
    }

    /// reads the stored receiver selection for one source
    ///
    /// absence means no receiver-specific choice has been stored yet, callers
    /// may fall back to the effective subscription intent for that user
    pub fn consumer_source_selection(&self, key: &ConsumerKey) -> Option<ConsumerSourceSelection> {
        self.consumers.source_selection(key)
    }

    /// ensures a receiver selection exists for the bootstrap state
    ///
    /// selections without committed consumers can only come from stored
    /// subscription intent, so bootstrap planning may replace them with the
    /// computed initial policy state, committed routes keep their current
    /// selector and budget state until source policy updates them
    pub fn ensure_consumer_source_selection(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.consumers.ensure_source_selection(key, selection);
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
        update(self.consumers.selection_mut_or_open(key));
        true
    }

    /// reserves cleanup ownership for a consumer bootstrap in flight
    ///
    /// the route is not committed yet, but the graph records the consumer key
    /// so replacement and source teardown can release the pending work if the
    /// async transport step fails or becomes stale
    pub fn reserve_consumer_bootstrap(&mut self, key: ConsumerKey) {
        self.consumers.reserve_bootstrap(key);
    }

    /// removes a pending bootstrap reservation after transport work finishes
    ///
    /// the consumer key indexes are pruned only when no committed route or
    /// stored selection still references the key
    pub fn remove_pending_consumer_bootstrap(&mut self, key: &ConsumerKey) {
        self.consumers.remove_pending_bootstrap(key);
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
        self.consumers.commit(key, state, selection)
    }

    /// resolves a committed consumer route without exposing the backing map
    ///
    /// callers use this for tests and explicit cleanup planning where a copied
    /// route state is enough
    #[cfg(test)]
    pub fn consumer_state(&self, key: &ConsumerKey) -> Option<ConsumerState> {
        self.consumers.consumer_state(key)
    }

    /// committed consumer sessions as transport cleanup candidates
    ///
    /// room shutdown uses this shape because transport cleanup is scoped by
    /// user connection before specific media cleanup runs
    pub fn committed_consumer_transport_entries(
        &self,
    ) -> impl Iterator<Item = (UserId, ConnectionId)> + '_ {
        self.consumers.committed_consumer_transport_entries()
    }

    /// users with pending consumer bootstrap reservations
    ///
    /// this lets room transport cleanup include users whose consumer route is
    /// not yet committed but whose bootstrap already reserved graph ownership
    pub fn pending_consumer_user_ids(&self) -> impl Iterator<Item = &UserId> {
        self.consumers.pending_consumer_user_ids()
    }

    /// borrowed live route views for diagnostics and policy planning
    ///
    /// the iterator joins source, producer, selection and consumer state inside
    /// the graph so callers cannot accidentally observe mismatched indexes
    pub fn live_consumer_routes(&self) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.consumers
            .committed_entries()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, state))
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
        let source = self.sources.source(key.source_id)?;
        let producer = self.producer_for_source(key.source_id)?;
        Some(ConsumerRouteView {
            consumer_user_id: key.consumer_user_id.clone(),
            state,
            source,
            producer,
            selection: self.consumers.source_selection(key),
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
        let state = self.consumers.consumer_state(key)?;
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
        self.consumers
            .pending_keys_for_user(user_id)
            .filter_map(|key| {
                let source = self.sources.source(key.source_id)?;
                Some(PendingConsumerRouteView {
                    source,
                    producer: self.producer_for_source(key.source_id),
                    selection: self.consumers.source_selection(key),
                })
            })
    }

    /// resolves the current producer attached to a source
    ///
    /// source removal and stale async callbacks can make this lookup fail even
    /// while a caller still holds an older source id
    pub fn producer_for_source(&self, source_id: PublishedSourceId) -> Option<&PublishedProducer> {
        self.sources.producer_for_source(source_id)
    }

    /// resolves a graph-local producer id into the current producer state
    pub fn producer(&self, producer_id: ProducerRuntimeId) -> Option<&PublishedProducer> {
        self.sources.producer(producer_id)
    }

    /// removes all state attached to one consumer key
    ///
    /// this clears committed route state, pending bootstrap state, receiver
    /// selection state and reverse indexes before releasing matching relay
    /// ownership
    pub fn remove_consumer_key_state(&mut self, key: &ConsumerKey) -> Vec<RelayRouteEffect> {
        self.consumers.remove_key_state(key);
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
        for source_id in self.sources.source_ids_for_owner_snapshot(user_id) {
            if let Some((_producer, effects)) = self.remove_source(source_id) {
                relay_effects.extend(effects);
            }
        }
        for key in self.consumers.keys_for_user(user_id) {
            relay_effects.extend(self.remove_consumer_key_state(&key));
        }
        relay_effects
    }

    /// consumer keys owned by a receiver user
    ///
    /// the returned vector is detached from the graph so callers can remove
    /// keys while iterating without borrowing conflicts
    #[cfg(test)]
    pub fn consumer_keys_for_user(&self, user_id: &UserId) -> Vec<ConsumerKey> {
        self.consumers.keys_for_user(user_id)
    }

    /// consumer keys that point at one published source
    ///
    /// the returned vector gives teardown code a stable snapshot before it
    /// starts mutating the graph
    pub fn consumer_keys_for_source(&self, source_id: PublishedSourceId) -> Vec<ConsumerKey> {
        self.consumers.keys_for_source(source_id)
    }

    #[cfg(test)]
    pub fn producer_ids_for_user(&self, user_id: &UserId) -> Vec<ProducerRuntimeId> {
        self.sources.producer_ids_for_user(user_id)
    }

    /// routed consumer ids that receive from one published source
    ///
    /// source teardown passes this snapshot to topology before the media graph
    /// removes the source-owned consumer edges
    pub fn routed_consumer_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Vec<RoutedConsumerId> {
        self.consumers.routed_consumer_ids_for_source(source_id)
    }

    /// routed consumers affected when one room user leaves or is replaced
    ///
    /// the result includes consumers owned by the departing receiver plus
    /// consumers attached to sources owned by the departing publisher
    pub fn routed_consumer_ids_affected_by_user(&self, user_id: &UserId) -> Vec<RoutedConsumerId> {
        let mut consumer_ids = self
            .consumers
            .routed_consumer_ids_for_keys(self.consumer_keys_affected_by_user(user_id));
        consumer_ids.sort_unstable();
        consumer_ids.dedup();
        consumer_ids
    }

    fn consumer_keys_affected_by_user(&self, user_id: &UserId) -> BTreeSet<ConsumerKey> {
        self.consumers
            .affected_keys_for_user(user_id, self.sources.source_ids_for_owner(user_id))
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
        self.sources.source(source_id)?;
        let consumer_keys = self.consumer_keys_for_source(source_id);
        let mut relay_effects = Vec::new();
        for key in consumer_keys {
            relay_effects.extend(self.remove_consumer_key_state(&key));
        }
        let producer = self.sources.remove_source(source_id)?;
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
        let mut removals = self
            .sources
            .producer_transport_removals_for_users(departing_user_ids);
        removals.extend(self.consumer_transport_removals_for_users(departing_user_ids));
        removals
    }

    /// transport media removals needed before explicit source unpublish
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
            self.consumers
                .transport_removals_for_source(producer_target.source_id),
        );
        removals
    }

    /// consumer transport media affected by departing receivers or publishers
    ///
    /// publisher departures include consumers of the departed user's sources
    /// because those receiver-side transport consumers must be closed as well
    fn consumer_transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let keys = departing_user_ids
            .iter()
            .flat_map(|user_id| self.consumer_keys_affected_by_user(user_id))
            .collect::<BTreeSet<_>>();

        self.consumers.transport_removals_for_keys(keys)
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
        self.consumers.bootstrap_exists(consumer_key)
    }

    /// checks whether a consumer key has a committed route
    pub fn contains_consumer(&self, key: &ConsumerKey) -> bool {
        self.consumers.contains_consumer(key)
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
