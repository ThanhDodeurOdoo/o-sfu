use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{
    Router, RouterError,
    rtp::{MediaCapabilities, MediaStream as RouterRtpParameters},
};
use tracing::{error, warn};

use super::{
    CommittedConsumerSetup, ConsumerId, ConsumerKey, ConsumerRouteTarget,
    ConsumerRouteTransportRef, ConsumerRouteView, ConsumerSetupTarget, DeclaredConsumerSetup,
    PendingConsumerRouteView, PendingConsumerSetup, ProducerId, ProducerRouteTarget,
    PublishedProducer, PublishedSourceInstall, ReceiverRouteActivity,
    SourceTransportMediaIndexEntry, SourceView, TransportMediaRemoval, ValidatedPublish,
    route_graph::{RelayRouteEffect, RouteGraph},
    source_index::SourceIndex,
};
use crate::{
    RoomSpilloverMode,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::{
            RelayRouteActivity, TransportConsumerRoute, TransportMediaId,
            TransportRelayRouteEffect, TransportSessionKey, TransportSourceKey,
        },
        room::{
            RoomMediaCounts, RoomRuntimeContext, RouterPlacement,
            cleanup::TransportCleanupOperation,
            effects::transport::RoomTransportPlan,
            outbound::OutboundSender,
            placement::{LoadTriggeredPlacementState, WorkerLoadIndex},
        },
        source_model::{
            ActiveSpeakerSourceRole, ConsumerSourceSelection, PublishedSourceDescriptor,
            PublishedSourceId, UserStreamId,
        },
    },
};

#[cfg(test)]
#[path = "TESTS/topology_support.rs"]
mod topology_support;

#[derive(Debug)]
pub struct RoomTopology {
    instance: RoomInstanceId,
    sources: SourceIndex,
    route_graph: RouteGraph,
    router: Router,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTransportReceipt {
    pub connection_id: ConnectionId,
    pub transport_session_key: TransportSessionKey,
}

#[derive(Debug)]
pub struct SessionPlacementCommit {
    pub receipt: CommittedTransportReceipt,
    pub replacement_transport_plan: RoomTransportPlan,
}

#[derive(Debug)]
pub(super) struct ConsumerActivityCommit {
    pub(super) update: Option<ReceiverRouteActivity>,
    pub(super) relay_effects: Vec<TransportRelayRouteEffect>,
}

#[derive(Debug)]
pub enum SessionPlacementRejection {
    MissingPreviousSession { previous_connection: ConnectionId },
    Router(RouterError),
}

impl RoomTopology {
    pub fn new(
        runtime_context: &RoomRuntimeContext,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        let router = match runtime_context.initial_router_placements() {
            Some(placements) => {
                Router::with_placements(placements.clone(), router_rtp_capabilities)
            }
            None => Router::new(runtime_context.primary_router(), router_rtp_capabilities),
        };
        Self {
            instance: runtime_context.instance(),
            sources: SourceIndex::default(),
            route_graph: RouteGraph::default(),
            router,
        }
    }

    pub(in crate::engine::room) fn router(&self) -> &Router {
        &self.router
    }

    #[must_use]
    pub fn committed_transport_user_key(
        &self,
        user_id: impl Into<Arc<UserId>>,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        let user_id = user_id.into();
        let worker = self
            .router
            .committed_media_worker_id(user_id.as_ref(), connection_id)?;
        Some(self.transport_session_key(user_id, connection_id, worker))
    }

    /// requires committed placement for `user_id` and `connection_id`
    /// use [`Self::committed_transport_user_key`] for stale callbacks or teardown races
    ///
    /// # Panics
    ///
    /// panics when no committed router placement exists
    #[must_use]
    #[expect(
        clippy::unreachable,
        reason = "current room operations require committed connection placement and must not synthesize a transport worker"
    )]
    pub fn transport_user_key(
        &self,
        user_id: impl Into<Arc<UserId>>,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        let user_id = user_id.into();
        let Some(worker) = self
            .router
            .committed_media_worker_id(user_id.as_ref(), connection_id)
        else {
            unreachable!("transport session key lookup requires committed connection placement");
        };
        self.transport_session_key(user_id, connection_id, worker)
    }

    fn transport_session_key(
        &self,
        user_id: Arc<UserId>,
        connection_id: ConnectionId,
        media_worker_id: MediaWorkerId,
    ) -> TransportSessionKey {
        TransportSessionKey::new(self.instance, media_worker_id, connection_id, user_id)
    }

    pub fn retire_committed_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        let media_worker = self
            .router
            .retire_committed_placement(user_id, connection_id)?;
        Some(self.transport_session_key(user_id.clone().into(), connection_id, media_worker))
    }

    pub fn reconcile_spillover_routers(
        &mut self,
        spillover: RoomSpilloverMode,
        placement: &mut LoadTriggeredPlacementState,
    ) {
        match spillover {
            RoomSpilloverMode::StrictSingleRouter => {}
            RoomSpilloverMode::BoundedLocalSpillover => {
                let idle_router_ids = self.router.idle_spillover_routers();
                self.router.detach_spillover_routers(&idle_router_ids);
                placement.clear_cooldowns(&idle_router_ids);
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let idle_router_ids = self.router.idle_spillover_routers();
                let policy = policy.parts();
                let detachments =
                    placement.cooldown_detachments(&idle_router_ids, policy.cooldown_window);
                self.router.detach_spillover_routers(&detachments);
            }
        }
    }

    #[must_use]
    pub fn media_counts(&self) -> RoomMediaCounts {
        RoomMediaCounts {
            publications: self.sources.publication_count(),
            subscriptions: self.route_graph.subscription_count(),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn consumer_count(&self) -> usize {
        self.route_graph.count()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn first_published_transport_media_id(
        &self,
    ) -> Option<TransportMediaId> {
        self.sources.first_published_transport_media_id()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        self.sources
            .producer_transport_media_id(user_id, connection_id, stream_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.sources.source(source_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.sources.transport_media_entry(transport_media_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.sources.id_for_owner_stream(owner_user_id, stream_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        self.sources
            .producer_route_target(owner_user_id, owner_connection_id, stream_id)
    }

    pub(in crate::engine::room) fn source_views(&self) -> impl Iterator<Item = SourceView<'_>> {
        self.sources.source_views()
    }

    pub(in crate::engine::room) fn active_stream_user_counts(&self) -> BTreeMap<UserStreamId, u64> {
        let mut users_by_stream: BTreeMap<UserStreamId, BTreeSet<UserId>> = BTreeMap::new();
        for view in self
            .sources
            .source_views()
            .filter(|view| view.producer.active)
        {
            users_by_stream
                .entry(view.producer.stream_id.clone())
                .or_default()
                .insert(view.producer.owner_user_id.clone());
        }
        users_by_stream
            .into_iter()
            .map(|(stream_id, users)| (stream_id, u64::try_from(users.len()).unwrap_or(u64::MAX)))
            .collect()
    }

    #[must_use]
    pub(in crate::engine::room) fn active_speaker_detector_owner(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserId> {
        let entry = self.source_transport_media_entry(transport_media_id)?;
        let detector_source = self.source(entry.source)?;
        let detector_policy = detector_source.policy().active_speaker()?;
        if detector_policy.role() != ActiveSpeakerSourceRole::Detector {
            return None;
        }
        self.sources
            .owner_has_promotable_source_in_group(&entry.owner, detector_policy.group())
            .then(|| entry.owner.clone())
    }

    pub(in crate::engine::room) fn live_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.route_graph
            .committed_entries()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, state))
    }

    pub(in crate::engine::room) fn committed_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.route_graph
            .committed_entries_for_user(user_id)
            .filter_map(|(key, state)| self.consumer_route_for_key(key, state))
    }

    pub(in crate::engine::room) fn pending_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PendingConsumerRouteView<'_>> {
        self.route_graph
            .pending_keys_for_user(user_id)
            .filter_map(|key| {
                let view = self.sources.source_view(key.source_id)?;
                Some(PendingConsumerRouteView {
                    source: view.source,
                    producer: view.producer,
                    selection: self.route_graph.selection(key),
                })
            })
    }

    #[must_use]
    pub(in crate::engine::room) fn consumer_source_selection(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerSourceSelection> {
        self.route_graph.selection(key)
    }

    pub(in crate::engine::room) fn committed_consumer_user_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> BTreeSet<UserId> {
        self.route_graph
            .keys_for_source(source_id)
            .filter(|key| self.route_graph.consumer_state(key).is_some())
            .map(|key| key.consumer_user_id.clone())
            .collect()
    }

    pub(in crate::engine::room) fn committed_consumer_user_ids_for_owner_sources(
        &self,
        user_id: &UserId,
    ) -> BTreeSet<UserId> {
        self.sources
            .ids_for_owner(user_id)
            .flat_map(|source_id| self.route_graph.keys_for_source(source_id))
            .filter(|key| self.route_graph.consumer_state(key).is_some())
            .map(|key| key.consumer_user_id.clone())
            .collect()
    }

    #[must_use]
    pub(in crate::engine::room) fn committed_consumer_route_for_key(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let state = self.route_graph.consumer_state(key)?;
        self.consumer_route_for_key(key, state)
    }

    #[must_use]
    pub(in crate::engine::room) fn has_consumer_setup_or_route(&self, key: &ConsumerKey) -> bool {
        self.route_graph.has_consumer_setup_or_route(key)
    }

    #[must_use]
    pub(in crate::engine::room) fn producer(
        &self,
        producer_id: ProducerId,
    ) -> Option<&PublishedProducer> {
        self.sources.producer(producer_id)
    }

    pub(super) fn missing_consumer_targets_for_producer<'a>(
        &self,
        producer_id: ProducerId,
        receivers: impl IntoIterator<Item = (&'a UserId, ConnectionId)>,
    ) -> Vec<ConsumerSetupTarget> {
        let Some(producer) = self.producer(producer_id) else {
            return Vec::new();
        };
        let Some(media) = producer.transport_media_id else {
            return Vec::new();
        };
        let Some(producer_session) = self.committed_transport_user_key(
            producer.owner_user_id.clone(),
            producer.owner_connection_id,
        ) else {
            return Vec::new();
        };
        let source = TransportSourceKey::new(producer_session, media);
        receivers
            .into_iter()
            .filter_map(|(user, connection)| {
                self.consumer_target(user, connection, producer, source.clone())
            })
            .collect()
    }

    fn consumer_target(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        producer: &PublishedProducer,
        source: TransportSourceKey,
    ) -> Option<ConsumerSetupTarget> {
        if producer.owner_user_id == *user_id {
            return None;
        }
        let key = ConsumerKey::new(user_id, producer.source_id);
        if self.has_consumer_setup_or_route(&key) {
            return None;
        }
        let consumer_session = self.committed_transport_user_key(user_id.clone(), connection_id)?;
        Some(ConsumerSetupTarget::new(consumer_session, source, producer))
    }

    pub(super) fn missing_consumer_targets(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        include_producer: impl Fn(&PublishedProducer) -> bool,
    ) -> Vec<ConsumerSetupTarget> {
        self.sources
            .producers()
            .filter_map(|producer| {
                if !include_producer(producer) {
                    return None;
                }
                let media = producer.transport_media_id?;
                let session = self.committed_transport_user_key(
                    producer.owner_user_id.clone(),
                    producer.owner_connection_id,
                )?;
                let source = TransportSourceKey::new(session, media);
                self.consumer_target(user_id, connection_id, producer, source)
            })
            .collect()
    }

    pub(in crate::engine::room) fn record_consumer_loads(
        &self,
        loads: &mut WorkerLoadIndex,
        current_connection_id: impl Fn(&UserId) -> Option<ConnectionId>,
    ) {
        for connection_id in self.route_graph.committed_consumer_connection_ids() {
            loads.record_consumer(self.router.media_worker_id_for_connection(connection_id));
        }
        for user_id in self.route_graph.pending_consumer_user_ids() {
            let Some(connection_id) = current_connection_id(user_id) else {
                continue;
            };
            loads.record_consumer(self.router.media_worker_id_for_connection(connection_id));
        }
    }

    #[must_use]
    pub(in crate::engine::room) fn source_fanout_pressure(
        &self,
        max_fanout_per_source: usize,
        current_connection_id: impl Fn(&UserId) -> Option<ConnectionId>,
    ) -> bool {
        if max_fanout_per_source == 0 {
            return false;
        }
        self.sources.source_views().any(|view| {
            if !view.producer.active {
                return false;
            }
            let mut deliveries_by_worker = BTreeMap::new();
            for key in self.route_graph.keys_for_source(view.source.source_id()) {
                if !self.route_graph.has_consumer_setup_or_route(key) {
                    continue;
                }
                if self
                    .route_graph
                    .selection(key)
                    .is_some_and(|selection| !selection.delivery_active())
                {
                    continue;
                }
                let Some(connection_id) = current_connection_id(&key.consumer_user_id) else {
                    continue;
                };
                let Some(media_worker) = self
                    .router
                    .committed_media_worker_id(&key.consumer_user_id, connection_id)
                else {
                    continue;
                };
                deliveries_by_worker
                    .entry(media_worker)
                    .and_modify(|count: &mut usize| *count = count.saturating_add(1))
                    .or_insert(1);
            }
            !deliveries_by_worker.is_empty()
                && deliveries_by_worker
                    .values()
                    .all(|count| *count >= max_fanout_per_source)
        })
    }

    pub fn update_consumer_source_selection(
        &mut self,
        route: &ConsumerRouteTransportRef,
        source_id: PublishedSourceId,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        let key = ConsumerKey::new(&route.consumer_user_id, source_id);
        let Some(current_route) = self.committed_consumer_route_for_key(&key) else {
            return false;
        };
        if !current_route.matches_transport_ref(route) {
            return false;
        }
        update(self.route_graph.selection_mut_or_open(key));
        true
    }

    fn remove_user_media(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        let mut relay_effects = Vec::new();
        let source_ids = self.sources.ids_for_owner(user_id).collect::<Vec<_>>();
        for source_id in source_ids {
            if let Some(effects) = self.remove_source(source_id) {
                relay_effects.extend(effects);
            }
        }
        for key in self.route_graph.keys_for_user(user_id) {
            relay_effects.extend(self.route_graph.remove_key_state(&key));
        }
        relay_effects
    }

    fn remove_source(&mut self, source_id: PublishedSourceId) -> Option<Vec<RelayRouteEffect>> {
        self.sources.source(source_id)?;
        let consumer_keys = self
            .route_graph
            .keys_for_source(source_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut relay_effects = Vec::new();
        for key in consumer_keys {
            relay_effects.extend(self.route_graph.remove_key_state(&key));
        }
        self.sources.remove_source(source_id)?;
        Some(relay_effects)
    }

    fn transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = self
            .sources
            .producer_transport_removals_for_users(departing_user_ids);
        removals.extend(self.consumer_transport_removals_for_users(departing_user_ids));
        removals
    }

    fn transport_removals_for_user(&self, user_id: &UserId) -> Vec<TransportMediaRemoval> {
        self.transport_removals_for_users(&BTreeSet::from([user_id.clone()]))
    }

    fn transport_removals_for_producer_target(
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
            self.route_graph
                .transport_removals_for_source(producer_target.source_id),
        );
        removals
    }

    fn consumer_transport_removals_for_users(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let affected_consumer_keys = departing_user_ids
            .iter()
            .flat_map(|user_id| {
                self.route_graph
                    .affected_keys_for_user(user_id, self.sources.ids_for_owner(user_id))
            })
            .collect::<BTreeSet<_>>();

        self.route_graph
            .transport_removals_for_keys(affected_consumer_keys)
    }

    fn consumer_route_for_key<'a>(
        &'a self,
        key: &ConsumerKey,
        state: &'a super::ConsumerState,
    ) -> Option<ConsumerRouteView<'a>> {
        let view = self.sources.source_view(key.source_id)?;
        Some(ConsumerRouteView {
            consumer_user_id: key.consumer_user_id.clone(),
            state,
            source: view.source,
            producer: view.producer,
            selection: self.route_graph.selection(key),
        })
    }

    pub fn publish_source(
        &mut self,
        publish: ValidatedPublish,
        producer_id: ProducerId,
        source_descriptor: PublishedSourceDescriptor,
        consumable_rtp_parameters: RouterRtpParameters,
        transport_media_id: TransportMediaId,
    ) -> Result<(), RouterError> {
        let routed_producer_id = self
            .router
            .add_producer(&publish.owner_user_id, producer_id)?;
        let source_id = source_descriptor.source_id();
        let producer = PublishedProducer {
            source_id,
            owner_user_id: publish.owner_user_id,
            owner_connection_id: publish.owner_connection_id,
            stream_id: publish.stream_id,
            media_kind: publish.media_kind,
            consumable_rtp_parameters,
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            active: true,
        };
        self.sources.install_source(PublishedSourceInstall {
            source_descriptor,
            producer,
            transport_media_id,
        });
        Ok(())
    }

    pub(in crate::engine::room) fn consumer_route_target_for_source(
        &self,
        route: &ConsumerRouteTransportRef,
        source: &PublishedSourceDescriptor,
    ) -> ConsumerRouteTarget {
        let transport_route = TransportConsumerRoute::new(
            self.transport_user_key(route.consumer_user_id.clone(), route.consumer_connection_id),
            route.consumer_media,
            TransportSourceKey::new(
                self.transport_user_key(route.source_user_id.clone(), route.source_connection_id),
                route.source_media,
            ),
        );
        ConsumerRouteTarget::new(
            transport_route,
            source.stream_id().clone(),
            source.media_kind(),
        )
    }

    fn transport_cleanup_operations(
        &self,
        removals: impl IntoIterator<Item = TransportMediaRemoval>,
    ) -> Vec<TransportCleanupOperation> {
        removals
            .into_iter()
            .map(|removal| TransportCleanupOperation::RemoveMedia {
                session_key: self.transport_user_key(removal.user, removal.connection),
                transport_media_id: removal.transport_media,
            })
            .collect()
    }

    fn resolve_relay_effects(
        &self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
    ) -> Vec<TransportRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| {
                let route = effect.route;
                TransportRelayRouteEffect {
                    source: TransportSourceKey::new(
                        self.transport_user_key(route.source_user, route.source_connection),
                        route.source_media,
                    ),
                    target_media_worker_id: route.target_worker,
                    action: effect.action,
                }
            })
            .collect()
    }

    fn resolve_relay_effects_with_displaced(
        &self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
        user_id: &UserId,
        session_key: &TransportSessionKey,
    ) -> Vec<TransportRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| {
                let route = effect.route;
                let source_session_key = if route.source_user == *user_id
                    && route.source_connection == session_key.connection_id()
                {
                    session_key.clone()
                } else {
                    self.transport_user_key(route.source_user, route.source_connection)
                };
                TransportRelayRouteEffect {
                    source: TransportSourceKey::new(source_session_key, route.source_media),
                    target_media_worker_id: route.target_worker,
                    action: effect.action,
                }
            })
            .collect()
    }

    pub(super) fn unpublish_source(
        &mut self,
        user_id: &UserId,
        target: &ProducerRouteTarget,
    ) -> Option<RoomTransportPlan> {
        let transport_removals = self.transport_removals_for_producer_target(user_id, target);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        if let Some(error) = self.router.remove_producer(target.routed_producer_id).err() {
            error!(
                ?user_id,
                ?target,
                ?error,
                "repaired published track room state after router producer teardown failed"
            );
        }
        let relay_effects = self.remove_source(target.source_id)?;
        let relay_effects = self.resolve_relay_effects(relay_effects);
        Some(RoomTransportPlan::from_relays_and_cleanup(
            relay_effects,
            transport_cleanup,
        ))
    }

    pub fn set_published_source_activity(
        &mut self,
        target: &ProducerRouteTarget,
        current_connection_id: Option<ConnectionId>,
        active: bool,
    ) -> bool {
        let Some(producer) = self
            .sources
            .producer_for_route_target(target, current_connection_id)
        else {
            return false;
        };
        if producer.active == active {
            return false;
        }
        self.sources.set_producer_active(target, active);
        true
    }

    /// commits the new connection before returning cleanup for displaced placement
    ///
    /// `previous_connection` must name the currently committed session for replacement joins
    pub fn commit_session_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        previous_connection: Option<ConnectionId>,
        home_placement: RouterPlacement,
    ) -> Result<SessionPlacementCommit, SessionPlacementRejection> {
        let previous_session_key = if let Some(previous_connection) = previous_connection {
            let Some(key) = self.committed_transport_user_key(user_id.clone(), previous_connection)
            else {
                return Err(SessionPlacementRejection::MissingPreviousSession {
                    previous_connection,
                });
            };
            Some(key)
        } else {
            None
        };
        let media_worker = self
            .router
            .commit_session_placement(user_id, connection_id, home_placement)
            .map_err(SessionPlacementRejection::Router)?;
        let session_key =
            self.transport_session_key(user_id.clone().into(), connection_id, media_worker);
        let receipt = CommittedTransportReceipt {
            connection_id,
            transport_session_key: session_key,
        };
        let replacement_transport_plan = previous_session_key.as_ref().map_or_else(
            RoomTransportPlan::default,
            |replaced_session_key| {
                let transport_cleanup = vec![TransportCleanupOperation::CloseUser {
                    session_key: replaced_session_key.clone(),
                }];
                let relay_effects = self.remove_user_media(user_id);
                let relay_effects = self.resolve_relay_effects_with_displaced(
                    relay_effects,
                    user_id,
                    replaced_session_key,
                );
                RoomTransportPlan::from_relays_and_cleanup(relay_effects, transport_cleanup)
            },
        );
        Ok(SessionPlacementCommit {
            receipt,
            replacement_transport_plan,
        })
    }

    pub fn remove_session(&mut self, user_id: &UserId) -> RoomTransportPlan {
        let transport_removals = self.transport_removals_for_user(user_id);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        let relay_effects = self.remove_user_media(user_id);
        let relay_effects = self.resolve_relay_effects(relay_effects);
        if let Some(error) = self.router.remove_session(user_id).err() {
            error!(?user_id, ?error, "failed to remove user from room router");
        }
        RoomTransportPlan::from_relays_and_cleanup(relay_effects, transport_cleanup)
    }

    pub(super) fn commit_consumer_setup(
        &mut self,
        setup: DeclaredConsumerSetup,
        selection: ConsumerSourceSelection,
    ) -> Result<CommittedConsumerSetup, (TransportConsumerRoute, Vec<TransportRelayRouteEffect>)>
    {
        let DeclaredConsumerSetup {
            pending:
                PendingConsumerSetup {
                    target,
                    consumer,
                    reservation,
                    sender,
                    rtp,
                    relays: _,
                },
            route,
            mid,
        } = setup;
        let active = selection.delivery_active();
        let declared_active = reservation.selection().delivery_active();
        let committed_mid = mid.unwrap_or_else(|| {
            rtp.mid()
                .map_or_else(|| consumer.to_string(), ToOwned::to_owned)
        });
        let state = target.consumer_state(route.consumer_transport_media_id(), committed_mid);
        let (route_graph, router) = (&mut self.route_graph, &mut self.router);
        let result = route_graph.commit(reservation, state, selection, || {
            match router.add_consumer(target.session.user_id(), consumer, target.routed) {
                Ok(_) => true,
                Err(error) => {
                    warn!(
                        consumer_user_id = ?target.session.user_id(),
                        source_id = ?target.source_id,
                        ?error,
                        "router rejected consumer creation"
                    );
                    false
                }
            }
        });
        if let Err(relays) = result {
            return Err((route, self.resolve_relay_effects(relays)));
        }
        Ok(CommittedConsumerSetup {
            target,
            route,
            sender,
            transport_activity_update: (active != declared_active).then_some(active),
        })
    }

    pub(super) fn reserve_consumer_setup(
        &mut self,
        target: ConsumerSetupTarget,
        consumer: ConsumerId,
        selection: ConsumerSourceSelection,
        sender: OutboundSender,
        rtp: RouterRtpParameters,
    ) -> Option<PendingConsumerSetup> {
        let key = target.consumer_key();
        let active = selection.delivery_active();
        let reservation = self.route_graph.reserve_consumer_setup(key, selection)?;
        let source_worker = target.source.session_key().media_worker_id();
        let target_worker = target.session.media_worker_id();
        let relays = if source_worker == target_worker {
            Vec::new()
        } else {
            let relays =
                self.route_graph
                    .reserve_relay(&reservation, &target, target_worker, active);
            self.resolve_relay_effects(relays)
        };
        Some(PendingConsumerSetup {
            target,
            consumer,
            reservation,
            sender,
            rtp,
            relays,
        })
    }

    pub(super) fn release_consumer_setup(
        &mut self,
        setup: PendingConsumerSetup,
    ) -> Vec<TransportRelayRouteEffect> {
        let relays = self.route_graph.release_consumer_setup(setup.reservation);
        self.resolve_relay_effects(relays)
    }

    pub(super) fn set_consumer_activity(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        stream_id: &UserStreamId,
        active: bool,
    ) -> Option<ConsumerActivityCommit> {
        let source_id = self.source_id_for_owner_stream(target_user_id, stream_id)?;
        let key = ConsumerKey::new(user_id, source_id);
        self.route_graph.set_selection(&key, active);
        let relay_effects = self.route_graph.set_relay_active(
            user_id,
            connection_id,
            source_id,
            RelayRouteActivity::from_active(active),
        );
        let relay_effects = self.resolve_relay_effects(relay_effects);
        let Some(route) = self.committed_consumer_route_for_key(&key) else {
            return Some(ConsumerActivityCommit {
                update: None,
                relay_effects,
            });
        };
        if route.state.consumer_connection_id != connection_id {
            return Some(ConsumerActivityCommit {
                update: None,
                relay_effects,
            });
        }
        let target = self.consumer_route_target_for_source(&route.transport_ref(), route.source);
        Some(ConsumerActivityCommit {
            update: Some(ReceiverRouteActivity::new(target, active)),
            relay_effects,
        })
    }
}
