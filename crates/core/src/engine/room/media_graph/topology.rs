use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{
    rtp::{MediaCapabilities, MediaStream as RouterRtpParameters},
    state::{
        ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState, ProducerRouteState,
    },
    topology::{RoutedConsumerId, RoutingError, RoutingPlacementCommit, RoutingTopology},
};
use tracing::{error, warn};

use super::{
    ConsumerKey, ConsumerRouteTarget, ConsumerRouteTransportRef, ConsumerRouteView,
    ConsumerSetupOutcome, ConsumerSetupTarget, PendingConsumerRouteView, PendingConsumerSetup,
    ProducerRouteTarget, ProducerRuntimeId, PublishedProducer, PublishedSourceInstall,
    ReceiverRouteActivity, RemoteTrackSetup, ResolvedRelayRouteEffect, RoomMediaGraph,
    SourceTransportMediaIndexEntry, SourceView, TransportMediaRemoval, ValidatedPublish,
    route_graph::RelayRouteEffect,
};
use crate::{
    RoomSpilloverMode,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::{
            RelayRouteActivity, TransportConsumerRoute, TransportMediaId, TransportSessionKey,
            TransportSourceKey,
        },
        room::{
            RoomMediaCounts, RoomRuntimeContext, RouterPlacement,
            cleanup::TransportCleanupOperation,
            outbound::OutboundSender,
            placement::{LoadTriggeredPlacementState, WorkerLoadIndex},
        },
        source_model::{
            ActiveSpeakerSourceRole, ConsumerSourceSelection, PublishedSourceDescriptor,
            PublishedSourceId, UserStreamId,
        },
    },
};

#[derive(Debug)]
pub struct RoomTopology {
    instance: RoomInstanceId,
    media: RoomMediaGraph,
    routing: RoutingTopology,
    transport_session_by_connection: BTreeMap<ConnectionId, TransportSessionKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTransportReceipt {
    pub connection_id: ConnectionId,
    pub transport_session_key: TransportSessionKey,
}

#[derive(Debug)]
pub struct SessionPlacementCommit {
    pub receipt: CommittedTransportReceipt,
    pub replacement_effects: MediaTopologyEffects,
}

#[derive(Debug)]
pub(super) struct ConsumerActivityCommit {
    pub(super) update: Option<ReceiverRouteActivity>,
    pub(super) relay_effects: Vec<ResolvedRelayRouteEffect>,
    pub(super) routing_error: Option<RoutingError>,
}

#[derive(Debug)]
pub enum SessionPlacementRejection {
    MissingPreviousSession { previous_connection: ConnectionId },
    Router(RoutingError),
}

#[derive(Debug, Default)]
pub struct MediaTopologyEffects {
    relay_effects: Vec<ResolvedRelayRouteEffect>,
    transport_cleanup: Vec<TransportCleanupOperation>,
}

impl MediaTopologyEffects {
    pub fn new(
        relay_effects: Vec<ResolvedRelayRouteEffect>,
        transport_cleanup: Vec<TransportCleanupOperation>,
    ) -> Self {
        Self {
            relay_effects,
            transport_cleanup,
        }
    }

    pub fn extend(&mut self, other: Self) {
        self.relay_effects.extend(other.relay_effects);
        self.transport_cleanup.extend(other.transport_cleanup);
    }

    pub fn extend_cleanup(&mut self, transport_cleanup: Vec<TransportCleanupOperation>) {
        self.transport_cleanup.extend(transport_cleanup);
    }

    pub fn push_cleanup(&mut self, operation: TransportCleanupOperation) {
        self.transport_cleanup.push(operation);
    }

    pub fn into_parts(
        self,
    ) -> (
        Vec<ResolvedRelayRouteEffect>,
        Vec<TransportCleanupOperation>,
    ) {
        (self.relay_effects, self.transport_cleanup)
    }
}

impl RoomTopology {
    pub fn new(
        runtime_context: &RoomRuntimeContext,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        Self {
            instance: runtime_context.instance(),
            media: RoomMediaGraph::default(),
            routing: RoutingTopology::new(
                runtime_context.primary_router(),
                runtime_context.initial_router_placements().cloned(),
                router_rtp_capabilities,
            ),
            transport_session_by_connection: BTreeMap::new(),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn media(&self) -> &RoomMediaGraph {
        &self.media
    }

    pub(in crate::engine::room) fn routing(&self) -> &RoutingTopology {
        &self.routing
    }

    #[must_use]
    pub fn committed_transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        self.routing
            .committed_media_worker_id(user_id, connection_id)?;
        self.transport_session_by_connection
            .get(&connection_id)
            .cloned()
    }

    pub(in crate::engine::room) fn committed_media_worker_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<MediaWorkerId> {
        self.routing
            .committed_media_worker_id(user_id, connection_id)?;
        self.transport_session_by_connection
            .get(&connection_id)
            .map(TransportSessionKey::media_worker_id)
    }

    #[must_use]
    #[expect(
        clippy::unreachable,
        reason = "current room operations require committed connection placement and must not synthesize a transport worker"
    )]
    pub fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        let Some(session_key) = self.committed_transport_user_key(user_id, connection_id) else {
            unreachable!("transport session key lookup requires committed connection placement");
        };
        session_key
    }

    fn transport_session_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_worker_id: MediaWorkerId,
    ) -> TransportSessionKey {
        TransportSessionKey::new(
            self.instance,
            media_worker_id,
            connection_id,
            user_id.clone(),
        )
    }

    pub fn unregister_committed_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) {
        if self
            .routing
            .committed_media_worker_id(user_id, connection_id)
            .is_some()
        {
            self.transport_session_by_connection.remove(&connection_id);
        }
        self.routing
            .unregister_committed_placement(user_id, connection_id);
    }

    pub fn reconcile_spillover_routers(
        &mut self,
        spillover: RoomSpilloverMode,
        placement: &mut LoadTriggeredPlacementState,
    ) {
        match spillover {
            RoomSpilloverMode::StrictSingleRouter => {}
            RoomSpilloverMode::BoundedLocalSpillover => {
                let idle_router_ids = self.routing.idle_spillover_routers();
                self.routing.detach_spillover_routers(&idle_router_ids);
                placement.clear_cooldowns(&idle_router_ids);
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let idle_router_ids = self.routing.idle_spillover_routers();
                let policy = policy.parts();
                let detachments =
                    placement.cooldown_detachments(&idle_router_ids, policy.cooldown_window);
                self.routing.detach_spillover_routers(&detachments);
            }
        }
    }

    #[must_use]
    pub fn media_counts(&self) -> RoomMediaCounts {
        RoomMediaCounts {
            publications: self.media.sources.publication_count(),
            subscriptions: self.media.routes.subscription_count(),
        }
    }

    #[must_use]
    pub(in crate::engine::room) fn source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.media.sources.source(source_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.media.sources.transport_media_entry(transport_media_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.media
            .sources
            .id_for_owner_stream(owner_user_id, stream_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        self.media
            .sources
            .producer_route_target(owner_user_id, owner_connection_id, stream_id)
    }

    pub(in crate::engine::room) fn source_views(&self) -> impl Iterator<Item = SourceView<'_>> {
        self.media.sources.source_views()
    }

    pub(in crate::engine::room) fn active_stream_user_counts(&self) -> BTreeMap<UserStreamId, u64> {
        let mut users_by_stream: BTreeMap<UserStreamId, BTreeSet<UserId>> = BTreeMap::new();
        for view in self
            .media
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
        self.media
            .sources
            .owner_has_promotable_source_in_group(&entry.owner, detector_policy.group())
            .then(|| entry.owner.clone())
    }

    pub(in crate::engine::room) fn live_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.media
            .routes
            .committed_entries()
            .filter_map(|(key, state)| self.consumer_route_for_key(key, state))
    }

    pub(in crate::engine::room) fn pending_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PendingConsumerRouteView<'_>> {
        self.media
            .routes
            .pending_keys_for_user(user_id)
            .filter_map(|key| {
                let view = self.media.sources.source_view(key.source_id)?;
                Some(PendingConsumerRouteView {
                    source: view.source,
                    producer: view.producer,
                    selection: self.media.routes.selection(key),
                })
            })
    }

    #[must_use]
    pub(in crate::engine::room) fn consumer_source_selection(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerSourceSelection> {
        self.media.routes.selection(key)
    }

    #[must_use]
    pub(in crate::engine::room) fn committed_consumer_route_for_key(
        &self,
        key: &ConsumerKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let state = self.media.routes.consumer_state(key)?;
        self.consumer_route_for_key(key, state)
    }

    #[must_use]
    pub(in crate::engine::room) fn has_consumer_setup_or_route(&self, key: &ConsumerKey) -> bool {
        self.media.routes.has_consumer_setup_or_route(key)
    }

    #[must_use]
    pub(in crate::engine::room) fn producer(
        &self,
        producer_id: ProducerRuntimeId,
    ) -> Option<&PublishedProducer> {
        self.media.sources.producer(producer_id)
    }

    pub(in crate::engine::room) fn consumer_targets_for_producer<'a>(
        &self,
        producer_id: ProducerRuntimeId,
        consumers: impl IntoIterator<Item = (&'a UserId, ConnectionId)>,
    ) -> Vec<ConsumerSetupTarget> {
        let Some(producer) = self.producer(producer_id) else {
            return Vec::new();
        };
        let Some(media) = producer.transport_media_id else {
            return Vec::new();
        };
        let Some(producer_session) = self
            .committed_transport_user_key(&producer.owner_user_id, producer.owner_connection_id)
        else {
            return Vec::new();
        };
        consumers
            .into_iter()
            .filter_map(|(user_id, connection_id)| {
                self.consumer_target(
                    user_id,
                    connection_id,
                    producer_id,
                    producer,
                    media,
                    &producer_session,
                )
            })
            .collect()
    }

    fn consumer_target(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        producer: &PublishedProducer,
        transport_media_id: TransportMediaId,
        producer_session: &TransportSessionKey,
    ) -> Option<ConsumerSetupTarget> {
        if producer.owner_user_id == *user_id {
            return None;
        }
        let key = ConsumerKey::new(user_id, producer.source_id);
        if self.has_consumer_setup_or_route(&key) {
            return None;
        }
        let consumer_session = self.committed_transport_user_key(user_id, connection_id)?;
        Some(ConsumerSetupTarget::new(
            user_id.clone(),
            connection_id,
            consumer_session,
            producer_session.clone(),
            producer_id,
            producer,
            transport_media_id,
        ))
    }

    pub(in crate::engine::room) fn missing_consumer_targets(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        include_producer: impl Fn(&PublishedProducer) -> bool,
    ) -> Vec<ConsumerSetupTarget> {
        self.media
            .sources
            .producers()
            .filter_map(|(producer_id, producer)| {
                if !include_producer(producer) {
                    return None;
                }
                let media = producer.transport_media_id?;
                let session = self.committed_transport_user_key(
                    &producer.owner_user_id,
                    producer.owner_connection_id,
                )?;
                self.consumer_target(
                    user_id,
                    connection_id,
                    producer_id,
                    producer,
                    media,
                    &session,
                )
            })
            .collect()
    }

    pub(in crate::engine::room) fn record_consumer_loads(
        &self,
        loads: &mut WorkerLoadIndex,
        current_connection_id: impl Fn(&UserId) -> Option<ConnectionId>,
    ) {
        for connection_id in self.media.routes.committed_consumer_connection_ids() {
            loads.record_consumer(self.routing.media_worker_id_for_connection(connection_id));
        }
        for user_id in self.media.routes.pending_consumer_user_ids() {
            let Some(connection_id) = current_connection_id(user_id) else {
                continue;
            };
            loads.record_consumer(self.routing.media_worker_id_for_connection(connection_id));
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
        self.media.sources.source_views().any(|view| {
            if !view.producer.active {
                return false;
            }
            let mut deliveries_by_worker = BTreeMap::new();
            for key in self.media.routes.keys_for_source(view.source.source_id()) {
                if !self.media.routes.has_consumer_setup_or_route(key) {
                    continue;
                }
                if self
                    .media
                    .routes
                    .selection(key)
                    .is_some_and(|selection| !selection.delivery_active())
                {
                    continue;
                }
                let Some(connection_id) = current_connection_id(&key.consumer_user_id) else {
                    continue;
                };
                let Some(media_worker) =
                    self.committed_media_worker_id(&key.consumer_user_id, connection_id)
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
        update(self.media.routes.selection_mut_or_open(key));
        true
    }

    #[cfg(test)]
    pub(in crate::engine::room) fn producer_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedProducer> {
        self.media.sources.producer_for_source(source_id)
    }

    #[cfg(test)]
    pub(in crate::engine::room) fn install_source_for_test(
        &mut self,
        install: PublishedSourceInstall,
    ) {
        self.media.sources.install_source(install);
    }

    fn consumer_route_for_key(
        &self,
        key: &ConsumerKey,
        state: super::ConsumerState,
    ) -> Option<ConsumerRouteView<'_>> {
        let view = self.media.sources.source_view(key.source_id)?;
        Some(ConsumerRouteView {
            consumer_user_id: key.consumer_user_id.clone(),
            state,
            source: view.source,
            producer: view.producer,
            selection: self.media.routes.selection(key),
        })
    }

    pub fn publish_source(
        &mut self,
        publish: ValidatedPublish,
        producer_id: ProducerRuntimeId,
        source_descriptor: PublishedSourceDescriptor,
        consumable_rtp_parameters: RouterRtpParameters,
        transport_media_id: TransportMediaId,
    ) -> Result<(), RoutingError> {
        let routed_producer_id = self
            .routing
            .add_producer(&publish.owner_user_id, publish.media_kind)?;
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
        self.media.sources.install_source(PublishedSourceInstall {
            source_descriptor,
            producer_id,
            producer,
            transport_media_id,
        });
        Ok(())
    }

    pub(in crate::engine::room) fn consumer_route_target_for_source(
        &self,
        route: ConsumerRouteTransportRef,
        source: &PublishedSourceDescriptor,
    ) -> ConsumerRouteTarget {
        let transport_route = TransportConsumerRoute::new(
            self.transport_user_key(&route.consumer_user_id, route.consumer_connection_id),
            route.consumer_media,
            TransportSourceKey::new(
                self.transport_user_key(&route.source_user_id, route.source_connection_id),
                route.source_media,
            ),
        );
        ConsumerRouteTarget::new(
            route,
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
            .map(|removal| {
                let connection_id = removal.connection;
                TransportCleanupOperation::RemoveMedia {
                    session_key: self.transport_user_key(&removal.user, connection_id),
                    transport_media_id: removal.transport_media,
                }
            })
            .collect()
    }

    fn resolved_relay_route_effects(
        &self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
    ) -> Vec<ResolvedRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| ResolvedRelayRouteEffect {
                source_session_key: self
                    .transport_user_key(&effect.route.source_user, effect.route.source_connection),
                route: effect.route,
                action: effect.action,
            })
            .collect()
    }

    fn resolved_relay_route_effects_with_displaced(
        &self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
        user_id: &UserId,
        session_key: &TransportSessionKey,
    ) -> Vec<ResolvedRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| {
                let source_session_key = if effect.route.source_user == *user_id
                    && effect.route.source_connection == session_key.connection_id()
                {
                    session_key.clone()
                } else {
                    self.transport_user_key(
                        &effect.route.source_user,
                        effect.route.source_connection,
                    )
                };
                ResolvedRelayRouteEffect {
                    source_session_key,
                    route: effect.route,
                    action: effect.action,
                }
            })
            .collect()
    }

    pub(super) fn unpublish_source(
        &mut self,
        user_id: &UserId,
        target: &ProducerRouteTarget,
    ) -> Option<MediaTopologyEffects> {
        let transport_removals = self
            .media
            .transport_removals_for_producer_target(user_id, target);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        if let Some(error) = self
            .routing
            .remove_producer(target.routed_producer_id)
            .err()
        {
            error!(
                ?user_id,
                ?target,
                ?error,
                "repaired published track room state after router producer teardown failed"
            );
        }
        let (_producer, relay_effects) = self.media.remove_source(target.source_id)?;
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        Some(MediaTopologyEffects::new(relay_effects, transport_cleanup))
    }

    pub fn set_published_source_activity(
        &mut self,
        target: &ProducerRouteTarget,
        current_connection_id: Option<ConnectionId>,
        active: bool,
    ) -> Result<bool, RoutingError> {
        let Some(producer) = self
            .media
            .sources
            .producer_for_route_target(target, current_connection_id)
        else {
            return Ok(false);
        };
        if producer.active == active {
            return Ok(false);
        }
        let route_state = if active {
            ProducerRouteState::Active
        } else {
            ProducerRouteState::Paused
        };
        self.routing
            .set_producer_route_state(target.routed_producer_id, route_state)?;
        self.media.sources.set_producer_active(target, active);
        Ok(true)
    }

    pub fn commit_session_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        previous_connection: Option<ConnectionId>,
        home_placement: RouterPlacement,
    ) -> Result<SessionPlacementCommit, SessionPlacementRejection> {
        let previous_session_key = if let Some(previous_connection) = previous_connection {
            let Some(key) = self.committed_transport_user_key(user_id, previous_connection) else {
                return Err(SessionPlacementRejection::MissingPreviousSession {
                    previous_connection,
                });
            };
            Some(key)
        } else {
            None
        };
        let RoutingPlacementCommit {
            receipt: router_receipt,
            displaced_connection,
        } = self
            .routing
            .commit_session_placement(user_id, connection_id, home_placement)
            .map_err(SessionPlacementRejection::Router)?;
        let session_key = self.transport_session_key(
            user_id,
            router_receipt.connection_id,
            router_receipt.media_worker_id,
        );
        if let Some(connection_id) = displaced_connection {
            self.transport_session_by_connection.remove(&connection_id);
        }
        self.transport_session_by_connection
            .insert(router_receipt.connection_id, session_key.clone());
        let receipt = CommittedTransportReceipt {
            connection_id: router_receipt.connection_id,
            transport_session_key: session_key,
        };
        let replacement_effects = if let Some(key) = previous_session_key.as_ref() {
            let transport_cleanup = vec![TransportCleanupOperation::CloseUser {
                session_key: key.clone(),
            }];
            let relay_effects = self.media.remove_user_media(user_id);
            let relay_effects =
                self.resolved_relay_route_effects_with_displaced(relay_effects, user_id, key);
            MediaTopologyEffects::new(relay_effects, transport_cleanup)
        } else {
            MediaTopologyEffects::default()
        };
        Ok(SessionPlacementCommit {
            receipt,
            replacement_effects,
        })
    }

    pub fn remove_session(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> MediaTopologyEffects {
        let transport_removals = self.media.transport_removals_for_user(user_id);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        let relay_effects = self.media.remove_user_media(user_id);
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        self.transport_session_by_connection.remove(&connection_id);
        let routing_repair = self.routing.remove_session_repairing(user_id);
        if !routing_repair.is_clean() {
            error!(
                ?user_id,
                errors = ?routing_repair.errors(),
                "repaired user topology during room teardown"
            );
        }
        MediaTopologyEffects::new(relay_effects, transport_cleanup)
    }

    pub(super) fn commit_consumer_setup(
        &mut self,
        mut setup: PendingConsumerSetup,
        selection: ConsumerSourceSelection,
        media: TransportMediaId,
        mid: Option<String>,
        producer_active: bool,
    ) -> ConsumerSetupOutcome {
        if self.media.routes.contains(setup.reservation.key()) {
            return ConsumerSetupOutcome::Released(self.release_consumer_setup(setup));
        }
        let active = selection.delivery_active();
        let Some(routed_consumer_id) = self.add_consumer_route(&setup.target, active) else {
            return ConsumerSetupOutcome::Released(self.release_consumer_setup(setup));
        };
        if self.media.routes.commit(
            &setup.reservation,
            setup.target.consumer_state(routed_consumer_id, media),
            selection,
        ) {
            if let Some(mid) = mid {
                setup.track.mid = mid;
            }
            setup.track.active = producer_active;
            return ConsumerSetupOutcome::Committed {
                sender: setup.sender,
                track: setup.track,
                transport_activity_update: (active
                    != setup.reservation.selection().delivery_active())
                .then_some(active),
            };
        }
        self.rollback_consumer_route(&setup.target, routed_consumer_id);
        ConsumerSetupOutcome::Released(self.release_consumer_setup(setup))
    }

    fn add_consumer_route(
        &mut self,
        target: &ConsumerSetupTarget,
        active: bool,
    ) -> Option<RoutedConsumerId> {
        let route_state = if active {
            RouterConsumerRouteState::Active
        } else {
            RouterConsumerRouteState::Paused
        };
        match self.routing.add_consumer_with_route_state(
            &target.user,
            target.routed,
            ConsumerCapability::Compatible,
            route_state,
        ) {
            Ok(routed_consumer_id) => Some(routed_consumer_id),
            Err(error) => {
                warn!(
                    consumer_user_id = ?target.user,
                    source_id = ?target.source_id,
                    ?error,
                    "router rejected consumer creation"
                );
                None
            }
        }
    }

    fn rollback_consumer_route(
        &mut self,
        target: &ConsumerSetupTarget,
        routed_consumer_id: RoutedConsumerId,
    ) {
        if let Some(error) = self.routing.remove_consumer(routed_consumer_id).err() {
            warn!(
                consumer_user_id = ?target.user,
                ?routed_consumer_id,
                ?error,
                "failed to roll back topology consumer after graph consumer commit rejection"
            );
        } else {
            warn!(
                consumer_user_id = ?target.user,
                ?routed_consumer_id,
                "media graph rejected topology consumer commit"
            );
        }
    }

    pub(super) fn reserve_consumer_setup(
        &mut self,
        target: ConsumerSetupTarget,
        selection: ConsumerSourceSelection,
        sender: OutboundSender,
        track: RemoteTrackSetup,
    ) -> Option<PendingConsumerSetup> {
        let key = target.consumer_key();
        let active = selection.delivery_active();
        let reservation = self.media.routes.reserve_consumer_setup(key, selection)?;
        let source_worker = target.producer_session.media_worker_id();
        let target_worker = target.user_session.media_worker_id();
        let relays = if source_worker == target_worker {
            Vec::new()
        } else {
            let relays =
                self.media
                    .routes
                    .reserve_relay(&reservation, &target, target_worker, active);
            self.resolved_relay_route_effects(relays)
        };
        Some(PendingConsumerSetup {
            target,
            reservation,
            sender,
            track,
            relays,
        })
    }

    pub(super) fn release_consumer_setup(
        &mut self,
        setup: PendingConsumerSetup,
    ) -> Vec<ResolvedRelayRouteEffect> {
        let relays = self.media.routes.release_consumer_setup(setup.reservation);
        self.resolved_relay_route_effects(relays)
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
        self.media.routes.set_selection(&key, active);
        let relay_effects = self.media.routes.set_relay_active(
            user_id,
            connection_id,
            source_id,
            RelayRouteActivity::from_active(active),
        );
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        let Some(route) = self.committed_consumer_route_for_key(&key) else {
            return Some(ConsumerActivityCommit {
                update: None,
                relay_effects,
                routing_error: None,
            });
        };
        if route.state.consumer_connection_id != connection_id {
            return Some(ConsumerActivityCommit {
                update: None,
                relay_effects,
                routing_error: None,
            });
        }
        let routed = route.state.routed_consumer_id;
        let target = self.consumer_route_target_for_source(route.transport_ref(), route.source);
        let route_state = if active {
            RouterConsumerRouteState::Active
        } else {
            RouterConsumerRouteState::Paused
        };
        let routing_error = self
            .routing
            .set_consumer_route_state(routed, route_state)
            .err();
        let update = routing_error
            .is_none()
            .then_some(ReceiverRouteActivity::new(target, active));
        Some(ConsumerActivityCommit {
            update,
            relay_effects,
            routing_error,
        })
    }

    #[cfg(test)]
    pub(in crate::engine::room) fn media_mut_for_test(&mut self) -> &mut RoomMediaGraph {
        &mut self.media
    }

    #[cfg(test)]
    pub(in crate::engine::room) fn routing_mut_for_test(&mut self) -> &mut RoutingTopology {
        &mut self.routing
    }
}
