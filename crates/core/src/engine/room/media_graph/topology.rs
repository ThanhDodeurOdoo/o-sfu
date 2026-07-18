use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{
    ProducerId, Router, RouterError,
    rtp::{MediaCapabilities, MediaStream as RouterRtpParameters},
};
use tracing::{error, warn};

use super::{
    CommittedConsumerSetup, ConsumerId, ConsumerRouteView, ConsumerSetupTarget,
    DeclaredConsumerSetup, PendingConsumerRouteView, PendingConsumerSetup, PublishedSource,
    ReceiverRouteActivity, SubscriptionKey, ValidatedPublish,
    producer::{PublicationCommitError, allocate_source_descriptor},
    route_graph::{CurrentPublication, RelayRouteEffect, RemovedRoutes, RouteGraph},
    source_index::PublishedSources,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
    media_transport::{
        SessionUploadEncoding, TransportConsumerRoute, TransportMediaId, TransportRelayRouteEffect,
        TransportSessionKey, TransportSourceKey, TransportTeardown,
    },
    room::{
        RoomMediaCounts, RoomRuntimeContext, RouterPlacement,
        effects::transport::RoomTransportPlan, outbound::OutboundSender,
        placement::WorkerLoadIndex,
    },
    source_model::{
        ActiveSpeakerSourceRole, ConsumerSourceSelection, PublishedSourceDescriptor,
        PublishedSourceId, SourceSubscriptionIntent, UserStreamId,
    },
};

#[cfg(test)]
#[path = "TESTS/topology_support.rs"]
mod topology_support;

#[derive(Debug)]
pub struct RoomTopology {
    instance: RoomInstanceId,
    sources: PublishedSources,
    route_graph: RouteGraph,
    router: Router,
    next_producer_id: u64,
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
            sources: PublishedSources::default(),
            route_graph: RouteGraph::default(),
            router,
            next_producer_id: 1,
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
        self.sources.first_transport_media_id()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<TransportMediaId> {
        self.sources
            .transport_media_id(user_id, connection_id, stream_id)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_descriptor(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.sources
            .source(source_id)
            .map(|source| &source.descriptor)
    }

    #[must_use]
    pub(in crate::engine::room) fn source_for_transport_media(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&PublishedSource> {
        self.sources.source_for_transport(transport_media_id)
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
    pub(in crate::engine::room) fn published_source_id(
        &self,
        owner: &UserId,
        connection: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        let id = self.sources.id_for_owner_stream(owner, stream_id)?;
        let source = self.sources.source(id)?;
        (source.transport.session_key().connection_id() == connection).then_some(id)
    }

    pub(in crate::engine::room) fn published_sources(
        &self,
    ) -> impl Iterator<Item = &PublishedSource> {
        self.sources.iter()
    }

    pub(in crate::engine::room) fn active_stream_user_counts(&self) -> BTreeMap<UserStreamId, u64> {
        let mut users_by_stream: BTreeMap<UserStreamId, BTreeSet<UserId>> = BTreeMap::new();
        for source in self.sources.iter().filter(|source| source.active) {
            users_by_stream
                .entry(source.descriptor.stream_id().clone())
                .or_default()
                .insert(source.descriptor.owner().user_id().clone());
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
        let source = self.source_for_transport_media(transport_media_id)?;
        let detector_policy = source.descriptor.policy().active_speaker()?;
        if detector_policy.role() != ActiveSpeakerSourceRole::Detector {
            return None;
        }
        let owner = source.descriptor.owner().user_id();
        self.sources
            .owner_has_promotable_source_in_group(owner, detector_policy.group())
            .then(|| owner.clone())
    }

    pub(in crate::engine::room) fn committed_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.route_graph
            .attached()
            .filter_map(|(key, current)| self.consumer_route(key, current))
    }

    pub(in crate::engine::room) fn committed_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.route_graph
            .attached_for_receiver(user_id)
            .filter_map(|(key, current)| self.consumer_route(key, current))
    }

    pub(in crate::engine::room) fn pending_consumer_routes_for_user(
        &self,
        user_id: &UserId,
    ) -> impl Iterator<Item = PendingConsumerRouteView<'_>> {
        self.route_graph
            .attached_for_receiver(user_id)
            .filter(|(_, current)| current.is_pending())
            .filter_map(|(_, current)| {
                let source = self.sources.source(current.source_id)?;
                Some(PendingConsumerRouteView {
                    source,
                    selection: current.selection,
                })
            })
    }

    #[must_use]
    pub(in crate::engine::room) fn consumer_source_selection(
        &self,
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
    ) -> Option<ConsumerSourceSelection> {
        self.route_graph.selection(key, source_id)
    }

    pub(in crate::engine::room) fn merge_subscription_intent(
        &mut self,
        key: SubscriptionKey,
        intent: SourceSubscriptionIntent,
    ) {
        self.route_graph.merge_intent(key, intent);
    }

    #[must_use]
    pub(in crate::engine::room) fn subscription_intent(
        &self,
        key: &SubscriptionKey,
    ) -> SourceSubscriptionIntent {
        self.route_graph.intent(key)
    }

    pub(in crate::engine::room) fn committed_consumer_user_ids_for_source(
        &self,
        source_id: PublishedSourceId,
    ) -> BTreeSet<UserId> {
        self.route_graph
            .attached_for_source(source_id)
            .filter(|(_, current)| current.committed().is_some())
            .map(|(key, _)| key.receiver.clone())
            .collect()
    }

    pub(in crate::engine::room) fn committed_consumer_user_ids_for_owner_sources(
        &self,
        user_id: &UserId,
    ) -> BTreeSet<UserId> {
        let route_graph = &self.route_graph;
        self.sources
            .ids_for_owner(user_id)
            .flat_map(|source_id| route_graph.attached_for_source(source_id))
            .filter(|(_, current)| current.committed().is_some())
            .map(|(key, _)| key.receiver.clone())
            .collect()
    }

    #[must_use]
    pub(in crate::engine::room) fn committed_consumer_route_for_key(
        &self,
        key: &SubscriptionKey,
    ) -> Option<ConsumerRouteView<'_>> {
        let (key, current) = self.route_graph.current(key)?;
        self.consumer_route(key, current)
    }

    #[must_use]
    pub(in crate::engine::room) fn published_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSource> {
        self.sources.source(source_id)
    }

    pub(super) fn missing_consumer_targets_for_source<'a>(
        &mut self,
        source_id: PublishedSourceId,
        receivers: impl IntoIterator<Item = (&'a UserId, ConnectionId)>,
    ) -> Vec<ConsumerSetupTarget> {
        receivers
            .into_iter()
            .filter_map(|(user, connection)| self.consumer_target(user, connection, source_id))
            .collect()
    }

    fn consumer_target(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        source_id: PublishedSourceId,
    ) -> Option<ConsumerSetupTarget> {
        let consumer_session = self.committed_transport_user_key(user_id.clone(), connection_id)?;
        let (sources, route_graph) = (&self.sources, &mut self.route_graph);
        Self::attach_consumer_target(route_graph, &consumer_session, sources.source(source_id)?)
    }

    fn attach_consumer_target(
        route_graph: &mut RouteGraph,
        consumer_session: &TransportSessionKey,
        source: &PublishedSource,
    ) -> Option<ConsumerSetupTarget> {
        if source.descriptor.owner().user_id() == consumer_session.user_id() {
            return None;
        }
        let target = ConsumerSetupTarget::new(consumer_session.clone(), source);
        route_graph
            .attach_for_setup(target.subscription_key(), target.source_id)
            .then_some(target)
    }

    pub(super) fn missing_consumer_targets(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        include_source: impl Fn(&PublishedSource) -> bool,
    ) -> Vec<ConsumerSetupTarget> {
        let Some(consumer_session) =
            self.committed_transport_user_key(user_id.clone(), connection_id)
        else {
            return Vec::new();
        };
        let (sources, route_graph) = (&self.sources, &mut self.route_graph);
        sources
            .iter()
            .filter(|source| include_source(source))
            .filter_map(|source| {
                Self::attach_consumer_target(route_graph, &consumer_session, source)
            })
            .collect()
    }

    pub(in crate::engine::room) fn record_consumer_loads(
        &self,
        loads: &mut WorkerLoadIndex,
        current_connection_id: impl Fn(&UserId) -> Option<ConnectionId>,
    ) {
        for (key, current) in self.route_graph.attached() {
            let connection_id = if let Some((route, _)) = current.committed() {
                route.consumer_session_key().connection_id()
            } else if current.is_pending() {
                let Some(connection_id) = current_connection_id(&key.receiver) else {
                    continue;
                };
                connection_id
            } else {
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
        self.sources.iter().any(|source| {
            if !source.active {
                return false;
            }
            let mut deliveries_by_worker = BTreeMap::new();
            for (key, current) in self
                .route_graph
                .attached_for_source(source.descriptor.source_id())
            {
                if current.committed().is_none() && !current.is_pending() {
                    continue;
                }
                if !current.selection.delivery_active() {
                    continue;
                }
                let Some(connection_id) = current_connection_id(&key.receiver) else {
                    continue;
                };
                let Some(media_worker) = self
                    .router
                    .committed_media_worker_id(&key.receiver, connection_id)
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
        key: &SubscriptionKey,
        source_id: PublishedSourceId,
        route: &TransportConsumerRoute,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        self.route_graph
            .update_selection(key, source_id, route, update)
    }

    fn detach_user_sources(
        &mut self,
        user_id: &UserId,
    ) -> (Vec<TransportSourceKey>, RemovedRoutes) {
        let mut sources = Vec::new();
        let mut removed = RemovedRoutes::default();
        let source_ids = self.sources.ids_for_owner(user_id).collect::<Vec<_>>();
        for source_id in source_ids {
            if let Some((source, routes)) = self.remove_source(source_id) {
                sources.push(source.transport);
                removed.extend(routes);
            }
        }
        (sources, removed)
    }

    fn remove_source(
        &mut self,
        source_id: PublishedSourceId,
    ) -> Option<(PublishedSource, RemovedRoutes)> {
        let source = self.sources.remove(source_id)?;
        let routes = self.route_graph.detach_source(source_id);
        Some((source, routes))
    }

    fn consumer_route<'a>(
        &'a self,
        key: &'a SubscriptionKey,
        current: &'a CurrentPublication,
    ) -> Option<ConsumerRouteView<'a>> {
        let (route, mid) = current.committed()?;
        let source = self.sources.source(current.source_id)?;
        Some(ConsumerRouteView {
            key,
            route,
            mid,
            source,
            selection: current.selection,
        })
    }

    pub(in crate::engine::room) fn commit_publication(
        &mut self,
        publish: ValidatedPublish,
        rtp: RouterRtpParameters,
        encodings: &[SessionUploadEncoding],
        media: TransportMediaId,
    ) -> Result<PublishedSourceId, PublicationCommitError> {
        let descriptor = allocate_source_descriptor(&mut self.sources, &publish, &rtp, encodings)?;
        let source_id = descriptor.source_id();
        let producer_id = ProducerId::allocate(&mut self.next_producer_id);
        let routed = self
            .router
            .add_producer(publish.session_key.user_id(), producer_id)?;
        self.sources.insert(PublishedSource {
            descriptor,
            transport: TransportSourceKey::new(publish.session_key, media),
            rtp,
            routed,
            active: true,
        });
        Ok(source_id)
    }

    fn media_teardowns(
        sources: impl IntoIterator<Item = TransportSourceKey>,
        routes: impl IntoIterator<Item = TransportConsumerRoute>,
    ) -> impl Iterator<Item = TransportTeardown> {
        sources
            .into_iter()
            .map(|source| TransportTeardown::RemoveMedia {
                session_key: source.session_key().clone(),
                transport_media_id: source.transport_media_id(),
            })
            .chain(
                routes
                    .into_iter()
                    .map(|route| TransportTeardown::RemoveMedia {
                        session_key: route.consumer_session_key().clone(),
                        transport_media_id: route.consumer_transport_media_id(),
                    }),
            )
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
        connection_id: ConnectionId,
        source_id: PublishedSourceId,
    ) -> Option<RoomTransportPlan> {
        let source = self.sources.source(source_id)?;
        if source.descriptor.owner().user_id() != user_id
            || source.transport.session_key().connection_id() != connection_id
        {
            return None;
        }
        let routed = source.routed;
        if let Some(error) = self.router.remove_producer(routed).err() {
            error!(
                ?user_id,
                ?source_id,
                ?error,
                "repaired published track room state after router producer teardown failed"
            );
        }
        let (source, removed) = self.remove_source(source_id)?;
        let teardown = Self::media_teardowns([source.transport], removed.routes);
        let relay_effects = self.resolve_relay_effects(removed.relays);
        Some(RoomTransportPlan::from_relays_and_teardown(
            relay_effects,
            teardown,
        ))
    }

    pub fn set_published_source_activity(
        &mut self,
        source_id: PublishedSourceId,
        connection_id: ConnectionId,
        active: bool,
    ) -> bool {
        let Some(source) = self.sources.source_mut(source_id) else {
            return false;
        };
        if source.transport.session_key().connection_id() != connection_id
            || source.active == active
        {
            return false;
        }
        source.active = active;
        true
    }

    /// commits the new connection before returning teardown for displaced placement
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
                let close_session = TransportTeardown::CloseSession {
                    session_key: replaced_session_key.clone(),
                };
                let (_, removed_sources) = self.detach_user_sources(user_id);
                let receiver_relays = self.route_graph.reset_receiver_for_replacement(user_id);
                let RemovedRoutes { routes, mut relays } = removed_sources;
                relays.extend(receiver_relays);
                let relay_effects = self.resolve_relay_effects_with_displaced(
                    relays,
                    user_id,
                    replaced_session_key,
                );
                let teardown = Self::media_teardowns([], routes).chain([close_session]);
                RoomTransportPlan::from_relays_and_teardown(relay_effects, teardown)
            },
        );
        Ok(SessionPlacementCommit {
            receipt,
            replacement_transport_plan,
        })
    }

    pub fn remove_session(&mut self, user_id: &UserId) -> RoomTransportPlan {
        let (sources, mut removed) = self.detach_user_sources(user_id);
        removed.extend(self.route_graph.remove_receiver(user_id));
        let teardown = Self::media_teardowns(sources, removed.routes);
        let relay_effects = self.resolve_relay_effects(removed.relays);
        if let Some(error) = self.router.remove_session(user_id).err() {
            error!(?user_id, ?error, "failed to remove user from room router");
        }
        RoomTransportPlan::from_relays_and_teardown(relay_effects, teardown)
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
        let (route_graph, router) = (&mut self.route_graph, &mut self.router);
        let result = route_graph.commit(
            reservation,
            route.clone(),
            committed_mid,
            selection,
            || match router.add_consumer(target.session.user_id(), consumer, target.routed) {
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
            },
        );
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
        let key = target.subscription_key();
        let active = selection.delivery_active();
        let reservation =
            self.route_graph
                .reserve_consumer_setup(key, target.source_id, selection)?;
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
        let key = SubscriptionKey::new(user_id, target_user_id, stream_id);
        let source_id = self.source_id_for_owner_stream(target_user_id, stream_id)?;
        let relay_effects =
            self.route_graph
                .set_activity(&key, source_id, connection_id, active)?;
        let relay_effects = self.resolve_relay_effects(relay_effects);
        let update = self
            .committed_consumer_route_for_key(&key)
            .filter(|route| route.route.consumer_session_key().connection_id() == connection_id)
            .map(|route| ReceiverRouteActivity::new(route.target(), active));
        Some(ConsumerActivityCommit {
            update,
            relay_effects,
        })
    }
}
