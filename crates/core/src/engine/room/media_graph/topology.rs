use std::collections::BTreeMap;

use o_sfu_router::{
    ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState, MediaCapabilities,
    MediaStream as RouterRtpParameters, ProducerRouteState, RoutedConsumerId, RoutingError,
    RoutingTopology,
};
use tracing::{error, warn};

use super::{
    ConsumerKey, ConsumerRouteTransportRef, ConsumerSetupOutcome, ConsumerSetupTarget,
    PendingConsumerSetup, ProducerRouteTarget, ProducerRuntimeId, PublishedProducer,
    PublishedSourceInstall, ReceiverRouteActivity, RemoteTrackSetup, ResolvedRelayRouteEffect,
    RoomMediaGraph, TransportMediaRemoval, ValidatedPublish, route_graph::RelayRouteEffect,
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
            cleanup::TransportCleanupOperation, outbound::OutboundSender,
            placement::LoadTriggeredPlacementState,
        },
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId, UserStreamId,
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
            publications: self.media.publication_count(),
            subscriptions: self.media.subscription_count(),
        }
    }

    pub fn update_consumer_source_selection(
        &mut self,
        route: &ConsumerRouteTransportRef,
        source_id: PublishedSourceId,
        update: impl FnOnce(&mut ConsumerSourceSelection),
    ) -> bool {
        self.media
            .update_consumer_source_selection(route, source_id, update)
    }

    pub fn publish_source(
        &mut self,
        publish: ValidatedPublish,
        producer_id: ProducerRuntimeId,
        source_descriptor: PublishedSourceDescriptor,
        consumable_rtp_parameters: RouterRtpParameters,
        transport_media_id: TransportMediaId,
    ) -> Result<PublishedProducer, RoutingError> {
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
        self.media.install_source(PublishedSourceInstall {
            source_descriptor,
            producer_id,
            producer: producer.clone(),
            transport_media_id,
        });
        Ok(producer)
    }

    pub fn transport_consumer_route(
        &self,
        route: &ConsumerRouteTransportRef,
    ) -> TransportConsumerRoute {
        TransportConsumerRoute::new(
            self.transport_user_key(&route.consumer_user_id, route.consumer_connection_id),
            route.consumer_media,
            TransportSourceKey::new(
                self.transport_user_key(&route.source_user_id, route.source_connection_id),
                route.source_media,
            ),
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
        let affected_consumers = self.media.routed_consumer_ids_for_source(target.source_id);
        if let Some(error) = self
            .routing
            .remove_producer(target.routed_producer_id, affected_consumers)
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
        self.media.set_producer_active(target, active);
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
        let affected_consumers = if previous_connection.is_some() {
            self.media.routed_consumer_ids_affected_by_user(user_id)
        } else {
            Vec::new()
        };
        let mut routing = self.routing.clone();
        let (router_receipt, displaced_connection) = routing
            .commit_session_placement(user_id, connection_id, home_placement, affected_consumers)
            .map_err(SessionPlacementRejection::Router)?;
        let session_key = self.transport_session_key(
            user_id,
            router_receipt.connection_id,
            router_receipt.media_worker_id,
        );
        self.routing = routing;
        if let Some(connection_id) = displaced_connection {
            self.transport_session_by_connection.remove(&connection_id);
        }
        self.transport_session_by_connection
            .insert(router_receipt.connection_id, session_key.clone());
        let receipt = CommittedTransportReceipt {
            connection_id: router_receipt.connection_id,
            transport_session_key: session_key,
        };
        let replacement_effects = if previous_connection.is_some() {
            let transport_cleanup = previous_session_key.as_ref().map_or_else(Vec::new, |key| {
                vec![TransportCleanupOperation::CloseUser {
                    session_key: key.clone(),
                }]
            });
            let relay_effects = self.media.remove_user_media(user_id);
            let relay_effects = if let Some(key) = previous_session_key.as_ref() {
                self.resolved_relay_route_effects_with_displaced(relay_effects, user_id, key)
            } else {
                self.resolved_relay_route_effects(relay_effects)
            };
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
        let affected_consumers = self.media.routed_consumer_ids_affected_by_user(user_id);
        let relay_effects = self.media.remove_user_media(user_id);
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        self.transport_session_by_connection.remove(&connection_id);
        let routing_repair = self
            .routing
            .remove_session_repairing(user_id, affected_consumers);
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
        if self.media.contains_consumer(setup.reservation.key()) {
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
        source_worker: MediaWorkerId,
        target_worker: MediaWorkerId,
        sender: OutboundSender,
        track: RemoteTrackSetup,
    ) -> Option<PendingConsumerSetup> {
        let key = target.consumer_key();
        let active = selection.delivery_active();
        let reservation = self.media.routes.reserve_consumer_setup(key, selection)?;
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
        let source_id = self
            .media
            .source_id_for_owner_stream(target_user_id, stream_id)?;
        let key = ConsumerKey::new(user_id, source_id);
        self.media.set_consumer_source_selection(&key, active);
        let relay_effects = self.media.set_relay_consumer_active(
            user_id,
            connection_id,
            source_id,
            RelayRouteActivity::from_active(active),
        );
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        let Some(route) = self.media.committed_consumer_route_for_key(&key) else {
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
        let (routed, target) = {
            let routed = route.state.routed_consumer_id;
            let route_ref = route.transport_ref();
            let transport_route = self.transport_consumer_route(&route_ref);
            (routed, route.target(transport_route))
        };
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
