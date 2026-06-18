use o_sfu_router::{
    ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState, MediaCapabilities,
    MediaStream as RouterRtpParameters, ProducerRouteState,
};
use tracing::warn;

use super::{
    ConsumerKey, ConsumerRouteTransportRef, ConsumerSetupTarget, ProducerRouteTarget,
    ProducerRuntimeId, PublishedProducer, PublishedSourceInstall, ReceiverRouteActivity,
    ResolvedRelayRouteEffect, RoomMediaGraph, TransportMediaRemoval, ValidatedPublish,
    route_graph::{ConsumerRouteReservation, RelayRouteEffect},
};
use crate::{
    RoomSpilloverMode,
    engine::{
        ConnectionId, MediaWorkerId, UserId,
        media_transport::{
            RelayRouteActivity, TransportConsumerRoute, TransportMediaId, TransportSourceKey,
        },
        room::{
            LocalRouterRuntimeContext, RoomMediaCounts, RoomRuntimeContext,
            cleanup::TransportCleanupOperation,
            placement::LoadTriggeredPlacementState,
            routing::{
                CommittedRoutingReceipt, DisplacedRoutingSession, RoomRoutingError,
                RoomRoutingRepairReport, RoomRoutingState,
            },
        },
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId, UserStreamId,
        },
    },
};

#[derive(Debug)]
pub struct RoomTopology {
    media: RoomMediaGraph,
    routing: RoomRoutingState,
}

#[derive(Debug)]
pub struct UserTopologyTeardown {
    pub effects: MediaTopologyEffects,
    pub routing_repair: RoomRoutingRepairReport,
}

#[derive(Debug)]
pub struct SessionPlacementCommit {
    pub receipt: CommittedRoutingReceipt,
    pub replacement_effects: MediaTopologyEffects,
}

#[derive(Debug)]
pub(super) struct ConsumerActivityCommit {
    pub(super) update: Option<ReceiverRouteActivity>,
    pub(super) relay_effects: Vec<ResolvedRelayRouteEffect>,
    pub(super) routing_error: Option<RoomRoutingError>,
}

#[derive(Debug)]
pub(super) struct ConsumerTopologyRejected;

#[derive(Debug)]
pub(super) struct PublishedSourceTeardown {
    pub effects: MediaTopologyEffects,
    pub router_teardown_error: Option<RoomRoutingError>,
}

#[derive(Debug)]
pub enum SessionPlacementRejection {
    MissingPreviousSession { previous_connection: ConnectionId },
    Router(RoomRoutingError),
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
            media: RoomMediaGraph::default(),
            routing: RoomRoutingState::new_with_runtime(
                runtime_context.instance(),
                runtime_context.primary_router(),
                runtime_context.initial_local_router_placements().cloned(),
                router_rtp_capabilities,
            ),
        }
    }

    pub(in crate::engine::room) fn media(&self) -> &RoomMediaGraph {
        &self.media
    }

    pub(in crate::engine::room) fn routing(&self) -> &RoomRoutingState {
        &self.routing
    }

    pub fn unregister_committed_placement(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) {
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

    pub fn commit_published_source(
        &mut self,
        publish: ValidatedPublish,
        producer_id: ProducerRuntimeId,
        source_descriptor: PublishedSourceDescriptor,
        consumable_rtp_parameters: RouterRtpParameters,
        transport_media_id: TransportMediaId,
    ) -> Result<PublishedProducer, RoomRoutingError> {
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
            self.routing
                .transport_user_key(&route.consumer_user_id, route.consumer_connection_id),
            route.consumer_media,
            TransportSourceKey::new(
                self.routing
                    .transport_user_key(&route.source_user_id, route.source_connection_id),
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
                    session_key: self
                        .routing
                        .transport_user_key(&removal.user, connection_id),
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
                    .routing
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
        session: &DisplacedRoutingSession,
    ) -> Vec<ResolvedRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| {
                let source_session_key = if effect.route.source_user == *user_id
                    && effect.route.source_connection == session.connection_id
                {
                    session.transport_session_key.clone()
                } else {
                    self.routing.transport_user_key(
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

    pub(super) fn remove_published_source(
        &mut self,
        user_id: &UserId,
        target: &ProducerRouteTarget,
    ) -> Option<PublishedSourceTeardown> {
        let transport_removals = self
            .media
            .transport_removals_for_producer_target(user_id, target);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        let affected_consumers = self.media.routed_consumer_ids_for_source(target.source_id);
        let router_teardown_error = self
            .routing
            .remove_producer(target.routed_producer_id, affected_consumers)
            .err();
        let (_producer, relay_effects) = self.media.remove_source(target.source_id)?;
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        Some(PublishedSourceTeardown {
            effects: MediaTopologyEffects::new(relay_effects, transport_cleanup),
            router_teardown_error,
        })
    }

    pub fn set_published_source_activity(
        &mut self,
        target: &ProducerRouteTarget,
        current_connection_id: Option<ConnectionId>,
        active: bool,
    ) -> Result<bool, RoomRoutingError> {
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
        home_placement: LocalRouterRuntimeContext,
    ) -> Result<SessionPlacementCommit, SessionPlacementRejection> {
        if let Some(previous_connection) = previous_connection
            && self
                .routing
                .committed_transport_user_key(user_id, previous_connection)
                .is_none()
        {
            return Err(SessionPlacementRejection::MissingPreviousSession {
                previous_connection,
            });
        }
        let affected_consumers = if previous_connection.is_some() {
            self.media.routed_consumer_ids_affected_by_user(user_id)
        } else {
            Vec::new()
        };
        let mut routing = self.routing.clone();
        let (receipt, displaced) = routing
            .commit_session_placement(user_id, connection_id, home_placement, affected_consumers)
            .map_err(SessionPlacementRejection::Router)?;
        self.routing = routing;
        let replacement_effects = if previous_connection.is_some() {
            let transport_cleanup = displaced.as_ref().map_or_else(Vec::new, |session| {
                vec![TransportCleanupOperation::CloseUser {
                    session_key: session.transport_session_key.clone(),
                }]
            });
            let relay_effects = self.media.remove_user_media(user_id);
            let relay_effects = if let Some(session) = displaced.as_ref() {
                self.resolved_relay_route_effects_with_displaced(relay_effects, user_id, session)
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

    pub fn remove_user(&mut self, user_id: &UserId) -> UserTopologyTeardown {
        let transport_removals = self.media.transport_removals_for_user(user_id);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        let affected_consumers = self.media.routed_consumer_ids_affected_by_user(user_id);
        let relay_effects = self.media.remove_user_media(user_id);
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        let routing_repair = self
            .routing
            .remove_session_repairing(user_id, affected_consumers);
        UserTopologyTeardown {
            effects: MediaTopologyEffects::new(relay_effects, transport_cleanup),
            routing_repair,
        }
    }

    pub(super) fn commit_consumer_setup(
        &mut self,
        reservation: &ConsumerRouteReservation,
        target: &ConsumerSetupTarget,
        selection: ConsumerSourceSelection,
        media: TransportMediaId,
    ) -> Result<Option<bool>, ConsumerTopologyRejected> {
        let active = selection.delivery_active();
        let route_state = if active {
            RouterConsumerRouteState::Active
        } else {
            RouterConsumerRouteState::Paused
        };
        let routed_consumer_id = self
            .routing
            .add_consumer_with_route_state(
                &target.user,
                target.routed,
                ConsumerCapability::Compatible,
                route_state,
            )
            .map_err(|error| {
                warn!(
                    consumer_user_id = ?target.user,
                    source_id = ?target.source_id,
                    ?error,
                    "router rejected consumer creation"
                );
                ConsumerTopologyRejected
            })?;
        if self.media.routes.commit(
            reservation,
            target.consumer_state(routed_consumer_id, media),
            selection,
        ) {
            return Ok((active != reservation.selection().delivery_active()).then_some(active));
        }
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
        Err(ConsumerTopologyRejected)
    }

    pub(super) fn reserve_consumer_setup(
        &mut self,
        target: &ConsumerSetupTarget,
        selection: ConsumerSourceSelection,
        source_worker: MediaWorkerId,
        target_worker: MediaWorkerId,
    ) -> Option<(ConsumerRouteReservation, Vec<ResolvedRelayRouteEffect>)> {
        let key = target.consumer_key();
        let active = selection.delivery_active();
        let reservation = self.media.routes.reserve_consumer_setup(key, selection)?;
        let relays = if source_worker == target_worker {
            Vec::new()
        } else {
            let relays =
                self.media
                    .routes
                    .reserve_relay(&reservation, target, target_worker, active);
            self.resolved_relay_route_effects(relays)
        };
        Some((reservation, relays))
    }

    pub(super) fn release_consumer_setup(
        &mut self,
        reservation: ConsumerRouteReservation,
    ) -> Vec<ResolvedRelayRouteEffect> {
        let relays = self.media.routes.release_consumer_setup(reservation);
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
    pub(in crate::engine::room) fn routing_mut_for_test(&mut self) -> &mut RoomRoutingState {
        &mut self.routing
    }
}
