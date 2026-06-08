use o_sfu_router::{ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState};
use tracing::warn;

use super::{
    ConsumerRouteUpdate, ConsumerSetupTarget, ResolvedRelayRouteEffect,
    route_graph::{ConsumerRouteReservation, RelayRouteEffect},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{RelayRouteActivity, TransportMediaId},
    room::{
        LocalRouterRuntimeContext,
        cleanup::TransportCleanupOperation,
        routing::{CommittedRoutingReceipt, RoomRoutingError, RoomRoutingRepairReport},
        state::RoomState,
    },
    source_model::{ConsumerSourceSelection, UserStreamId},
};

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
    pub(super) update: Option<ConsumerRouteUpdate>,
    pub(super) relay_effects: Vec<ResolvedRelayRouteEffect>,
    pub(super) routing_error: Option<RoomRoutingError>,
}

#[derive(Debug)]
pub(super) struct ConsumerTopologyRejected;

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

    pub fn extend_relays(&mut self, relay_effects: Vec<ResolvedRelayRouteEffect>) {
        self.relay_effects.extend(relay_effects);
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

impl RoomState {
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
                    connection_id: session.connection_id,
                }]
            });
            let relay_effects = self.purge_user_media(user_id);
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

    pub fn purge_user_media(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        self.media.remove_user_media(user_id)
    }

    pub fn remove_user(&mut self, user_id: &UserId) -> UserTopologyTeardown {
        let transport_removals = self.media.transport_removals_for_user(user_id);
        let transport_cleanup = self.transport_cleanup_operations(transport_removals);
        let affected_consumers = self.media.routed_consumer_ids_affected_by_user(user_id);
        let relay_effects = self.purge_user_media(user_id);
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
                target.kind,
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
            })
            .map_err(|()| ConsumerTopologyRejected)?;
        if self.media.routes.commit(
            reservation,
            target.consumer_state(routed_consumer_id, media),
            selection,
        ) {
            return Ok((active != reservation.selection().delivery_active()).then_some(active));
        }
        let rollback_error = self.routing.remove_consumer(routed_consumer_id).err();
        if let Some(error) = rollback_error {
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
            self.media
                .routes
                .reserve_relay(&reservation, target, target_worker, active)
        };
        let relays = self.resolved_relay_route_effects(relays);
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
        let key = super::ConsumerKey::new(user_id, source_id);
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
            .then_some(ConsumerRouteUpdate { target, active });
        Some(ConsumerActivityCommit {
            update,
            relay_effects,
            routing_error,
        })
    }
}
