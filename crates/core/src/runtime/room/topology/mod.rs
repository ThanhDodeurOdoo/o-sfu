//! Room-local router topology and same-room spillover.
//!
//! `RoomTopology` is the boundary between room-domain media state and the pure
//! router state machines. Room state owns user membership, sources and desired
//! subscriptions. The topology owns where those router records live.
//!
//! ```text
//! RoomState
//!   users, sources, desired subscriptions
//!          |
//!          v
//! RoomTopology
//!   user -> home router
//!   producer -> source router
//!   consumer -> source router
//!          |
//!          v
//! RoomRouterState
//!   pure router sessions, producers and consumers
//! ```
//!
//! This module does not own transports and it does not forward RTP packets. It
//! owns the cold-path routing placement needed before transport work can be
//! addressed correctly. Producers are anchored on the router where their owner
//! publishes. Consumers are created on the source producer's router, even when
//! the receiver's home session lives on another local router.
//!
//! # Spillover model
//!
//! A room starts with one primary router and attaches spillover router state
//! only when room placement hands a resolved join placement to topology. Strict
//! policy only uses the primary router. Bounded and load-triggered spillover may
//! place home sessions on assigned spillover placements. Spillover router state
//! is detached once no live home session remains there.
//!
//! Cross-router subscriptions use a shadow session on the source router. The
//! receiver keeps its home router for transport ownership, while the consumer
//! edge is routed through the source router so producer state remains
//! authoritative in one pure router instance.
//!
//! # Performance
//!
//! Topology operations are room control-plane work: join, leave, publish,
//! subscribe and cleanup. Packet forwarding does not consult this module.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, OnceLock},
};

use o_sfu_router::{
    ConsumerCapability, ConsumerId as RouterConsumerId, ConsumerRouteState, MediaCapabilities,
    MediaKind as RouterMediaKind, ProducerId as RouterProducerId, ProducerRouteState, RouterId,
};

use super::{
    LocalRoomRouterPlacements, ResolvedPlacement,
    router_state::{RoomRouterState, RoomRouterStateError},
};
use crate::runtime::{UserId, router_events::RoomRouterEventSink};

mod shadow;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

use shadow::{ShadowSessionKey, ShadowSessionTracker};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) enum RoomTopologyError {
    /// A router-scoped operation targeted a router that is not attached.
    ///
    /// This indicates stale routed ids or an internal topology bug. Normal
    /// spillover attachment paths should attach a reserved router before
    /// routing work to it.
    MissingRouter { router_id: RouterId },
    /// A user-specific operation targeted a router that is missing.
    ///
    /// The error keeps the user id because missing router state during join or
    /// leave usually needs room-level context to diagnose. It does not imply
    /// the user is present on every other router.
    MissingRouterForSession {
        user_id: UserId,
        router_id: RouterId,
    },
    /// A user-scoped routing operation was requested before topology admitted
    /// that user.
    MissingSessionPlacement { user_id: UserId },
    /// The pure router rejected a topology mutation.
    ///
    /// Callers should treat this as a room-internal consistency failure. The
    /// topology layer has already resolved placement by the time this variant
    /// is produced.
    RouterState(RoomRouterStateError),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct RoomTopologyRepairReport {
    errors: Vec<RoomTopologyError>,
}

impl RoomTopologyRepairReport {
    fn record(&mut self, error: RoomTopologyError) {
        self.errors.push(error);
    }

    pub fn errors(&self) -> &[RoomTopologyError] {
        &self.errors
    }

    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

impl From<RoomRouterStateError> for RoomTopologyError {
    fn from(error: RoomRouterStateError) -> Self {
        Self::RouterState(error)
    }
}

/// Producer identity with the router placement needed to route later changes.
///
/// Room code stores this instead of a bare router producer ID so future
/// multi-router placement does not require guessing which router owns the
/// producer when activity changes or teardown arrives.
///
/// The router id is authoritative. A producer never moves between routers
/// after creation, because producer state drives dependent consumer state in
/// the pure router.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RoutedProducerId {
    router_id: RouterId,
    producer_id: RouterProducerId,
}

impl RoutedProducerId {
    #[must_use]
    pub(super) const fn new(router_id: RouterId, producer_id: RouterProducerId) -> Self {
        Self {
            router_id,
            producer_id,
        }
    }

    #[must_use]
    pub(super) const fn producer_id(self) -> RouterProducerId {
        self.producer_id
    }

    #[must_use]
    pub(super) const fn router_id(self) -> RouterId {
        self.router_id
    }
}

/// Consumer identity with the router placement needed to route later changes.
///
/// Consumer activity is controlled by the router that owns the source producer,
/// not necessarily by the consumer user's home router. Carrying the router ID
/// keeps that boundary explicit.
///
/// This is the main cross-router subscription rule: receiver transport
/// placement and consumer route ownership can differ. Room code must use this
/// routed id for later pause, resume and teardown operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RoutedConsumerId {
    router_id: RouterId,
    consumer_id: RouterConsumerId,
}

impl RoutedConsumerId {
    #[must_use]
    pub(super) const fn new(router_id: RouterId, consumer_id: RouterConsumerId) -> Self {
        Self {
            router_id,
            consumer_id,
        }
    }

    #[must_use]
    pub(super) const fn consumer_id(self) -> RouterConsumerId {
        self.consumer_id
    }

    #[must_use]
    pub(super) const fn router_id(self) -> RouterId {
        self.router_id
    }
}

/// Room-local routing over one or more pure router instances.
///
/// The topology records the attached router state for each live user, keeps
/// routed producer and consumer ids tied to their owning router and controls
/// when spillover routers are attached or detached. Room placement owns the
/// reserved router set and hands topology a resolved placement during join.
///
/// # Invariants
///
/// The primary router is always present. A home session can only point to an
/// attached router. Producers are created on their owner's home router.
/// Consumers are created on the producer's router, with a shadow receiver
/// session on that router when needed.
///
/// Cross-placement subscriptions split control ownership from packet movement:
///
/// ```text
/// Control topology:
///
///   User A home placement        User B home placement
///   router R0 / worker W0        router R1 / worker W1
///          |                            |
///          v                            v
///   R0 owns A producer           R1 owns B home session
///   R0 owns B->A consumer
///   R0 owns B shadow session
///
/// Packet path:
///
///   A RTP -> W0 packet loop -> W1 relay mailbox -> W1 packet loop -> B RTP
/// ```
///
/// `RoomTopology` owns the first graph. The transport worker manager owns the
/// second graph.
#[derive(Debug, Clone)]
pub(super) struct RoomTopology {
    /// Router that exists for the full room lifetime.
    primary_router: RouterId,
    /// Builder for router state attached after room construction.
    ///
    /// Lazy spillover attachment needs the same router event sink as the
    /// primary router. Keeping a factory here avoids giving room state direct
    /// access to router internals.
    router_state_factory: RoomRouterStateFactory,
    /// Currently attached pure router states.
    ///
    /// Idle spillover routers can be detached. The primary router is the only
    /// permanent entry.
    routers: BTreeMap<RouterId, RoomRouterState>,
    /// Authoritative home router for each live user.
    ///
    /// Home placement decides where a user publishes and which local media
    /// worker owns the corresponding transport session.
    session_home_router: BTreeMap<UserId, RouterId>,
    /// Stable router seed captured at join for each live user.
    ///
    /// Cross-router shadow sessions reuse this seed when materialized on a
    /// source router, so router-local session ids remain derived from the same
    /// user connection.
    session_seed_by_user: BTreeMap<UserId, u64>,
    /// Tracks cross-router receiver sessions that exist only to host consumer
    /// edges on a producer's source router.
    shadow_sessions: ShadowSessionTracker,
}

/// Factory for router states that need room-owned event sinks.
///
/// `RoomTopology` can attach spillover routers after placement. Each
/// new router state must still report router lifecycle events through the
/// room-owned sink, so the dependency is stored as a small cloneable factory
/// instead of being threaded through every topology call.
#[derive(Clone)]
pub(super) struct RoomRouterStateFactory {
    event_sink: Arc<dyn RoomRouterEventSink>,
}

impl fmt::Debug for RoomRouterStateFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomRouterStateFactory")
            .finish_non_exhaustive()
    }
}

impl RoomRouterStateFactory {
    #[must_use]
    pub(super) fn new(event_sink: Arc<dyn RoomRouterEventSink>) -> Self {
        Self { event_sink }
    }

    fn build_router_state(
        &self,
        router_id: RouterId,
        router_rtp_capabilities: MediaCapabilities,
    ) -> RoomRouterState {
        RoomRouterState::new(
            router_id,
            router_rtp_capabilities,
            Arc::clone(&self.event_sink),
        )
    }
}

impl RoomTopology {
    /// Build the room topology from the room's initial local placement.
    ///
    /// The first placement becomes the primary router and is attached
    /// immediately. Production rooms start without spillover placements.
    /// The returned topology owns router state only. It does not register users
    /// and it does not allocate transport sessions.
    pub(super) fn new_with_router_state_factory(
        local_routers: &LocalRoomRouterPlacements,
        router_rtp_capabilities: MediaCapabilities,
        router_state_factory: &RoomRouterStateFactory,
    ) -> Self {
        let primary_router_id = local_routers.primary().router;
        let mut routers = BTreeMap::new();
        routers.insert(
            primary_router_id,
            router_state_factory.build_router_state(primary_router_id, router_rtp_capabilities),
        );
        Self {
            primary_router: primary_router_id,
            router_state_factory: router_state_factory.clone(),
            routers,
            session_home_router: BTreeMap::new(),
            session_seed_by_user: BTreeMap::new(),
            shadow_sessions: ShadowSessionTracker::default(),
        }
    }

    /// Return the capability baseline exposed by the primary router.
    ///
    /// RTP capabilities are a room-level negotiation baseline, not a per-router
    /// load-balancing decision. Spillover routers clone the same baseline when
    /// attached so clients see one stable room capability surface.
    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        let Some(primary_router) = self.routers.get(&self.primary_router) else {
            return empty_capabilities();
        };
        primary_router.rtp_capabilities()
    }

    /// Ensure the user's home router has the transport-side router records.
    ///
    /// Call this after `ensure_session` when a join or reconnect has to make
    /// the user's pure router session ready for media transport work.
    pub(super) fn ensure_session_transports(
        &mut self,
        user_id: &UserId,
    ) -> Result<(), RoomTopologyError> {
        let router_id = self.require_home_router_id(user_id)?;
        self.router_mut_for_user(user_id, router_id)?
            .ensure_session_transports(user_id)?;
        Ok(())
    }

    pub(super) fn apply_client_join_on_placement(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
        placement: ResolvedPlacement,
    ) -> Result<(), RoomTopologyError> {
        self.ensure_session_on_placement(user_id, router_session_seed, placement)?;
        self.ensure_session_transports(user_id)?;
        Ok(())
    }

    /// Replace an existing user's topology session with a new connection seed.
    ///
    /// Replacement joins are not duplicate setup work: the old connection loses
    /// ownership and the new connection must be placed from its own seed so
    /// router placement and transport worker ownership remain aligned.
    pub(super) fn replace_client_session_on_placement(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
        placement: ResolvedPlacement,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> Result<(), RoomTopologyError> {
        self.remove_session(user_id, affected_consumers)?;
        self.apply_client_join_on_placement(user_id, router_session_seed, placement)
    }

    #[cfg(test)]
    pub(super) fn primary_router_id(&self) -> RouterId {
        self.primary_router
    }

    /// Attach one resolved placement if it is not already live.
    ///
    /// Attachment is idempotent and cold-path. The method clones the primary
    /// RTP capability baseline so a spillover router starts with the same
    /// negotiation surface as the rest of the room.
    pub(super) fn attach_placement(&mut self, placement: ResolvedPlacement) {
        let router_id = placement.router();
        if self.routers.contains_key(&router_id) {
            return;
        }
        let router_rtp_capabilities = self.rtp_capabilities().clone();
        self.routers.insert(
            router_id,
            self.router_state_factory
                .build_router_state(router_id, router_rtp_capabilities),
        );
    }

    /// Add a producer on the publisher's home router.
    ///
    /// Producer ownership is intentionally tied to the publisher's home router.
    /// Dependent consumer state and producer route-state propagation should
    /// stay in that pure router instance for the producer lifetime.
    pub(super) fn add_producer(
        &mut self,
        user_id: &UserId,
        media_kind: RouterMediaKind,
    ) -> Result<RoutedProducerId, RoomTopologyError> {
        let router_id = self.require_home_router_id(user_id)?;
        let producer_id = self
            .router_mut_for_user(user_id, router_id)?
            .add_producer(user_id, media_kind)?;
        let routed_producer_id = RoutedProducerId::new(router_id, producer_id);
        Ok(routed_producer_id)
    }

    /// Add a consumer route on the source producer's router.
    ///
    /// If the receiver's home router differs from the producer router, the
    /// topology first materializes a shadow receiver session on the producer
    /// router. That keeps producer pause propagation and consumer teardown in
    /// the same pure router instance that owns the source.
    #[cfg(test)]
    pub(super) fn add_consumer(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
    ) -> Result<RoutedConsumerId, RoomTopologyError> {
        self.add_consumer_with_route_state(
            consumer_user_id,
            producer_id,
            media_kind,
            capability,
            ConsumerRouteState::Active,
        )
    }

    pub(super) fn add_consumer_with_route_state(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
        route_state: ConsumerRouteState,
    ) -> Result<RoutedConsumerId, RoomTopologyError> {
        let receiver_session =
            self.ensure_session_on_router(consumer_user_id, producer_id.router_id())?;
        let consumer_result = self
            .router_mut(producer_id.router_id())?
            .add_consumer_with_route_state(
                consumer_user_id,
                producer_id.producer_id(),
                media_kind,
                capability,
                route_state,
            );
        let consumer_id = match consumer_result {
            Ok(consumer_id) => consumer_id,
            Err(error) => {
                if receiver_session.created_untracked_shadow {
                    self.router_mut_for_user(consumer_user_id, producer_id.router_id())?
                        .remove_session(consumer_user_id)?;
                }
                return Err(error.into());
            }
        };
        let routed_consumer_id = RoutedConsumerId::new(producer_id.router_id(), consumer_id);
        self.shadow_sessions
            .register_consumer(routed_consumer_id, receiver_session.shadow_key);
        Ok(routed_consumer_id)
    }

    /// Forward a producer route-state change to the router that owns it.
    ///
    /// The topology layer only resolves placement. The pure router remains the
    /// owner of producer shadow propagation to dependent consumers.
    pub(super) fn set_producer_route_state(
        &mut self,
        producer_id: RoutedProducerId,
        route_state: ProducerRouteState,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(producer_id.router_id())?
            .set_producer_route_state(producer_id.producer_id(), route_state)?;
        Ok(())
    }

    /// Forward a consumer-local route-state change to the router that owns it.
    ///
    /// The routed consumer ID is authoritative for placement because consumer
    /// edges are created on the source producer's router.
    pub(super) fn set_consumer_route_state(
        &mut self,
        consumer_id: RoutedConsumerId,
        route_state: ConsumerRouteState,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(consumer_id.router_id())?
            .set_consumer_route_state(consumer_id.consumer_id(), route_state)?;
        Ok(())
    }

    /// Remove a routed producer and reconcile the shadows of dependent
    /// cross-router consumers.
    ///
    /// The pure router already removes dependent consumers when a producer is
    /// removed. The shadow tracker mirrors that derived ownership so source
    /// teardown cannot leave receiver shadows behind on the source router.
    pub(super) fn remove_producer(
        &mut self,
        producer_id: RoutedProducerId,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(producer_id.router_id())?
            .remove_producer(producer_id.producer_id())?;
        let shadow_sessions = self
            .shadow_sessions
            .unregister_consumers(affected_consumers);
        self.prune_shadow_sessions(shadow_sessions)?;
        Ok(())
    }

    /// Remove the user's router records from all attached routers.
    ///
    /// The home router must still be attached when removal starts. If it is
    /// missing, the topology has already lost authoritative placement for the
    /// user and the caller receives `MissingRouterForSession`.
    ///
    /// Idle spillover cleanup is reconciled by room policy after the state
    /// transition commits. Transport session cleanup remains owned by the room
    /// effect layer.
    pub(super) fn remove_session(
        &mut self,
        user_id: &UserId,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> Result<(), RoomTopologyError> {
        let home_router_id = self.require_home_router_id(user_id)?;
        if !self.routers.contains_key(&home_router_id) {
            return Err(RoomTopologyError::MissingRouterForSession {
                user_id: user_id.clone(),
                router_id: home_router_id,
            });
        }
        let router_ids = self.routers.keys().copied().collect::<Vec<_>>();
        for router_id in router_ids {
            self.router_mut_for_user(user_id, router_id)?
                .remove_session(user_id)?;
        }
        let shadow_sessions = self
            .shadow_sessions
            .unregister_consumers(affected_consumers);
        self.prune_shadow_sessions(shadow_sessions)?;
        self.session_home_router.remove(user_id);
        self.session_seed_by_user.remove(user_id);
        Ok(())
    }

    pub(super) fn remove_session_repairing(
        &mut self,
        user_id: &UserId,
        affected_consumers: impl IntoIterator<Item = RoutedConsumerId>,
    ) -> RoomTopologyRepairReport {
        let mut report = RoomTopologyRepairReport::default();
        match self.require_home_router_id(user_id) {
            Ok(home_router_id) if !self.routers.contains_key(&home_router_id) => {
                report.record(RoomTopologyError::MissingRouterForSession {
                    user_id: user_id.clone(),
                    router_id: home_router_id,
                });
            }
            Err(error) => report.record(error),
            Ok(_) => {}
        }
        let router_ids = self.routers.keys().copied().collect::<Vec<_>>();
        for router_id in router_ids {
            let removal = self
                .router_mut_for_user(user_id, router_id)
                .and_then(|router| router.remove_session_repairing(user_id).map_err(Into::into));
            if let Err(error) = removal {
                report.record(error);
            }
        }
        let shadow_sessions = self
            .shadow_sessions
            .unregister_consumers(affected_consumers);
        self.prune_shadow_sessions_repairing(shadow_sessions, &mut report);
        self.session_home_router.remove(user_id);
        self.session_seed_by_user.remove(user_id);
        report
    }

    /// Return the user's home router after topology admission.
    fn require_home_router_id(&self, user_id: &UserId) -> Result<RouterId, RoomTopologyError> {
        self.session_home_router
            .get(user_id)
            .copied()
            .ok_or_else(|| RoomTopologyError::MissingSessionPlacement {
                user_id: user_id.clone(),
            })
    }

    fn ensure_session_on_placement(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
        placement: ResolvedPlacement,
    ) -> Result<(), RoomTopologyError> {
        if let Some(router_id) = self.session_home_router.get(user_id).copied() {
            let router = self.router_mut_for_user(user_id, router_id)?;
            router.ensure_session(user_id, router_session_seed)?;
            return Ok(());
        }
        let router_id = placement.router();
        self.attach_placement(placement);
        let router = self.router_mut_for_user(user_id, router_id)?;
        router.ensure_session(user_id, router_session_seed)?;
        self.session_home_router.insert(user_id.clone(), router_id);
        self.session_seed_by_user
            .insert(user_id.clone(), router_session_seed);
        Ok(())
    }

    /// Ensure the user exists on a specific router.
    ///
    /// This is used for cross-router consumers. The target router is the
    /// source producer's router, not necessarily the receiver's home router.
    /// The created session is a router-local shadow that lets the pure router
    /// own the consumer edge next to the producer.
    fn ensure_session_on_router(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<ReceiverRouterSession, RoomTopologyError> {
        let Some(router_session_seed) = self.session_seed_by_user.get(user_id).copied() else {
            return Err(RoomTopologyError::MissingSessionPlacement {
                user_id: user_id.clone(),
            });
        };
        let home_router_id = self.require_home_router_id(user_id)?;
        let shadow_key = (home_router_id != router_id)
            .then(|| ShadowSessionKey::new(router_id, user_id.clone()));
        if !self.routers.contains_key(&router_id) {
            return Err(RoomTopologyError::MissingRouter { router_id });
        }
        let created_untracked_shadow = shadow_key
            .as_ref()
            .is_some_and(|key| !self.shadow_sessions.contains_shadow_session(key));
        self.router_mut_for_user(user_id, router_id)?
            .ensure_session(user_id, router_session_seed)?;
        if let Err(error) = self
            .router_mut_for_user(user_id, router_id)?
            .ensure_session_transports(user_id)
        {
            if created_untracked_shadow {
                self.router_mut_for_user(user_id, router_id)?
                    .remove_session(user_id)?;
            }
            return Err(error.into());
        }
        Ok(ReceiverRouterSession {
            shadow_key,
            created_untracked_shadow,
        })
    }

    /// Remove receiver shadows that no routed consumer edge still justifies.
    ///
    /// This is topology cleanup only. The room membership and media indexes
    /// remain authoritative for live users, sources and subscriptions. A pruned
    /// shadow removes only the receiver's router-local records on the source
    /// router.
    fn prune_shadow_sessions(
        &mut self,
        shadow_sessions: BTreeSet<ShadowSessionKey>,
    ) -> Result<(), RoomTopologyError> {
        for shadow_session in shadow_sessions {
            self.router_mut_for_user(shadow_session.user_id(), shadow_session.router_id())?
                .remove_session(shadow_session.user_id())?;
        }
        Ok(())
    }

    fn prune_shadow_sessions_repairing(
        &mut self,
        shadow_sessions: BTreeSet<ShadowSessionKey>,
        report: &mut RoomTopologyRepairReport,
    ) {
        for shadow_session in shadow_sessions {
            let removal = self
                .router_mut_for_user(shadow_session.user_id(), shadow_session.router_id())
                .and_then(|router| {
                    router
                        .remove_session_repairing(shadow_session.user_id())
                        .map_err(Into::into)
                });
            if let Err(error) = removal {
                report.record(error);
            }
        }
    }

    /// Return idle spillover routers that may be detached by room policy.
    ///
    /// Only attached non-primary routers are candidates. Shadow sessions do not
    /// make a router idle, so cross-router routes can finish cleanup before a
    /// delayed detach removes the attached router state.
    pub(in crate::runtime::room) fn idle_spillover_routers(&self) -> Vec<RouterId> {
        let active_home_routers = self
            .session_home_router
            .values()
            .copied()
            .collect::<BTreeSet<_>>();
        self.routers
            .iter()
            .filter_map(|(router_id, router)| {
                if *router_id == self.primary_router || active_home_routers.contains(router_id) {
                    return None;
                }
                (router.mapped_session_count() == 0).then_some(*router_id)
            })
            .collect()
    }

    /// Drop explicitly selected idle spillover router state.
    pub(in crate::runtime::room) fn detach_spillover_routers(&mut self, router_ids: &[RouterId]) {
        for router_id in router_ids {
            if *router_id == self.primary_router {
                continue;
            }
            self.routers.remove(router_id);
        }
    }

    fn router_mut(
        &mut self,
        router_id: RouterId,
    ) -> Result<&mut RoomRouterState, RoomTopologyError> {
        self.routers
            .get_mut(&router_id)
            .ok_or(RoomTopologyError::MissingRouter { router_id })
    }

    fn router_mut_for_user(
        &mut self,
        user_id: &UserId,
        router_id: RouterId,
    ) -> Result<&mut RoomRouterState, RoomTopologyError> {
        self.routers
            .get_mut(&router_id)
            .ok_or_else(|| RoomTopologyError::MissingRouterForSession {
                user_id: user_id.clone(),
                router_id,
            })
    }
}

/// Result of materializing a receiver on the source router for consumer setup.
///
/// A cross-router consumer needs a router-local receiver session before the
/// pure router can create the consumer edge. If the edge creation later fails,
/// `created_untracked_shadow` tells `RoomTopology` whether it may remove the
/// newly materialized shadow without disrupting an older tracked edge.
#[derive(Debug, Clone)]
struct ReceiverRouterSession {
    /// Shadow identity when the receiver home router differs from the source router.
    shadow_key: Option<ShadowSessionKey>,
    /// Whether this call created a shadow not yet owned by the tracker.
    created_untracked_shadow: bool,
}

fn empty_capabilities() -> &'static MediaCapabilities {
    static EMPTY: OnceLock<MediaCapabilities> = OnceLock::new();
    EMPTY.get_or_init(|| MediaCapabilities::new(Vec::new(), Vec::new()))
}
