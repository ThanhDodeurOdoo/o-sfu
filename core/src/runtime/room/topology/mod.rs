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
//! A room has one primary router and an immutable set of reserved local router
//! placements. Strict policy only uses the primary router. Bounded local
//! spillover may place home sessions on any reserved placement. Spillover router
//! state is attached lazily when a placed session or cross-router consumer needs
//! it, then detached once no live home session remains there.
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
    sync::{Arc, OnceLock},
};

use o_sfu_router::{
    ConsumerCapability, ConsumerId as RouterConsumerId, ConsumerRouteState, MediaCapabilities,
    MediaKind as RouterMediaKind, ProducerId as RouterProducerId, ProducerRouteState, RouterId,
};

use super::{
    LocalRoomRouterPlacements,
    router_state::{RoomRouterState, RoomRouterStateError},
};
use crate::{
    RoomShardingPolicy,
    runtime::{UserId, recording::RecordingService},
};

mod policy;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

#[cfg(test)]
pub(in crate::runtime::room) use policy::LoadPressureReason;
pub(in crate::runtime::room) use policy::TopologyPressureSnapshot;
use policy::{CleanupInput, HomePlacementInput, PlacementPolicy};

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
    /// A topology mutation targeted a router outside this room's reserved
    /// process-local placement set.
    UnreservedRouter { router_id: RouterId },
    /// The pure router rejected a topology mutation.
    ///
    /// Callers should treat this as a room-internal consistency failure. The
    /// topology layer has already resolved placement by the time this variant
    /// is produced.
    RouterState(RoomRouterStateError),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Room-local routing placement over one or more pure router instances.
///
/// The topology is authoritative for router placement inside one room. It
/// records the home router for each live user, keeps routed producer and
/// consumer ids tied to their owning router and controls when spillover routers
/// are attached or detached.
///
/// # What belongs here
///
/// This type owns pure router state and placement maps. It does not own room
/// membership, compatibility user identity, transport sessions or network I/O.
/// Room state decides that a user, source or subscription exists. Topology
/// decides which router instance should model the corresponding router records.
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
/// `RoomTopology` owns the first graph. The transport shard set owns the
/// second graph.
#[derive(Debug, Clone)]
pub(super) struct RoomTopology {
    /// Router that exists for the full room lifetime.
    primary_router: RouterId,
    /// Immutable policy copied from room creation.
    ///
    /// The policy decides whether user home placement may use the reserved
    /// spillover set. It is never consulted by packet forwarding.
    placement_policy: PlacementPolicy,
    /// Immutable local router placements reserved for this room.
    ///
    /// The order must match `RoomDefinition` transport placement. Connection
    /// seeds are mapped by index in both places.
    local_routers: LocalRoomRouterPlacements,
    /// Builder for router state attached after room construction.
    ///
    /// Lazy spillover attachment needs the same recording observer wiring as
    /// the primary router. Keeping a factory here avoids giving room state
    /// direct access to router internals.
    router_observer_factory: RoomRouterObserverFactory,
    /// Currently attached pure router states.
    ///
    /// This map can be smaller than `local_routers` because idle spillover
    /// routers are detached. The primary router is the only permanent entry.
    routers: BTreeMap<RouterId, RoomRouterState>,
    /// Attached-router membership class used for cleanup decisions.
    router_memberships: BTreeMap<RouterId, RouterMembershipState>,
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
}

/// Attachment role for one router inside a room topology.
///
/// Membership is separate from the router map because detach decisions care
/// about why the router is present, not just whether it has state. The primary
/// router cannot be detached. Spillover routers can be removed after their last
/// home session leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouterMembershipState {
    /// Permanent router for the room.
    Primary,
    /// Lazily attached router from the reserved local spillover set.
    ActiveSpillover,
}

/// Factory for router states that need room-owned observers.
///
/// `RoomTopology` can attach spillover routers after room construction. Each
/// new router state must still report recording observer events through the
/// room's recording service, so the observer dependency is stored as a small
/// cloneable factory instead of being threaded through every topology call.
#[derive(Debug, Clone)]
pub(super) struct RoomRouterObserverFactory {
    recording_service: Arc<RecordingService>,
}

impl RoomRouterObserverFactory {
    #[must_use]
    pub(super) fn new(recording_service: Arc<RecordingService>) -> Self {
        Self { recording_service }
    }

    fn build_router_state(
        &self,
        router_id: RouterId,
        router_rtp_capabilities: MediaCapabilities,
    ) -> RoomRouterState {
        RoomRouterState::new_with_recording_service(
            router_id,
            router_rtp_capabilities,
            Arc::clone(&self.recording_service),
        )
    }
}

impl RoomTopology {
    /// Build the room topology from its reserved local placements.
    ///
    /// The first placement becomes the primary router and is attached
    /// immediately. Additional placements remain reserved but detached until
    /// bounded spillover places a home session there or a cross-router
    /// consumer needs a shadow session.
    ///
    /// The returned topology owns router state only. It does not register users
    /// and it does not allocate transport sessions.
    pub(super) fn new_with_recording_observer_factory(
        local_routers: LocalRoomRouterPlacements,
        room_sharding_policy: RoomShardingPolicy,
        router_rtp_capabilities: MediaCapabilities,
        router_observer_factory: &RoomRouterObserverFactory,
    ) -> Self {
        let primary_router_id = local_routers.primary().router;
        let mut routers = BTreeMap::new();
        routers.insert(
            primary_router_id,
            router_observer_factory.build_router_state(primary_router_id, router_rtp_capabilities),
        );
        let mut router_memberships = BTreeMap::new();
        router_memberships.insert(primary_router_id, RouterMembershipState::Primary);
        Self {
            primary_router: primary_router_id,
            placement_policy: PlacementPolicy::new(room_sharding_policy),
            local_routers,
            router_observer_factory: router_observer_factory.clone(),
            routers,
            router_memberships,
            session_home_router: BTreeMap::new(),
            session_seed_by_user: BTreeMap::new(),
        }
    }

    /// Return the capability baseline exposed by the primary router.
    ///
    /// RTP capabilities are a room-level negotiation baseline, not a per-router
    /// load-balancing decision. Spillover routers clone the same baseline when
    /// attached so clients see one stable room capability surface.
    pub(super) fn rtp_capabilities(&self) -> &MediaCapabilities {
        let Some(primary_router) = self.routers.get(&self.primary_router) else {
            return empty_router_capabilities();
        };
        primary_router.rtp_capabilities()
    }

    /// Ensure the user has a home session in the topology.
    ///
    /// The first call chooses the home router from the room's placement policy
    /// and stores it. Later calls keep the existing home router so reconnect or
    /// duplicate setup work cannot move a live user between routers.
    ///
    /// The seed is also retained for shadow sessions created by cross-router
    /// subscriptions. This keeps all router-local records for one connection
    /// tied to the same deterministic session seed.
    pub(super) fn ensure_session_with_pressure(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
        pressure: TopologyPressureSnapshot,
    ) -> Result<(), RoomTopologyError> {
        if let Some(router_id) = self.session_home_router.get(user_id).copied() {
            self.attach_router(router_id)?;
            let router = self.router_mut_for_user(user_id, router_id)?;
            router.ensure_session(user_id, router_session_seed)?;
            return Ok(());
        }
        let router_id = self.router_id_for_connection(router_session_seed, pressure);
        self.attach_router(router_id)?;
        let router = self.router_mut_for_user(user_id, router_id)?;
        router.ensure_session(user_id, router_session_seed)?;
        self.session_home_router.insert(user_id.clone(), router_id);
        self.session_seed_by_user
            .insert(user_id.clone(), router_session_seed);
        Ok(())
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

    pub(super) fn apply_client_join_with_pressure(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
        pressure: TopologyPressureSnapshot,
    ) -> Result<(), RoomTopologyError> {
        self.ensure_session_with_pressure(user_id, router_session_seed, pressure)?;
        self.ensure_session_transports(user_id)?;
        Ok(())
    }

    /// Replace an existing user's topology session with a new connection seed.
    ///
    /// Replacement joins are not duplicate setup work: the old connection loses
    /// ownership and the new connection must be placed from its own seed so
    /// router placement and transport worker ownership remain aligned.
    pub(super) fn replace_client_session_with_pressure(
        &mut self,
        user_id: &UserId,
        router_session_seed: u64,
        pressure: TopologyPressureSnapshot,
    ) -> Result<(), RoomTopologyError> {
        self.remove_session(user_id)?;
        self.apply_client_join_with_pressure(user_id, router_session_seed, pressure)
    }

    pub(super) fn home_placement_for_user(
        &self,
        user_id: &UserId,
    ) -> Option<super::LocalRouterRuntimeContext> {
        let router_id = self.session_home_router.get(user_id).copied()?;
        self.local_routers
            .iter()
            .find(|placement| placement.router == router_id)
    }

    /// Attach one reserved router if it is not already live.
    ///
    /// Attachment is idempotent and cold-path. The method clones the primary
    /// RTP capability baseline so a spillover router starts with the same
    /// negotiation surface as the rest of the room.
    pub(super) fn attach_router(&mut self, router_id: RouterId) -> Result<(), RoomTopologyError> {
        if !self.local_routers.contains_router(router_id) {
            return Err(RoomTopologyError::UnreservedRouter { router_id });
        }
        if self.routers.contains_key(&router_id) {
            return Ok(());
        }
        let router_rtp_capabilities = self.rtp_capabilities().clone();
        self.routers.insert(
            router_id,
            self.router_observer_factory
                .build_router_state(router_id, router_rtp_capabilities),
        );
        self.router_memberships
            .insert(router_id, RouterMembershipState::ActiveSpillover);
        Ok(())
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
        Ok(RoutedProducerId::new(router_id, producer_id))
    }

    /// Add a consumer route on the source producer's router.
    ///
    /// If the receiver's home router differs from the producer router, the
    /// topology first materializes a shadow receiver session on the producer
    /// router. That keeps producer pause propagation and consumer teardown in
    /// the same pure router instance that owns the source.
    pub(super) fn add_consumer(
        &mut self,
        consumer_user_id: &UserId,
        producer_id: RoutedProducerId,
        media_kind: RouterMediaKind,
        capability: ConsumerCapability,
    ) -> Result<RoutedConsumerId, RoomTopologyError> {
        self.ensure_session_on_router(consumer_user_id, producer_id.router_id())?;
        let consumer_id = self.router_mut(producer_id.router_id())?.add_consumer(
            consumer_user_id,
            producer_id.producer_id(),
            media_kind,
            capability,
        )?;
        Ok(RoutedConsumerId::new(producer_id.router_id(), consumer_id))
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

    pub(super) fn remove_consumer(
        &mut self,
        consumer_id: RoutedConsumerId,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(consumer_id.router_id())?
            .remove_consumer(consumer_id.consumer_id())?;
        Ok(())
    }

    pub(super) fn remove_producer(
        &mut self,
        producer_id: RoutedProducerId,
    ) -> Result<(), RoomTopologyError> {
        self.router_mut(producer_id.router_id())?
            .remove_producer(producer_id.producer_id())?;
        Ok(())
    }

    /// Remove the user's router records from all attached routers.
    ///
    /// The home router must still be attached when removal starts. If it is
    /// missing, the topology has already lost authoritative placement for the
    /// user and the caller receives `MissingRouterForSession`.
    ///
    /// After successful removal, idle spillover routers are detached. This is
    /// a cleanup step for local topology state only. Transport session cleanup
    /// remains owned by the room effect layer.
    pub(super) fn remove_session(&mut self, user_id: &UserId) -> Result<(), RoomTopologyError> {
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
        self.session_home_router.remove(user_id);
        self.session_seed_by_user.remove(user_id);
        self.detach_idle_spillover_routers();
        Ok(())
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

    /// Map a connection seed onto the room's allowed local router set.
    ///
    /// The seed is derived from the room connection id by the caller. Using the
    /// seed instead of user identity lets reconnects receive fresh placement
    /// while keeping placement deterministic inside one join transition.
    fn router_id_for_connection(
        &mut self,
        router_session_seed: u64,
        pressure: TopologyPressureSnapshot,
    ) -> RouterId {
        let decision = self
            .placement_policy
            .choose_home_router(HomePlacementInput {
                connection_seed: router_session_seed,
                reserved_router_count: self.local_routers.len(),
                pressure,
            });
        let router_index = decision.router_index().min(self.local_routers.len() - 1);
        self.local_routers
            .get(router_index)
            .map_or(self.primary_router, |placement| placement.router)
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
    ) -> Result<(), RoomTopologyError> {
        let Some(router_session_seed) = self.session_seed_by_user.get(user_id).copied() else {
            return Err(RoomTopologyError::MissingSessionPlacement {
                user_id: user_id.clone(),
            });
        };
        if !self.session_home_router.contains_key(user_id) {
            return Err(RoomTopologyError::MissingSessionPlacement {
                user_id: user_id.clone(),
            });
        }
        self.attach_router(router_id)?;
        self.router_mut_for_user(user_id, router_id)?
            .ensure_session(user_id, router_session_seed)?;
        self.router_mut_for_user(user_id, router_id)?
            .ensure_session_transports(user_id)?;
        Ok(())
    }

    /// Drop idle spillover router state after home sessions leave.
    ///
    /// Only active-spillover routers are candidates. Shadow sessions do not
    /// keep a router attached after the last home session leaves because they
    /// should have been removed with the source or receiver room state before
    /// leave cleanup reaches this point.
    fn detach_idle_spillover_routers(&mut self) {
        let active_home_routers = self
            .session_home_router
            .values()
            .copied()
            .collect::<BTreeSet<_>>();
        let occupied_router_count = self.occupied_router_count(&active_home_routers);
        let keep_active_router_count = self
            .placement_policy
            .active_router_count_to_keep_after_cleanup(CleanupInput {
                reserved_router_count: self.local_routers.len(),
                occupied_router_count,
                pressure: TopologyPressureSnapshot::default(),
            });
        let removable_router_ids = self
            .router_memberships
            .iter()
            .filter_map(|(router_id, membership)| {
                (*membership == RouterMembershipState::ActiveSpillover
                    && !active_home_routers.contains(router_id)
                    && self.router_index(*router_id) >= keep_active_router_count)
                    .then_some(*router_id)
            })
            .collect::<Vec<_>>();
        for router_id in removable_router_ids {
            self.routers.remove(&router_id);
            self.router_memberships.remove(&router_id);
        }
    }

    fn occupied_router_count(&self, active_home_routers: &BTreeSet<RouterId>) -> usize {
        active_home_routers
            .iter()
            .filter_map(|router_id| self.local_router_index(*router_id))
            .max()
            .map_or(1, |index| index.saturating_add(1))
    }

    fn router_index(&self, router_id: RouterId) -> usize {
        self.local_router_index(router_id).unwrap_or(0)
    }

    fn local_router_index(&self, router_id: RouterId) -> Option<usize> {
        self.local_routers
            .iter()
            .position(|placement| placement.router == router_id)
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

fn empty_router_capabilities() -> &'static MediaCapabilities {
    static EMPTY: OnceLock<MediaCapabilities> = OnceLock::new();
    EMPTY.get_or_init(|| MediaCapabilities::new(Vec::new(), Vec::new()))
}
