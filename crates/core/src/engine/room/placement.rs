//! join-time placement planning for room-local media workers
//!
//! [`crate::engine::room::manager::RoomManager::join_user`] uses this module
//! after admission has selected a live room but before the membership transition
//! commits
//!
//! the planner ranks workers from committed room state plus transport pressure
//! snapshots, then returns a decision that the manager resolves into a concrete
//! placement
//!
//! this is cold-path control-plane work
//! the packet loop only sees the resolved transport keys after the join has
//! committed
//!
//! joins pass through three placement shapes:
//!
//! ```text
//! room snapshot -> planned decision -> resolved placement -> committed mapping
//! ```
//!
//! the manager can compute a [`RoomPlacementDecision`] before it takes the room
//! write lock
//! [`JoinPlacementPlan::resolve_for_commit`] converts that decision into a
//! concrete [`ResolvedPlacement`] while the membership transition is being
//! applied
//! [`RoomPlacementState::register_committed_placement`] records the final mapping
//! only after the join is accepted

use std::{collections::BTreeMap, iter, sync::Mutex};

use o_sfu_router::RouterId;

use crate::{
    LocalSpilloverPolicy, RoomSpilloverMode, RoomWorkerPolicy,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::{
            TransportPlacementPressureSnapshot, TransportSessionKey,
            TransportWorkerPressureSnapshot,
        },
        sync::lock_unpoisoned,
    },
};

/// runtime placement seed passed into a room at construction
///
/// a production room is created with a stable [`RoomInstanceId`] and primary
/// [`RouterId`], but worker identity may not exist until the first join commits
/// placement
/// tests and explicit runtime builders may still pass a complete placement set
/// when they need a pre-assigned room
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRuntimeContext {
    /// live instance id used for transport keys, diagnostics and teardown
    instance: RoomInstanceId,
    /// primary router reserved before a worker assignment exists
    primary_router: RouterId,
    /// pre-existing local placements for rooms that start already assigned
    initial_local_router_placements: Option<LocalRoomRouterPlacements>,
}

/// one room-local router placement and its owning media worker
///
/// a placement is the unit that room membership, transport routing and local
/// spillover agree on
/// the router side identifies the room-local routing graph
/// while [`MediaWorkerId`] identifies the local rtc worker that should host the
/// transport session for users placed there
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRouterRuntimeContext {
    /// router id used inside the room topology
    pub router: RouterId,
    /// rtc media worker used for transport sessions placed here
    pub media_worker: MediaWorkerId,
}

/// non-empty router placement set assigned to one live room
///
/// the primary placement is stored with spillover placements so callers cannot
/// express a primary router that disagrees with the placement list
/// a value of this type means at least one real [`MediaWorkerId`] has been
/// assigned
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRoomRouterPlacements {
    /// first placement used by strict rooms and empty-room reuse
    primary: LocalRouterRuntimeContext,
    /// extra local routers attached by bounded or load-triggered spillover
    spillover: Vec<LocalRouterRuntimeContext>,
}

/// failure to build [`LocalRoomRouterPlacements`] from an unchecked list
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRoomRouterPlacementsError {
    /// no placement was supplied, so there is no primary router
    Empty,
}

impl LocalRoomRouterPlacements {
    /// creates a placement set from an explicit primary placement
    ///
    /// callers should use this when the primary router is already known
    /// use [`Self::try_from_vec`] for unchecked primary-first lists
    #[must_use]
    pub fn new(
        primary: LocalRouterRuntimeContext,
        spillover: Vec<LocalRouterRuntimeContext>,
    ) -> Self {
        Self { primary, spillover }
    }

    /// builds a placement set from the primary-first runtime placement list
    ///
    /// # Errors
    ///
    /// returns [`LocalRoomRouterPlacementsError::Empty`] when no primary
    /// placement is supplied
    pub fn try_from_vec(
        placements: Vec<LocalRouterRuntimeContext>,
    ) -> Result<Self, LocalRoomRouterPlacementsError> {
        let mut placements = placements.into_iter();
        let Some(primary) = placements.next() else {
            return Err(LocalRoomRouterPlacementsError::Empty);
        };
        Ok(Self::new(primary, placements.collect()))
    }

    /// returns the placement used when a room has no connection-specific mapping
    #[must_use]
    pub const fn primary(&self) -> LocalRouterRuntimeContext {
        self.primary
    }

    /// records a placement without duplicating a router entry
    ///
    /// join finalization uses this after resolving a placement
    /// if the router is already known, the worker mapping is replaced so stale
    /// test fixtures or recovered runtime state cannot leave two entries for the
    /// same router
    pub(in crate::engine::room) fn upsert(&mut self, placement: LocalRouterRuntimeContext) {
        if self.primary.router == placement.router {
            self.primary = placement;
            return;
        }
        if let Some(existing) = self
            .spillover
            .iter_mut()
            .find(|existing| existing.router == placement.router)
        {
            *existing = placement;
            return;
        }
        self.spillover.push(placement);
    }

    /// yields primary first, then spillover placements in allocation order
    pub fn iter(&self) -> impl Iterator<Item = LocalRouterRuntimeContext> + '_ {
        iter::once(self.primary).chain(self.spillover.iter().copied())
    }
}

impl RoomRuntimeContext {
    /// builds a context for rooms that already have a primary worker placement
    ///
    /// this is used by tests and integration paths that need explicit placement
    /// normal room creation should use the unassigned constructor so it does not
    /// invent a worker id before the first join
    #[must_use]
    pub fn new(
        instance: RoomInstanceId,
        primary: LocalRouterRuntimeContext,
        spillover: Vec<LocalRouterRuntimeContext>,
    ) -> Self {
        Self {
            instance,
            primary_router: primary.router,
            initial_local_router_placements: Some(LocalRoomRouterPlacements::new(
                primary, spillover,
            )),
        }
    }

    /// builds a context for a newly allocated production room
    ///
    /// the primary router is reserved immediately so topology can be initialized,
    /// but no [`MediaWorkerId`] is available until the join planner commits the
    /// first session placement
    #[must_use]
    pub(in crate::engine::room) const fn new_unassigned(
        instance: RoomInstanceId,
        primary_router: RouterId,
    ) -> Self {
        Self {
            instance,
            primary_router,
            initial_local_router_placements: None,
        }
    }

    /// builds a context from a primary-first placement list
    ///
    /// # Errors
    ///
    /// returns [`LocalRoomRouterPlacementsError::Empty`] when the placement
    /// list has no primary router
    pub fn try_from_placements(
        instance: RoomInstanceId,
        placements: Vec<LocalRouterRuntimeContext>,
    ) -> Result<Self, LocalRoomRouterPlacementsError> {
        let local_routers = LocalRoomRouterPlacements::try_from_vec(placements)?;
        Ok(Self {
            instance,
            primary_router: local_routers.primary().router,
            initial_local_router_placements: Some(local_routers),
        })
    }

    /// returns the live room instance id used by runtime subsystems
    #[must_use]
    pub const fn instance(&self) -> RoomInstanceId {
        self.instance
    }

    /// returns the router reserved for the primary room topology
    #[must_use]
    pub const fn primary_router(&self) -> RouterId {
        self.primary_router
    }

    /// returns pre-assigned placements when construction supplied them
    ///
    /// `None` means the room has a primary router but no committed worker yet
    #[must_use]
    pub(in crate::engine::room) fn initial_local_router_placements(
        &self,
    ) -> Option<&LocalRoomRouterPlacements> {
        self.initial_local_router_placements.as_ref()
    }
}

/// committed room-local placement state
///
/// joins register the selected router and media worker for each committed
/// connection
/// later transport commands use the same mapping to build stable session keys
/// without deriving placement from topology internals
#[derive(Debug)]
pub(super) struct RoomPlacementState {
    instance_id: RoomInstanceId,
    inner: Mutex<RoomPlacementStateInner>,
}

#[derive(Debug)]
struct RoomPlacementStateInner {
    /// primary router kept even before worker placement exists
    primary_router: RouterId,
    /// local placements after the first committed assignment
    local_routers: Option<LocalRoomRouterPlacements>,
    /// connection-specific placements that override primary fallback
    placement_by_connection: BTreeMap<ConnectionId, LocalRouterRuntimeContext>,
}

/// placement data returned by a committed join transition
///
/// async finalization uses this receipt instead of rebuilding transport keys
/// from topology state after the room lock has been released
/// the receipt only exists for accepted joins
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommittedPlacementReceipt {
    /// connection accepted by the membership transition
    connection_id: ConnectionId,
    /// placement recorded for that connection
    placement: LocalRouterRuntimeContext,
    /// transport key derived from the committed placement
    transport_session_key: TransportSessionKey,
}

/// concrete placement selected for a join but not yet recorded
///
/// this separates planning from mutation
/// the manager can carry a plan across async work, then membership resolves it
/// under the room write lock and records the final placement only if the state
/// transition succeeds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::room) struct ResolvedPlacement(LocalRouterRuntimeContext);

impl RoomPlacementState {
    /// creates the mutable placement map for one room
    ///
    /// `local_routers` is present only for contexts that started with an explicit
    /// worker placement
    /// production rooms normally start without one and commit the first
    /// [`LocalRoomRouterPlacements`] during join finalization
    #[must_use]
    pub(super) fn new(
        instance_id: RoomInstanceId,
        primary_router: RouterId,
        local_routers: Option<LocalRoomRouterPlacements>,
    ) -> Self {
        Self {
            instance_id,
            inner: Mutex::new(RoomPlacementStateInner {
                primary_router,
                local_routers,
                placement_by_connection: BTreeMap::new(),
            }),
        }
    }

    /// builds the transport key for a live connection
    ///
    /// callers must only ask for keys after the connection has either a committed
    /// placement or can validly fall back to the room primary placement
    /// asking before the first room placement is a caller-ordering bug
    #[must_use]
    pub(super) fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        TransportSessionKey::new(
            self.instance_id,
            self.media_worker_id_for_connection(connection_id),
            connection_id,
            user_id.clone(),
        )
    }

    /// records the placement selected by an accepted join
    ///
    /// this is the only path that turns a newly created room from "router only"
    /// into a room with assigned worker placement
    /// it also returns the transport key that async finalization should use for
    /// bootstrap effects
    pub(super) fn register_committed_placement(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        placement: ResolvedPlacement,
    ) -> CommittedPlacementReceipt {
        let placement = placement.into_context();
        {
            let mut inner = lock_unpoisoned(&self.inner);
            match &mut inner.local_routers {
                Some(local_routers) => local_routers.upsert(placement),
                None => {
                    inner.local_routers =
                        Some(LocalRoomRouterPlacements::new(placement, Vec::new()));
                }
            }
            inner
                .placement_by_connection
                .insert(connection_id, placement);
        }
        let transport_session_key = TransportSessionKey::new(
            self.instance_id,
            placement.media_worker,
            connection_id,
            user_id.clone(),
        );
        CommittedPlacementReceipt {
            connection_id,
            placement,
            transport_session_key,
        }
    }

    /// removes the connection-specific placement after leave or replacement
    ///
    /// the room-local placement set remains available so strict rooms can reuse
    /// their first worker after becoming temporarily empty
    pub(super) fn unregister_committed_placement(&self, connection_id: ConnectionId) {
        lock_unpoisoned(&self.inner)
            .placement_by_connection
            .remove(&connection_id);
    }

    /// returns the placement for a connection or the committed primary fallback
    ///
    /// older room flows may build source routes for state that predates a
    /// connection-specific entry
    /// the fallback is valid only after the room has a real primary placement
    #[expect(
        clippy::unreachable,
        reason = "transport routing must fail loudly if called before join placement commits instead of inventing a worker id"
    )]
    fn placement_for_connection(&self, connection_id: ConnectionId) -> LocalRouterRuntimeContext {
        let inner = lock_unpoisoned(&self.inner);
        let placement = inner
            .placement_by_connection
            .get(&connection_id)
            .copied()
            .unwrap_or_else(|| {
                let Some(local_routers) = &inner.local_routers else {
                    unreachable!("connection placement lookup requires an assigned room worker");
                };
                local_routers.primary()
            });
        drop(inner);
        placement
    }

    /// returns the worker that should handle transport work for a connection
    pub(super) fn media_worker_id_for_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MediaWorkerId {
        self.placement_for_connection(connection_id).media_worker
    }

    /// returns the committed primary worker when the room has one
    ///
    /// diagnostics use this as a best-effort room-level summary
    /// a newly created room without accepted joins has no worker assignment yet
    pub(super) fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        lock_unpoisoned(&self.inner)
            .local_routers
            .as_ref()
            .map(|local_routers| local_routers.primary().media_worker)
    }

    /// captures a stable connection-to-worker lookup for media planning
    ///
    /// the returned closure is detached from the mutex guard so subscription and
    /// graph planning can call it without holding placement state locked
    #[expect(
        clippy::unreachable,
        reason = "source fanout lookup requires committed worker placement and must not synthesize worker identity"
    )]
    pub(super) fn worker_lookup(&self) -> impl Fn(ConnectionId) -> MediaWorkerId {
        let (primary_media_worker, media_worker_by_connection) = {
            let inner = lock_unpoisoned(&self.inner);
            (
                inner
                    .local_routers
                    .as_ref()
                    .map(|local_routers| local_routers.primary().media_worker),
                inner
                    .placement_by_connection
                    .iter()
                    .map(|(connection_id, placement)| (*connection_id, placement.media_worker))
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        move |connection_id| {
            media_worker_by_connection
                .get(&connection_id)
                .copied()
                .unwrap_or_else(|| {
                    let Some(media_worker) = primary_media_worker else {
                        unreachable!("worker lookup requires an assigned room worker");
                    };
                    media_worker
                })
        }
    }

    /// snapshots placement state for join planning
    ///
    /// the snapshot is read-only and may become stale before commit
    /// final resolution rechecks the current state under the room write lock
    pub(super) fn usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        let inner = lock_unpoisoned(&self.inner);
        let placements = inner
            .local_routers
            .as_ref()
            .map(|local_routers| local_routers.iter().collect())
            .unwrap_or_default();
        RoomPlacementUsageSnapshot::new(
            inner.primary_router,
            inner.local_routers.is_some(),
            placements,
        )
    }
}

impl CommittedPlacementReceipt {
    /// returns the connection accepted by the committed join
    pub(super) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    /// returns the worker selected for the accepted connection
    pub(super) const fn media_worker_id(&self) -> MediaWorkerId {
        self.placement.media_worker
    }

    /// returns the transport key derived from the committed placement
    pub(super) const fn transport_session_key(&self) -> &TransportSessionKey {
        &self.transport_session_key
    }
}

impl ResolvedPlacement {
    /// builds a resolved placement without running the planner
    ///
    /// test harnesses use this to target a specific router and worker while still
    /// exercising the normal membership commit path
    #[cfg(test)]
    pub const fn for_test(placement: LocalRouterRuntimeContext) -> Self {
        Self(placement)
    }

    /// returns the router selected for the pending join
    pub const fn router(self) -> RouterId {
        self.0.router
    }

    /// releases the resolved placement so membership can commit it
    pub const fn into_context(self) -> LocalRouterRuntimeContext {
        self.0
    }
}

/// snapshot of one room's local placement surface at the start of a join
///
/// `has_assigned_placements` separates a brand-new room from a room that has
/// already committed its first placement but is temporarily empty
/// strict rooms rely on that distinction so their first worker remains stable
/// across leave and rejoin cycles
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RoomPlacementUsageSnapshot {
    /// router used when the first session commits the room primary placement
    primary_router: RouterId,
    /// whether topology has ever committed a local placement for this room
    has_assigned_placements: bool,
    /// placements that the next join may reuse or compare against the cap
    placements: Vec<LocalRouterRuntimeContext>,
}

/// room contribution to process-wide worker load
///
/// sessions are counted on the worker that owns their transport session
/// consumers are counted on the receiver worker because that worker owns egress
/// delivery
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RoomWorkerLoadContribution {
    /// worker ids for live room sessions
    pub(super) session_worker_ids: Vec<MediaWorkerId>,
    /// worker ids for active or pending receiver-side consumers
    pub(super) consumer_worker_ids: Vec<MediaWorkerId>,
}

impl RoomPlacementUsageSnapshot {
    /// creates a read-only placement snapshot for one join decision
    ///
    /// callers must pass `has_assigned_placements` separately because an empty
    /// placement list can mean either "new room" or "room already assigned but no
    /// current connections"
    #[must_use]
    pub(super) fn new(
        primary_router: RouterId,
        has_assigned_placements: bool,
        placements: Vec<LocalRouterRuntimeContext>,
    ) -> Self {
        Self {
            primary_router,
            has_assigned_placements,
            placements,
        }
    }

    /// returns the router used when first placement assigns the primary worker
    #[must_use]
    pub(super) const fn primary_router(&self) -> RouterId {
        self.primary_router
    }

    /// next router id available to fake transports that allocate local routers
    ///
    /// production allocation stays in [`crate::engine::room::factory::RoomFactory`]
    /// so this snapshot remains a read-only view for normal joins
    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub(super) fn next_local_router_id(&self) -> RouterId {
        let router_id = self
            .placements
            .iter()
            .map(|placement| placement.router.0)
            .max()
            .map_or(self.primary_router.0, |router_id| {
                router_id.saturating_add(1)
            });
        RouterId(router_id)
    }

    /// returns placements that are available for reuse by a new join
    ///
    /// a room that has never committed placement behaves like it has no assigned
    /// placements even though its primary router has already been reserved
    fn assigned_placements(&self) -> &[LocalRouterRuntimeContext] {
        if self.has_assigned_placements {
            &self.placements
        } else {
            &[]
        }
    }
}

/// pure placement result returned to the room manager
///
/// the decision may defer router allocation
/// this keeps planning independent from the room write lock while still letting
/// finalization allocate spillover only if the join is going to commit
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoomPlacementDecision {
    /// assign the primary router to this worker for the first committed session
    AssignPrimary { media_worker_id: MediaWorkerId },
    /// place the session on an already committed room-local router
    UseExisting(LocalRouterRuntimeContext),
    /// allocate another local router on this worker before committing the join
    AllocateSpillover { media_worker_id: MediaWorkerId },
}

/// placement work carried from manager admission to room commit
///
/// this type stores the pure decision plus the worker load snapshot used to make
/// it
/// resolving can happen later against a fresher room snapshot, so stale plans
/// never force a router that is no longer valid
#[derive(Debug)]
pub(super) enum JoinPlacementPlan {
    /// already resolved placement used by targeted test harnesses
    #[cfg(any(test, feature = "testing-transport"))]
    Resolved(ResolvedPlacement),
    /// deferred decision produced from process-wide worker pressure
    Planned {
        decision: RoomPlacementDecision,
        worker_loads: WorkerLoadIndex,
        policy: RoomWorkerPolicy,
    },
}

#[cfg(any(test, feature = "testing-transport"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::room) enum RoomPlacementDecisionReason {
    ExistingPlacementAvailable,
    ReceiverCountPressure,
    ConsumerPressure,
    SourceFanoutPressure,
    EgressPressure,
    PacketLoopLagPressure,
    CommandBacklogPressure,
    RelayMailboxPressure,
    WorkerPressure,
    ActivationWindowNotMet,
    LocalRouterCapReached,
}

#[cfg(any(test, feature = "testing-transport"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestPlacementReason {
    ExistingPlacementAvailable,
    ReceiverCountPressure,
    ConsumerPressure,
    SourceFanoutPressure,
    EgressPressure,
    PacketLoopLagPressure,
    CommandBacklogPressure,
    RelayMailboxPressure,
    WorkerPressure,
    ActivationWindowNotMet,
    LocalRouterCapReached,
}

#[cfg(any(test, feature = "testing-transport"))]
impl From<RoomPlacementDecisionReason> for TestPlacementReason {
    fn from(reason: RoomPlacementDecisionReason) -> Self {
        match reason {
            RoomPlacementDecisionReason::ExistingPlacementAvailable => {
                Self::ExistingPlacementAvailable
            }
            RoomPlacementDecisionReason::ReceiverCountPressure => Self::ReceiverCountPressure,
            RoomPlacementDecisionReason::ConsumerPressure => Self::ConsumerPressure,
            RoomPlacementDecisionReason::SourceFanoutPressure => Self::SourceFanoutPressure,
            RoomPlacementDecisionReason::EgressPressure => Self::EgressPressure,
            RoomPlacementDecisionReason::PacketLoopLagPressure => Self::PacketLoopLagPressure,
            RoomPlacementDecisionReason::CommandBacklogPressure => Self::CommandBacklogPressure,
            RoomPlacementDecisionReason::RelayMailboxPressure => Self::RelayMailboxPressure,
            RoomPlacementDecisionReason::WorkerPressure => Self::WorkerPressure,
            RoomPlacementDecisionReason::ActivationWindowNotMet => Self::ActivationWindowNotMet,
            RoomPlacementDecisionReason::LocalRouterCapReached => Self::LocalRouterCapReached,
        }
    }
}

/// state that adds hysteresis to load-triggered placement
///
/// receiver pressure can fluctuate across adjacent joins
/// the activation streak prevents one noisy sample from allocating spillover
/// cooldowns let idle spillover routers detach only after remaining idle for the
/// configured window
#[derive(Debug, Default)]
pub(in crate::engine::room) struct LoadTriggeredPlacementState {
    /// consecutive pressured decisions observed for the current room
    activation_streak: usize,
    /// pressure reported by room media graph fanout rather than transport metrics
    source_fanout_pressure: bool,
    /// idle streak per spillover router before detach is allowed
    cooldown_by_router: BTreeMap<RouterId, usize>,
    /// reason surfaced only to tests and explicit testing-transport callers
    #[cfg(any(test, feature = "testing-transport"))]
    last_decision_reason: Option<RoomPlacementDecisionReason>,
}

impl LoadTriggeredPlacementState {
    /// updates whether source fanout pressure should count toward activation
    pub fn set_source_fanout_pressure(&mut self, pressured: bool) {
        self.source_fanout_pressure = pressured;
    }

    #[cfg(any(test, feature = "testing-transport"))]
    fn record_decision(&mut self, reason: RoomPlacementDecisionReason) {
        self.last_decision_reason = Some(reason);
    }

    /// clears activation after a non-pressured or successful allocation decision
    fn reset_activation(&mut self) {
        self.activation_streak = 0;
    }

    /// records one pressured placement decision and returns whether it activates
    fn record_pressure(&mut self, policy: LocalSpilloverPolicy) -> bool {
        self.activation_streak = self.activation_streak.saturating_add(1);
        self.activation_streak >= policy.parts().activation_window
    }

    /// returns spillover routers that have stayed idle long enough to detach
    ///
    /// callers pass the complete current idle set
    /// routers missing from that set lose their cooldown so a short burst of
    /// traffic requires a fresh idle window before detachment
    pub fn cooldown_detachments(
        &mut self,
        idle_router_ids: &[RouterId],
        cooldown_window: usize,
    ) -> Vec<RouterId> {
        self.cooldown_by_router
            .retain(|router_id, _| idle_router_ids.contains(router_id));
        let mut detached = Vec::new();
        for router_id in idle_router_ids {
            let cooldown = self.cooldown_by_router.entry(*router_id).or_default();
            *cooldown = cooldown.saturating_add(1);
            if *cooldown >= cooldown_window {
                detached.push(*router_id);
            }
        }
        for router_id in &detached {
            self.cooldown_by_router.remove(router_id);
        }
        detached
    }

    /// clears cooldowns for routers that are no longer eligible for detach
    pub fn clear_cooldowns(&mut self, router_ids: &[RouterId]) {
        for router_id in router_ids {
            self.cooldown_by_router.remove(router_id);
        }
    }

    /// returns the last decision reason exposed by the testing boundary
    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn last_decision_reason(&self) -> Option<RoomPlacementDecisionReason> {
        self.last_decision_reason
    }
}

impl JoinPlacementPlan {
    /// stores a deferred placement decision with the load snapshot that shaped it
    pub(super) fn planned(
        decision: RoomPlacementDecision,
        worker_loads: WorkerLoadIndex,
        policy: RoomWorkerPolicy,
    ) -> Self {
        Self::Planned {
            decision,
            worker_loads,
            policy,
        }
    }

    /// resolves a placement plan immediately before committing membership state
    ///
    /// the room snapshot passed here should be the latest snapshot available
    /// under the room write lock
    /// if the original decision targeted a placement that was removed or the room
    /// reached its placement cap, this method falls back to the least loaded
    /// currently assigned placement
    pub(super) fn resolve_for_commit(
        self,
        room: &RoomPlacementUsageSnapshot,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> ResolvedPlacement {
        let (decision, worker_loads, policy) = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Resolved(placement) => return placement,
            Self::Planned {
                decision,
                worker_loads,
                policy,
            } => (decision, worker_loads, policy),
        };
        let assigned_placements = room.assigned_placements();
        let score_policy = score_policy(policy);
        let Some(first_assigned) = assigned_placements.first().copied() else {
            return match decision {
                RoomPlacementDecision::AssignPrimary { media_worker_id }
                | RoomPlacementDecision::AllocateSpillover { media_worker_id } => {
                    ResolvedPlacement(LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: media_worker_id,
                    })
                }
                RoomPlacementDecision::UseExisting(placement) => {
                    ResolvedPlacement(LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: placement.media_worker,
                    })
                }
            };
        };
        match decision {
            RoomPlacementDecision::AssignPrimary { .. } => {
                ResolvedPlacement(worker_loads.least_loaded_placement(
                    assigned_placements,
                    first_assigned,
                    score_policy,
                ))
            }
            RoomPlacementDecision::UseExisting(placement) => {
                if let Some(assigned) = assigned_placements
                    .iter()
                    .find(|assigned| assigned.router == placement.router)
                {
                    return ResolvedPlacement(*assigned);
                }
                ResolvedPlacement(worker_loads.least_loaded_placement(
                    assigned_placements,
                    first_assigned,
                    score_policy,
                ))
            }
            RoomPlacementDecision::AllocateSpillover { .. } => {
                let placement_cap = policy
                    .max_local_routers()
                    .min(worker_loads.worker_count())
                    .max(1);
                if assigned_placements.len() >= placement_cap {
                    return ResolvedPlacement(worker_loads.least_loaded_placement(
                        assigned_placements,
                        first_assigned,
                        score_policy,
                    ));
                }
                // router allocation stays in final resolution so stale plans do
                // not reserve spillover the room no longer needs
                ResolvedPlacement(LocalRouterRuntimeContext {
                    router: allocate_spillover_router(),
                    media_worker: worker_loads
                        .least_loaded_worker(assigned_placements, score_policy),
                })
            }
        }
    }
}

/// aggregate placement load for one local media worker
///
/// the room manager builds this from committed room mappings plus
/// transport-observed pressure
/// each value is a one-join snapshot rather than a continuously maintained
/// counter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerPlacementLoad {
    /// worker this load describes after normalization by configured worker count
    media_worker_id: MediaWorkerId,
    /// sessions currently mapped to this worker
    session_count: usize,
    /// consumers whose receiver-side egress is owned by this worker
    consumer_count: usize,
    /// transport pressure published by the worker observability boundary
    pressure: TransportPlacementPressureSnapshot,
}

impl WorkerPlacementLoad {
    /// starts a worker load bucket with transport pressure already applied
    #[must_use]
    const fn new(
        media_worker_id: MediaWorkerId,
        pressure: TransportPlacementPressureSnapshot,
    ) -> Self {
        Self {
            media_worker_id,
            session_count: 0,
            consumer_count: 0,
            pressure,
        }
    }

    /// records one live session on this worker
    fn record_session(&mut self) {
        self.session_count = self.session_count.saturating_add(1);
    }

    /// records one receiver-side consumer on this worker
    fn record_consumer(&mut self) {
        self.consumer_count = self.consumer_count.saturating_add(1);
    }

    /// score used by placement ranking where lower values are preferred
    fn score(self, policy: LocalSpilloverPolicy) -> WorkerPlacementScore {
        WorkerPlacementScore {
            overloaded: self.is_overloaded(policy),
            consumer_count: self.consumer_count,
            session_count: self.session_count,
            worker_pressure_score: self.pressure.worker_pressure_score,
            packet_loop_lag_ms: self.pressure.packet_loop_lag_ms,
            command_backlog_depth: self.pressure.command_backlog_depth,
            relay_mailbox_depth: self.pressure.relay_mailbox_depth,
            egress_bitrate: self.pressure.egress_bitrate.as_bps(),
            media_worker_id: self.media_worker_id,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    fn pressure_reason(self, policy: LocalSpilloverPolicy) -> Option<RoomPlacementDecisionReason> {
        let policy = policy.parts();
        if self.session_count.saturating_add(1) >= policy.min_receiver_count {
            return Some(RoomPlacementDecisionReason::ReceiverCountPressure);
        }
        if self.consumer_count >= policy.max_active_consumers_per_router {
            return Some(RoomPlacementDecisionReason::ConsumerPressure);
        }
        if policy.egress_bitrate_threshold > crate::Bitrate::zero()
            && self.pressure.egress_bitrate >= policy.egress_bitrate_threshold
        {
            return Some(RoomPlacementDecisionReason::EgressPressure);
        }
        if policy.packet_loop_lag_threshold_ms > 0
            && self.pressure.packet_loop_lag_ms >= policy.packet_loop_lag_threshold_ms
        {
            return Some(RoomPlacementDecisionReason::PacketLoopLagPressure);
        }
        if policy.command_backlog_threshold > 0
            && self.pressure.command_backlog_depth >= policy.command_backlog_threshold
        {
            return Some(RoomPlacementDecisionReason::CommandBacklogPressure);
        }
        if policy.relay_mailbox_depth_threshold > 0
            && self.pressure.relay_mailbox_depth >= policy.relay_mailbox_depth_threshold
        {
            return Some(RoomPlacementDecisionReason::RelayMailboxPressure);
        }
        if policy.worker_pressure_threshold > 0
            && self.pressure.worker_pressure_score >= policy.worker_pressure_threshold
        {
            return Some(RoomPlacementDecisionReason::WorkerPressure);
        }
        None
    }

    /// whether adding another receiver would cross any spillover trigger
    fn is_overloaded(self, policy: LocalSpilloverPolicy) -> bool {
        let policy = policy.parts();
        self.session_count.saturating_add(1) >= policy.min_receiver_count
            || self.consumer_count >= policy.max_active_consumers_per_router
            || policy.egress_bitrate_threshold > crate::Bitrate::zero()
                && self.pressure.egress_bitrate >= policy.egress_bitrate_threshold
            || policy.packet_loop_lag_threshold_ms > 0
                && self.pressure.packet_loop_lag_ms >= policy.packet_loop_lag_threshold_ms
            || policy.command_backlog_threshold > 0
                && self.pressure.command_backlog_depth >= policy.command_backlog_threshold
            || policy.relay_mailbox_depth_threshold > 0
                && self.pressure.relay_mailbox_depth >= policy.relay_mailbox_depth_threshold
            || policy.worker_pressure_threshold > 0
                && self.pressure.worker_pressure_score >= policy.worker_pressure_threshold
    }
}

/// ordered load tuple used as the deterministic worker tie-break
///
/// the field order is the policy contract
/// overloaded workers lose first, then consumer count, session count, worker
/// pressure, packet-loop lag, command backlog, relay mailbox depth, egress
/// bitrate and worker id decide ties
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WorkerPlacementScore {
    overloaded: bool,
    consumer_count: usize,
    session_count: usize,
    worker_pressure_score: u8,
    packet_loop_lag_ms: u64,
    command_backlog_depth: usize,
    relay_mailbox_depth: usize,
    egress_bitrate: u64,
    media_worker_id: MediaWorkerId,
}

/// load accumulator for one placement decision
///
/// every configured worker receives an entry, even if transport observability
/// has not published pressure yet
/// this lets first-join placement prefer unused workers
#[derive(Debug)]
pub(super) struct WorkerLoadIndex {
    loads: Vec<WorkerPlacementLoad>,
}

impl WorkerLoadIndex {
    /// create a complete worker load set from best-effort transport pressure
    ///
    /// missing snapshots are treated as idle workers
    /// snapshot worker ids are normalized by the configured worker count because
    /// fake transports can replay stale test data across differently sized
    /// runtimes
    #[must_use]
    pub(super) fn new(
        media_worker_count: usize,
        pressure_snapshots: Vec<TransportWorkerPressureSnapshot>,
    ) -> Self {
        let media_worker_count = media_worker_count.max(1);
        let mut loads = (0..media_worker_count)
            .map(|media_worker_id| {
                WorkerPlacementLoad::new(
                    MediaWorkerId::from_raw(media_worker_id),
                    TransportPlacementPressureSnapshot::default(),
                )
            })
            .collect::<Vec<_>>();
        for snapshot in pressure_snapshots {
            let media_worker_id =
                MediaWorkerId::from_raw(snapshot.media_worker_id.as_usize() % media_worker_count);
            if let Some(load) = loads.get_mut(media_worker_id.as_usize()) {
                *load = WorkerPlacementLoad::new(media_worker_id, snapshot.pressure);
            }
        }
        Self { loads }
    }

    /// adds one room session to the normalized worker bucket
    pub(super) fn record_session(&mut self, media_worker_id: MediaWorkerId) {
        if let Some(load) = self.load_mut_for_worker(media_worker_id) {
            load.record_session();
        }
    }

    /// adds one receiver-side consumer to the normalized worker bucket
    pub(super) fn record_consumer(&mut self, media_worker_id: MediaWorkerId) {
        if let Some(load) = self.load_mut_for_worker(media_worker_id) {
            load.record_consumer();
        }
    }

    /// returns the mutable load bucket for possibly noncanonical worker input
    fn load_mut_for_worker(
        &mut self,
        media_worker_id: MediaWorkerId,
    ) -> Option<&mut WorkerPlacementLoad> {
        let worker_count = self.worker_count();
        self.loads
            .get_mut(media_worker_id.as_usize() % worker_count)
    }

    /// returns the load bucket for a possibly noncanonical worker id
    ///
    /// noncanonical ids are folded by the configured worker count because test
    /// and diagnostics boundaries can replay raw ids from a different runtime
    fn load_for_worker(&self, media_worker_id: MediaWorkerId) -> WorkerPlacementLoad {
        let media_worker_id =
            MediaWorkerId::from_raw(media_worker_id.as_usize() % self.worker_count());
        self.loads
            .get(media_worker_id.as_usize())
            .copied()
            .unwrap_or_else(|| {
                WorkerPlacementLoad::new(
                    media_worker_id,
                    TransportPlacementPressureSnapshot::default(),
                )
            })
    }

    /// returns the least loaded worker outside the current room placements
    ///
    /// when all workers are already used by the room, the same scoring fallback
    /// chooses among every configured worker so callers still get deterministic
    /// placement
    #[expect(
        clippy::unreachable,
        reason = "WorkerLoadIndex::new normalizes the worker count to at least one load"
    )]
    fn least_loaded_worker(
        &self,
        excluded_placements: &[LocalRouterRuntimeContext],
        policy: LocalSpilloverPolicy,
    ) -> MediaWorkerId {
        let Some(load) = self
            .loads
            .iter()
            .filter(|load| {
                !excluded_placements
                    .iter()
                    .any(|placement| placement.media_worker == load.media_worker_id)
            })
            .min_by_key(|load| load.score(policy))
            .or_else(|| self.loads.iter().min_by_key(|load| load.score(policy)))
        else {
            unreachable!("worker load index is built with at least one worker");
        };
        load.media_worker_id
    }

    /// returns the least loaded placement from the room-local placement list
    ///
    /// `fallback` represents the caller's known valid placement and protects
    /// against stale or empty snapshots without synthesizing a router or worker id
    fn least_loaded_placement(
        &self,
        placements: &[LocalRouterRuntimeContext],
        fallback: LocalRouterRuntimeContext,
        policy: LocalSpilloverPolicy,
    ) -> LocalRouterRuntimeContext {
        placements
            .iter()
            .copied()
            .min_by_key(|placement| self.load_for_worker(placement.media_worker).score(policy))
            .unwrap_or(fallback)
    }

    /// returns the normalized worker count used for modulo operations
    fn worker_count(&self) -> usize {
        self.loads.len().max(1)
    }
}

/// pure policy evaluator for join-time room placement
///
/// the planner reads one room snapshot and one process-wide load snapshot
/// the caller serializes the eventual room mutation so later joins observe the
/// committed placement
#[derive(Debug, Clone)]
pub(super) struct RoomPlacementPlanner {
    media_worker_count: usize,
    policy: RoomWorkerPolicy,
}

impl RoomPlacementPlanner {
    /// create a planner for the configured local workers and room policy
    #[must_use]
    pub(super) fn new(media_worker_count: usize, policy: RoomWorkerPolicy) -> Self {
        Self {
            media_worker_count: media_worker_count.max(1),
            policy,
        }
    }

    /// choose the placement for the next session joining a room
    ///
    /// strict rooms keep their first assigned worker
    /// bounded rooms allocate unused workers until the cap
    /// load-triggered rooms reuse a capable room worker and allocate only when
    /// all existing room placements are pressured
    #[must_use]
    pub(super) fn choose(
        &self,
        room: &RoomPlacementUsageSnapshot,
        load_index: &WorkerLoadIndex,
    ) -> RoomPlacementDecision {
        let mut load_state = LoadTriggeredPlacementState::default();
        self.choose_with_load_state(room, load_index, &mut load_state)
    }

    /// chooses a placement while reusing caller-managed load-triggered state
    ///
    /// callers that keep [`LoadTriggeredPlacementState`] across joins get the
    /// activation-window behavior for load-triggered spillover
    /// callers that use [`Self::choose`] get a stateless decision suitable for
    /// strict or bounded policies
    pub(super) fn choose_with_load_state(
        &self,
        room: &RoomPlacementUsageSnapshot,
        load_index: &WorkerLoadIndex,
        load_state: &mut LoadTriggeredPlacementState,
    ) -> RoomPlacementDecision {
        let placement_cap = self
            .policy
            .max_local_routers()
            .min(self.media_worker_count)
            .max(1);
        let score_policy = score_policy(self.policy);
        let assigned_placements = room.assigned_placements();
        let Some(first_assigned) = assigned_placements.first().copied() else {
            load_state.reset_activation();
            return RoomPlacementDecision::AssignPrimary {
                media_worker_id: load_index.least_loaded_worker(&[], score_policy),
            };
        };
        match self.policy.spillover() {
            RoomSpilloverMode::StrictSingleRouter => {
                load_state.reset_activation();
                RoomPlacementDecision::UseExisting(first_assigned)
            }
            RoomSpilloverMode::BoundedLocalSpillover => {
                if assigned_placements.len() < placement_cap {
                    load_state.reset_activation();
                    return RoomPlacementDecision::AllocateSpillover {
                        media_worker_id: load_index
                            .least_loaded_worker(assigned_placements, score_policy),
                    };
                }
                load_state.reset_activation();
                RoomPlacementDecision::UseExisting(load_index.least_loaded_placement(
                    assigned_placements,
                    first_assigned,
                    score_policy,
                ))
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let placement =
                    load_index.least_loaded_placement(assigned_placements, first_assigned, policy);
                let load = load_index.load_for_worker(placement.media_worker);
                // testing callers need the dominant reason, production only needs
                // the boolean overload result to choose the same placement path
                #[cfg(any(test, feature = "testing-transport"))]
                let pressure_reason = load.pressure_reason(policy).or_else(|| {
                    load_state
                        .source_fanout_pressure
                        .then_some(RoomPlacementDecisionReason::SourceFanoutPressure)
                });
                #[cfg(any(test, feature = "testing-transport"))]
                if pressure_reason.is_none() {
                    load_state.reset_activation();
                    load_state
                        .record_decision(RoomPlacementDecisionReason::ExistingPlacementAvailable);
                    return RoomPlacementDecision::UseExisting(placement);
                }
                #[cfg(not(any(test, feature = "testing-transport")))]
                if !load.is_overloaded(policy) && !load_state.source_fanout_pressure {
                    load_state.reset_activation();
                    return RoomPlacementDecision::UseExisting(placement);
                }
                if !load_state.record_pressure(policy) {
                    #[cfg(any(test, feature = "testing-transport"))]
                    load_state.record_decision(RoomPlacementDecisionReason::ActivationWindowNotMet);
                    return RoomPlacementDecision::UseExisting(placement);
                }
                if assigned_placements.len() < placement_cap {
                    load_state.reset_activation();
                    #[cfg(any(test, feature = "testing-transport"))]
                    let reason =
                        pressure_reason.unwrap_or(RoomPlacementDecisionReason::WorkerPressure);
                    #[cfg(any(test, feature = "testing-transport"))]
                    load_state.record_decision(reason);
                    return RoomPlacementDecision::AllocateSpillover {
                        media_worker_id: load_index
                            .least_loaded_worker(assigned_placements, policy),
                    };
                }
                #[cfg(any(test, feature = "testing-transport"))]
                load_state.record_decision(RoomPlacementDecisionReason::LocalRouterCapReached);
                RoomPlacementDecision::UseExisting(placement)
            }
        }
    }
}

/// returns the score policy used when the room policy has no load thresholds
fn score_policy(policy: RoomWorkerPolicy) -> LocalSpilloverPolicy {
    match policy.spillover() {
        RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => policy,
        RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
            LocalSpilloverPolicy::conservative()
        }
    }
}

#[cfg(test)]
mod tests;
