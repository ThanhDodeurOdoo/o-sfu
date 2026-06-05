//! join-time placement planning for room-local media workers
//!
//! the room manager gathers process-wide worker pressure after admission has
//! selected a live room
//! [`Room::plan_join_placement`] combines that load snapshot with room-local
//! policy state before the membership transition commits
//!
//! the planner ranks workers from committed room state plus transport pressure
//! snapshots, then returns a decision that membership resolves into a concrete
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
//! the room can compute a [`RoomPlacementDecision`] before it takes the room
//! write lock
//! [`JoinPlacementPlan::resolve_for_commit`] converts that decision into a
//! concrete [`ResolvedPlacement`] while the membership transition is being
//! applied
//! the room routing state records the final mapping only after the join is
//! accepted

use std::{collections::BTreeMap, iter};

use o_sfu_router::RouterId;

use super::{Room, SourcePolicyEvent};
use crate::{
    LocalSpilloverPolicy, RoomSpilloverMode, RoomWorkerPolicy,
    engine::{
        MediaWorkerId, RoomInstanceId,
        media_transport::{TransportPlacementPressureSnapshot, TransportWorkerPressureSnapshot},
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

    #[must_use]
    pub const fn instance(&self) -> RoomInstanceId {
        self.instance
    }

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

/// concrete placement selected for a join but not yet recorded
///
/// this separates planning from mutation
/// the manager can carry a plan across async work, then membership resolves it
/// under the room write lock and records the final placement only if the state
/// transition succeeds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::room) struct ResolvedPlacement(LocalRouterRuntimeContext);

impl ResolvedPlacement {
    /// builds a resolved placement without running the planner
    ///
    /// test harnesses use this to target a specific router and worker while still
    /// exercising the normal membership commit path
    #[cfg(test)]
    pub const fn for_test(placement: LocalRouterRuntimeContext) -> Self {
        Self(placement)
    }

    pub const fn router(self) -> RouterId {
        self.0.router
    }

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
    primary_router: RouterId,
    has_assigned_placements: bool,
    placements: Vec<LocalRouterRuntimeContext>,
}

/// room contribution to process-wide worker load
///
/// sessions are counted on the worker that owns their transport session
/// consumers are counted on the receiver worker because that worker owns egress
/// delivery
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RoomWorkerLoadContribution {
    pub(super) session_worker_ids: Vec<MediaWorkerId>,
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

/// state that adds hysteresis to load-triggered placement
///
/// receiver pressure can fluctuate across adjacent joins
/// the activation streak prevents one noisy sample from allocating spillover
/// cooldowns let idle spillover routers detach only after remaining idle for the
/// configured window
#[derive(Debug, Default)]
pub(in crate::engine::room) struct LoadTriggeredPlacementState {
    activation_streak: usize,
    source_fanout_pressure: bool,
    cooldown_by_router: BTreeMap<RouterId, usize>,
}

impl LoadTriggeredPlacementState {
    pub fn set_source_fanout_pressure(&mut self, pressured: bool) {
        self.source_fanout_pressure = pressured;
    }

    fn reset_activation(&mut self) {
        self.activation_streak = 0;
    }

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

    pub fn clear_cooldowns(&mut self, router_ids: &[RouterId]) {
        for router_id in router_ids {
            self.cooldown_by_router.remove(router_id);
        }
    }
}

impl Room {
    pub(in crate::engine::room) async fn plan_join_placement(
        &self,
        worker_loads: WorkerLoadIndex,
    ) -> JoinPlacementPlan {
        let room_snapshot = self.placement_usage_snapshot().await;
        let policy = self.room_worker_policy();
        let planner = RoomPlacementPlanner::new(policy);
        let decision = match policy.spillover() {
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_) => {
                self.handle_source_policy_event(SourcePolicyEvent::FanoutPressureChanged, None)
                    .await;
                let mut load_state = lock_unpoisoned(&self.load_triggered_placement);
                planner.choose_with_load_state(&room_snapshot, &worker_loads, &mut load_state)
            }
            RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
                planner.choose(&room_snapshot, &worker_loads)
            }
        };
        JoinPlacementPlan::planned(decision, worker_loads, policy)
    }
}

impl JoinPlacementPlan {
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

    fn record_session(&mut self) {
        self.session_count = self.session_count.saturating_add(1);
    }

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

    pub(super) fn record_session(&mut self, media_worker_id: MediaWorkerId) {
        if let Some(load) = self.load_mut_for_worker(media_worker_id) {
            load.record_session();
        }
    }

    pub(super) fn record_consumer(&mut self, media_worker_id: MediaWorkerId) {
        if let Some(load) = self.load_mut_for_worker(media_worker_id) {
            load.record_consumer();
        }
    }

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
    policy: RoomWorkerPolicy,
}

impl RoomPlacementPlanner {
    /// create a planner for one room-worker policy
    #[must_use]
    pub(super) const fn new(policy: RoomWorkerPolicy) -> Self {
        Self { policy }
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
            .min(load_index.worker_count())
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
                if !load.is_overloaded(policy) && !load_state.source_fanout_pressure {
                    load_state.reset_activation();
                    return RoomPlacementDecision::UseExisting(placement);
                }
                if !load_state.record_pressure(policy) {
                    return RoomPlacementDecision::UseExisting(placement);
                }
                if assigned_placements.len() < placement_cap {
                    load_state.reset_activation();
                    return RoomPlacementDecision::AllocateSpillover {
                        media_worker_id: load_index
                            .least_loaded_worker(assigned_placements, policy),
                    };
                }
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
