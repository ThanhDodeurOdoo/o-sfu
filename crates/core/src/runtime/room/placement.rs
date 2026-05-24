//! join-time placement planning for room-local media workers
//!
//! `RoomManager::join_user` uses this module after admission has selected a
//! live room but before the membership transition commits
//!
//! the planner ranks workers from committed room state plus transport pressure
//! snapshots, then returns a decision that the manager resolves into a concrete
//! placement
//!
//! this is cold-path control-plane work
//! the packet loop only sees the resolved transport keys after the join has
//! committed

use std::{collections::BTreeMap, iter, sync::Mutex};

use o_sfu_router::RouterId;

use crate::{
    LocalSpilloverPolicy, RoomSpilloverMode, RoomWorkerPolicy,
    runtime::{
        ConnectionId, RoomInstanceId, UserId,
        media_transport::{
            TransportPlacementPressureSnapshot, TransportSessionKey,
            TransportWorkerPressureSnapshot,
        },
        sync::lock_unpoisoned,
    },
};

/// stable runtime placement chosen when the room is created
///
/// these values identify where the room lives inside the current process.
/// unlike room identity, they are runtime-local and mainly matter for routing,
/// transport ownership, diagnostics correlation and teardown
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRuntimeContext {
    /// unique live instance id used to correlate runtime events and health
    instance: RoomInstanceId,
    /// local router and worker placements already known for this room
    local_routers: LocalRoomRouterPlacements,
}

/// one room-local router placement and its owning media worker
///
/// this is runtime-local metadata. it is never sent to clients and it is not a
/// distributed owner identity. the room factory creates the primary router and
/// the room manager adds spillover placements when live load needs them
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRouterRuntimeContext {
    /// pure router id used inside the room topology
    pub router: RouterId,
    /// local rtc media worker that owns transport sessions placed here
    pub media_worker: usize,
}

/// non-empty router placement set assigned to one live room
///
/// this is the shared placement contract consumed by `RoomDefinition` and
/// `RoomTopology`. keeping the primary placement inside this validated value
/// avoids an api where one field can disagree with the placement list
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRoomRouterPlacements {
    primary: LocalRouterRuntimeContext,
    spillover: Vec<LocalRouterRuntimeContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRoomRouterPlacementsError {
    Empty,
}

impl LocalRoomRouterPlacements {
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

    pub(in crate::runtime::room) fn upsert(&mut self, placement: LocalRouterRuntimeContext) {
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

    #[must_use]
    pub fn len(&self) -> usize {
        self.spillover.len().saturating_add(1)
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<LocalRouterRuntimeContext> {
        if index == 0 {
            return Some(self.primary);
        }
        self.spillover.get(index.checked_sub(1)?).copied()
    }

    #[must_use]
    pub fn contains_router(&self, router_id: RouterId) -> bool {
        self.primary.router == router_id
            || self
                .spillover
                .iter()
                .any(|placement| placement.router == router_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = LocalRouterRuntimeContext> + '_ {
        iter::once(self.primary).chain(self.spillover.iter().copied())
    }
}

impl RoomRuntimeContext {
    #[must_use]
    pub fn new(
        instance: RoomInstanceId,
        primary: LocalRouterRuntimeContext,
        spillover: Vec<LocalRouterRuntimeContext>,
    ) -> Self {
        Self {
            instance,
            local_routers: LocalRoomRouterPlacements::new(primary, spillover),
        }
    }

    /// builds a runtime context from an explicit instance id and placements
    ///
    /// # Errors
    ///
    /// returns [`LocalRoomRouterPlacementsError::Empty`] when the placement
    /// list has no primary router
    pub fn try_from_placements(
        instance: RoomInstanceId,
        placements: Vec<LocalRouterRuntimeContext>,
    ) -> Result<Self, LocalRoomRouterPlacementsError> {
        Ok(Self {
            instance,
            local_routers: LocalRoomRouterPlacements::try_from_vec(placements)?,
        })
    }

    #[must_use]
    pub const fn instance(&self) -> RoomInstanceId {
        self.instance
    }

    #[must_use]
    pub const fn media_worker(&self) -> usize {
        self.local_routers.primary().media_worker
    }

    #[must_use]
    pub const fn primary_router(&self) -> RouterId {
        self.local_routers.primary().router
    }

    #[must_use]
    pub const fn local_routers(&self) -> &LocalRoomRouterPlacements {
        &self.local_routers
    }
}

/// committed room-local placement state
///
/// joins register the selected router and media worker for each committed
/// connection. later transport commands use the same mapping to build stable
/// session keys without deriving placement from topology internals
#[derive(Debug)]
pub(super) struct RoomPlacementState {
    instance_id: RoomInstanceId,
    inner: Mutex<RoomPlacementStateInner>,
}

#[derive(Debug)]
struct RoomPlacementStateInner {
    local_routers: LocalRoomRouterPlacements,
    has_assigned_placements: bool,
    placement_by_connection: BTreeMap<ConnectionId, LocalRouterRuntimeContext>,
}

/// placement data returned by a committed join transition
///
/// async finalization uses this receipt instead of rebuilding transport keys
/// from topology state after the room lock has been released
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommittedPlacementReceipt {
    connection_id: ConnectionId,
    placement: LocalRouterRuntimeContext,
    transport_session_key: TransportSessionKey,
}

impl RoomPlacementState {
    #[must_use]
    pub(super) fn new(
        instance_id: RoomInstanceId,
        local_routers: LocalRoomRouterPlacements,
    ) -> Self {
        Self {
            instance_id,
            inner: Mutex::new(RoomPlacementStateInner {
                local_routers,
                has_assigned_placements: false,
                placement_by_connection: BTreeMap::new(),
            }),
        }
    }

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

    pub(super) fn register_committed_placement(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        placement: LocalRouterRuntimeContext,
    ) -> CommittedPlacementReceipt {
        {
            let mut inner = lock_unpoisoned(&self.inner);
            inner.local_routers.upsert(placement);
            inner.has_assigned_placements = true;
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

    pub(super) fn unregister_committed_placement(&self, connection_id: ConnectionId) {
        lock_unpoisoned(&self.inner)
            .placement_by_connection
            .remove(&connection_id);
    }

    fn placement_for_connection(&self, connection_id: ConnectionId) -> LocalRouterRuntimeContext {
        let inner = lock_unpoisoned(&self.inner);
        if let Some(placement) = inner.placement_by_connection.get(&connection_id).copied() {
            return placement;
        }
        inner.local_routers.primary()
    }

    pub(super) fn media_worker_id_for_connection(&self, connection_id: ConnectionId) -> usize {
        self.placement_for_connection(connection_id).media_worker
    }

    pub(super) fn media_worker_id(&self) -> usize {
        lock_unpoisoned(&self.inner)
            .local_routers
            .primary()
            .media_worker
    }

    pub(super) fn usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        let inner = lock_unpoisoned(&self.inner);
        RoomPlacementUsageSnapshot::new(
            inner.local_routers.primary().router,
            inner.has_assigned_placements,
            inner.local_routers.iter().collect(),
        )
    }
}

impl CommittedPlacementReceipt {
    pub(super) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(super) const fn media_worker_id(&self) -> usize {
        self.placement.media_worker
    }

    pub(super) const fn transport_session_key(&self) -> &TransportSessionKey {
        &self.transport_session_key
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
    pub(super) session_workers: Vec<usize>,
    /// worker ids for active or pending receiver-side consumers
    pub(super) consumer_workers: Vec<usize>,
}

impl RoomPlacementUsageSnapshot {
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
    /// production allocation stays in `RoomFactory` so this snapshot remains a
    /// read-only view for normal joins
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
/// new placements carry only a worker id
/// the manager still owns router allocation
/// membership finalization owns the later state commit
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoomPlacementDecision {
    /// assign the primary router to this worker for the first committed session
    AssignPrimary { media_worker_id: usize },
    /// place the session on an already committed room-local router
    UseExisting(LocalRouterRuntimeContext),
    /// allocate another local router on this worker before committing the join
    AllocateSpillover { media_worker_id: usize },
}

#[derive(Debug)]
pub(super) enum JoinPlacement {
    #[cfg(any(test, feature = "testing-transport"))]
    Resolved(LocalRouterRuntimeContext),
    Planned {
        decision: RoomPlacementDecision,
        worker_loads: WorkerLoadIndex,
        media_worker_count: usize,
        policy: RoomWorkerPolicy,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::room) enum RoomPlacementDecisionReason {
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

#[derive(Debug, Default)]
pub(in crate::runtime::room) struct LoadTriggeredPlacementState {
    activation_streak: usize,
    source_fanout_pressure: bool,
    cooldown_by_router: BTreeMap<RouterId, usize>,
    last_decision_reason: Option<RoomPlacementDecisionReason>,
}

impl LoadTriggeredPlacementState {
    pub(in crate::runtime::room) fn set_source_fanout_pressure(&mut self, pressured: bool) {
        self.source_fanout_pressure = pressured;
    }

    fn record_decision(&mut self, reason: RoomPlacementDecisionReason) {
        self.last_decision_reason = Some(reason);
    }

    fn reset_activation(&mut self) {
        self.activation_streak = 0;
    }

    fn record_pressure(&mut self, policy: LocalSpilloverPolicy) -> bool {
        self.activation_streak = self.activation_streak.saturating_add(1);
        self.activation_streak >= policy.activation_window()
    }

    pub(in crate::runtime::room) fn cooldown_detachments(
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

    pub(in crate::runtime::room) fn clear_cooldowns(&mut self, router_ids: &[RouterId]) {
        for router_id in router_ids {
            self.cooldown_by_router.remove(router_id);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime::room) const fn last_decision_reason(
        &self,
    ) -> Option<RoomPlacementDecisionReason> {
        self.last_decision_reason
    }
}

impl JoinPlacement {
    pub(super) fn planned(
        decision: RoomPlacementDecision,
        worker_loads: WorkerLoadIndex,
        media_worker_count: usize,
        policy: RoomWorkerPolicy,
    ) -> Self {
        Self::Planned {
            decision,
            worker_loads,
            media_worker_count,
            policy,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) const fn resolved(placement: LocalRouterRuntimeContext) -> Self {
        Self::Resolved(placement)
    }

    pub(super) fn resolve_for_commit(
        self,
        room: &RoomPlacementUsageSnapshot,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> LocalRouterRuntimeContext {
        let (decision, worker_loads, media_worker_count, policy) = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Resolved(placement) => return placement,
            Self::Planned {
                decision,
                worker_loads,
                media_worker_count,
                policy,
            } => (decision, worker_loads, media_worker_count, policy),
        };
        let assigned_placements = room.assigned_placements();
        let score_policy = score_policy(policy);
        let existing_placement =
            worker_loads.least_loaded_placement(assigned_placements, score_policy);
        match decision {
            RoomPlacementDecision::AssignPrimary { media_worker_id } => {
                if assigned_placements.is_empty() {
                    return LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: media_worker_id,
                    };
                }
                existing_placement
            }
            RoomPlacementDecision::UseExisting(placement) => {
                if let Some(assigned) = assigned_placements
                    .iter()
                    .find(|assigned| assigned.router == placement.router)
                {
                    return *assigned;
                }
                if assigned_placements.is_empty() {
                    return LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: placement.media_worker,
                    };
                }
                existing_placement
            }
            RoomPlacementDecision::AllocateSpillover { media_worker_id } => {
                if assigned_placements.is_empty() {
                    return LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: media_worker_id,
                    };
                }
                let placement_cap = policy.max_local_routers().min(media_worker_count).max(1);
                if assigned_placements.len() >= placement_cap {
                    return existing_placement;
                }
                LocalRouterRuntimeContext {
                    router: allocate_spillover_router(),
                    media_worker: worker_loads
                        .least_loaded_worker(assigned_placements, score_policy),
                }
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
    media_worker_id: usize,
    /// sessions currently mapped to this worker
    session_count: usize,
    /// consumers whose receiver-side egress is owned by this worker
    consumer_count: usize,
    /// transport pressure published by the worker observability boundary
    pressure: TransportPlacementPressureSnapshot,
}

impl WorkerPlacementLoad {
    #[must_use]
    const fn new(media_worker_id: usize, pressure: TransportPlacementPressureSnapshot) -> Self {
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

    fn pressure_reason(self, policy: LocalSpilloverPolicy) -> Option<RoomPlacementDecisionReason> {
        if self.session_count.saturating_add(1) >= policy.min_receiver_count() {
            return Some(RoomPlacementDecisionReason::ReceiverCountPressure);
        }
        if self.consumer_count >= policy.max_active_consumers_per_router() {
            return Some(RoomPlacementDecisionReason::ConsumerPressure);
        }
        if policy.egress_bitrate_threshold() > crate::Bitrate::zero()
            && self.pressure.egress_bitrate >= policy.egress_bitrate_threshold()
        {
            return Some(RoomPlacementDecisionReason::EgressPressure);
        }
        if policy.packet_loop_lag_threshold_ms() > 0
            && self.pressure.packet_loop_lag_ms >= policy.packet_loop_lag_threshold_ms()
        {
            return Some(RoomPlacementDecisionReason::PacketLoopLagPressure);
        }
        if policy.command_backlog_threshold() > 0
            && self.pressure.command_backlog_depth >= policy.command_backlog_threshold()
        {
            return Some(RoomPlacementDecisionReason::CommandBacklogPressure);
        }
        if policy.relay_mailbox_depth_threshold() > 0
            && self.pressure.relay_mailbox_depth >= policy.relay_mailbox_depth_threshold()
        {
            return Some(RoomPlacementDecisionReason::RelayMailboxPressure);
        }
        if policy.worker_pressure_threshold() > 0
            && self.pressure.worker_pressure_score >= policy.worker_pressure_threshold()
        {
            return Some(RoomPlacementDecisionReason::WorkerPressure);
        }
        None
    }

    /// whether adding another receiver would cross any spillover trigger
    fn is_overloaded(self, policy: LocalSpilloverPolicy) -> bool {
        self.pressure_reason(policy).is_some()
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
    media_worker_id: usize,
}

/// load accumulator for one placement decision
///
/// every configured worker receives an entry, even if transport observability
/// has not published pressure yet
/// this lets first-join placement prefer unused workers
#[derive(Debug)]
pub(super) struct WorkerLoadIndex {
    media_worker_count: usize,
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
                    media_worker_id,
                    TransportPlacementPressureSnapshot::default(),
                )
            })
            .collect::<Vec<_>>();
        for snapshot in pressure_snapshots {
            let media_worker_id = snapshot.media_worker_id % media_worker_count;
            if let Some(load) = loads.get_mut(media_worker_id) {
                *load = WorkerPlacementLoad::new(media_worker_id, snapshot.pressure);
            }
        }
        Self {
            media_worker_count,
            loads,
        }
    }

    pub(super) fn record_session(&mut self, media_worker_id: usize) {
        if let Some(load) = self.load_mut_for_worker(media_worker_id) {
            load.record_session();
        }
    }

    pub(super) fn record_consumer(&mut self, media_worker_id: usize) {
        if let Some(load) = self.load_mut_for_worker(media_worker_id) {
            load.record_consumer();
        }
    }

    fn load_mut_for_worker(&mut self, media_worker_id: usize) -> Option<&mut WorkerPlacementLoad> {
        self.loads
            .get_mut(media_worker_id % self.media_worker_count)
    }

    fn load_for_worker(&self, media_worker_id: usize) -> WorkerPlacementLoad {
        let media_worker_id = media_worker_id % self.media_worker_count;
        self.loads.get(media_worker_id).copied().unwrap_or_else(|| {
            WorkerPlacementLoad::new(
                media_worker_id,
                TransportPlacementPressureSnapshot::default(),
            )
        })
    }

    fn least_loaded_worker(
        &self,
        excluded_placements: &[LocalRouterRuntimeContext],
        policy: LocalSpilloverPolicy,
    ) -> usize {
        self.loads
            .iter()
            .filter(|load| {
                !excluded_placements
                    .iter()
                    .any(|placement| placement.media_worker == load.media_worker_id)
            })
            .min_by_key(|load| load.score(policy))
            .or_else(|| self.loads.iter().min_by_key(|load| load.score(policy)))
            .map_or(0, |load| load.media_worker_id)
    }

    fn least_loaded_placement(
        &self,
        placements: &[LocalRouterRuntimeContext],
        policy: LocalSpilloverPolicy,
    ) -> LocalRouterRuntimeContext {
        placements
            .iter()
            .copied()
            .min_by_key(|placement| self.load_for_worker(placement.media_worker).score(policy))
            .unwrap_or(LocalRouterRuntimeContext {
                router: RouterId(0),
                media_worker: 0,
            })
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
        if assigned_placements.is_empty() {
            load_state.reset_activation();
            return RoomPlacementDecision::AssignPrimary {
                media_worker_id: load_index.least_loaded_worker(&[], score_policy),
            };
        }
        match self.policy.spillover() {
            RoomSpilloverMode::StrictSingleRouter => {
                load_state.reset_activation();
                RoomPlacementDecision::UseExisting(assigned_placements.first().copied().unwrap_or(
                    LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: 0,
                    },
                ))
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
                RoomPlacementDecision::UseExisting(
                    load_index.least_loaded_placement(assigned_placements, score_policy),
                )
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let placement = load_index.least_loaded_placement(assigned_placements, policy);
                let worker_pressure = load_index
                    .load_for_worker(placement.media_worker)
                    .pressure_reason(policy);
                let pressure_reason = worker_pressure.or_else(|| {
                    load_state
                        .source_fanout_pressure
                        .then_some(RoomPlacementDecisionReason::SourceFanoutPressure)
                });
                let Some(reason) = pressure_reason else {
                    load_state.reset_activation();
                    load_state
                        .record_decision(RoomPlacementDecisionReason::ExistingPlacementAvailable);
                    return RoomPlacementDecision::UseExisting(placement);
                };
                if !load_state.record_pressure(policy) {
                    load_state.record_decision(RoomPlacementDecisionReason::ActivationWindowNotMet);
                    return RoomPlacementDecision::UseExisting(placement);
                }
                if assigned_placements.len() < placement_cap {
                    load_state.reset_activation();
                    load_state.record_decision(reason);
                    return RoomPlacementDecision::AllocateSpillover {
                        media_worker_id: load_index
                            .least_loaded_worker(assigned_placements, policy),
                    };
                }
                load_state.record_decision(RoomPlacementDecisionReason::LocalRouterCapReached);
                RoomPlacementDecision::UseExisting(placement)
            }
        }
    }
}

fn score_policy(policy: RoomWorkerPolicy) -> LocalSpilloverPolicy {
    match policy.spillover() {
        RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => policy,
        RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
            LocalSpilloverPolicy::conservative()
        }
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_router::RouterId;

    use super::*;
    use crate::{
        Bitrate,
        prelude::{LocalSpilloverPolicyError, LocalSpilloverPolicyParts},
    };

    fn placement(router: u64, media_worker: usize) -> LocalRouterRuntimeContext {
        LocalRouterRuntimeContext {
            router: RouterId(router),
            media_worker,
        }
    }

    fn primary_placement() -> LocalRouterRuntimeContext {
        placement(7, 0)
    }

    fn room_with(placements: Vec<LocalRouterRuntimeContext>) -> RoomPlacementUsageSnapshot {
        RoomPlacementUsageSnapshot::new(RouterId(7), true, placements)
    }

    fn primary_room() -> RoomPlacementUsageSnapshot {
        room_with(vec![primary_placement()])
    }

    fn hot_loads(workers: impl IntoIterator<Item = usize>) -> WorkerLoadIndex {
        WorkerLoadIndex::new(
            2,
            workers
                .into_iter()
                .map(|worker| {
                    TransportWorkerPressureSnapshot::new(
                        worker,
                        TransportPlacementPressureSnapshot {
                            egress_bitrate: Bitrate::from_bps(512),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        )
    }

    fn egress_policy(
        activation_window: usize,
    ) -> Result<LocalSpilloverPolicy, LocalSpilloverPolicyError> {
        LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 99,
            egress_bitrate_threshold: Bitrate::from_bps(256),
            activation_window,
            ..LocalSpilloverPolicyParts::conservative()
        })
    }

    fn load_planner(policy: LocalSpilloverPolicy) -> RoomPlacementPlanner {
        RoomPlacementPlanner::new(
            2,
            RoomWorkerPolicy::load_triggered_local_spillover(2, policy),
        )
    }

    fn assert_reason(state: &LoadTriggeredPlacementState, reason: RoomPlacementDecisionReason) {
        assert_eq!(state.last_decision_reason(), Some(reason));
    }

    #[test]
    fn first_join_uses_lowest_load_worker() {
        let mut loads = WorkerLoadIndex::new(2, Vec::new());
        loads.record_session(0);
        let planner = RoomPlacementPlanner::new(2, RoomWorkerPolicy::strict_single_router());
        let room = RoomPlacementUsageSnapshot::new(RouterId(7), false, Vec::new());

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::AssignPrimary { media_worker_id: 1 }
        );
    }

    #[test]
    fn bounded_spillover_allocates_unused_worker_until_cap() {
        let mut loads = WorkerLoadIndex::new(3, Vec::new());
        loads.record_session(0);
        let planner = RoomPlacementPlanner::new(3, RoomWorkerPolicy::bounded_local_spillover(2));
        let room = primary_room();

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
    }

    #[test]
    fn strict_room_reuses_assigned_worker_after_it_becomes_empty() {
        let planner = RoomPlacementPlanner::new(3, RoomWorkerPolicy::strict_single_router());
        let room = room_with(vec![placement(7, 2)]);

        assert_eq!(
            planner.choose(&room, &WorkerLoadIndex::new(3, Vec::new())),
            RoomPlacementDecision::UseExisting(placement(7, 2))
        );
    }

    #[test]
    fn load_triggered_spillover_reuses_capable_room_worker() -> Result<(), LocalSpilloverPolicyError>
    {
        let mut loads = WorkerLoadIndex::new(2, Vec::new());
        loads.record_session(0);
        let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 4,
            ..LocalSpilloverPolicyParts::conservative()
        })?;
        let planner = load_planner(policy);
        let placement = primary_placement();
        let room = primary_room();

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::UseExisting(placement)
        );
        Ok(())
    }

    #[test]
    fn load_triggered_spillover_allocates_when_existing_worker_is_hot()
    -> Result<(), LocalSpilloverPolicyError> {
        let mut loads = hot_loads([0]);
        loads.record_session(0);
        let planner = load_planner(egress_policy(1)?);
        let room = primary_room();

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
        Ok(())
    }

    #[test]
    fn activation_window_delays_load_triggered_allocation() -> Result<(), LocalSpilloverPolicyError>
    {
        let mut loads = hot_loads([0]);
        loads.record_session(0);
        let planner = load_planner(egress_policy(2)?);
        let room = primary_room();
        let mut state = LoadTriggeredPlacementState::default();

        assert_eq!(
            planner.choose_with_load_state(&room, &loads, &mut state),
            RoomPlacementDecision::UseExisting(primary_placement())
        );
        assert_reason(&state, RoomPlacementDecisionReason::ActivationWindowNotMet);
        assert_eq!(
            planner.choose_with_load_state(&room, &loads, &mut state),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
        assert_reason(&state, RoomPlacementDecisionReason::EgressPressure);
        Ok(())
    }

    #[test]
    fn pressure_clearing_resets_activation_window() -> Result<(), LocalSpilloverPolicyError> {
        let hot = hot_loads([0]);
        let idle_loads = WorkerLoadIndex::new(2, Vec::new());
        let planner = load_planner(egress_policy(2)?);
        let placement = primary_placement();
        let room = primary_room();
        let mut state = LoadTriggeredPlacementState::default();

        assert_eq!(
            planner.choose_with_load_state(&room, &hot, &mut state),
            RoomPlacementDecision::UseExisting(placement)
        );
        assert_eq!(
            planner.choose_with_load_state(&room, &idle_loads, &mut state),
            RoomPlacementDecision::UseExisting(placement)
        );
        assert_reason(
            &state,
            RoomPlacementDecisionReason::ExistingPlacementAvailable,
        );
        assert_eq!(
            planner.choose_with_load_state(&room, &hot, &mut state),
            RoomPlacementDecision::UseExisting(placement)
        );
        assert_reason(&state, RoomPlacementDecisionReason::ActivationWindowNotMet);
        Ok(())
    }

    #[test]
    fn allocation_resets_activation_window() -> Result<(), LocalSpilloverPolicyError> {
        let loads = hot_loads([0]);
        let planner = load_planner(egress_policy(2)?);
        let placement = primary_placement();
        let room = primary_room();
        let mut state = LoadTriggeredPlacementState::default();

        assert_eq!(
            planner.choose_with_load_state(&room, &loads, &mut state),
            RoomPlacementDecision::UseExisting(placement)
        );
        assert_eq!(
            planner.choose_with_load_state(&room, &loads, &mut state),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
        assert_eq!(
            planner.choose_with_load_state(&room, &loads, &mut state),
            RoomPlacementDecision::UseExisting(placement)
        );
        assert_reason(&state, RoomPlacementDecisionReason::ActivationWindowNotMet);
        Ok(())
    }

    #[test]
    fn cap_reached_reuses_existing_placement_after_activation()
    -> Result<(), LocalSpilloverPolicyError> {
        let loads = hot_loads([0, 1]);
        let planner = load_planner(egress_policy(1)?);
        let first = primary_placement();
        let room = room_with(vec![first, placement(8, 1)]);
        let mut state = LoadTriggeredPlacementState::default();

        assert_eq!(
            planner.choose_with_load_state(&room, &loads, &mut state),
            RoomPlacementDecision::UseExisting(first)
        );
        assert_reason(&state, RoomPlacementDecisionReason::LocalRouterCapReached);
        Ok(())
    }

    #[test]
    fn stale_spillover_allocation_reuses_existing_placement_at_cap()
    -> Result<(), LocalSpilloverPolicyError> {
        let policy = RoomWorkerPolicy::load_triggered_local_spillover(2, egress_policy(1)?);
        let stale_room = primary_room();
        let planner = RoomPlacementPlanner::new(2, policy);
        let first_decision = planner.choose(&stale_room, &hot_loads([0]));
        let second_decision = planner.choose(&stale_room, &hot_loads([0]));
        let mut allocation_count = 0;

        let first_placement = JoinPlacement::planned(first_decision, hot_loads([0]), 2, policy)
            .resolve_for_commit(&stale_room, || {
                allocation_count += 1;
                RouterId(8)
            });
        let second_placement = JoinPlacement::planned(second_decision, hot_loads([0]), 2, policy)
            .resolve_for_commit(
                &room_with(vec![primary_placement(), placement(8, 1)]),
                || {
                    allocation_count += 1;
                    RouterId(9)
                },
            );

        assert_eq!(first_placement, placement(8, 1));
        assert_eq!(second_placement, placement(8, 1));
        assert_eq!(allocation_count, 1);
        Ok(())
    }

    #[test]
    fn stale_primary_assignment_keeps_committed_primary_worker() {
        let stale_decision = RoomPlacementDecision::AssignPrimary { media_worker_id: 1 };
        let mut allocation_count = 0;

        let placement = JoinPlacement::planned(
            stale_decision,
            WorkerLoadIndex::new(2, Vec::new()),
            2,
            RoomWorkerPolicy::strict_single_router(),
        )
        .resolve_for_commit(&primary_room(), || {
            allocation_count += 1;
            RouterId(8)
        });

        assert_eq!(placement, primary_placement());
        assert_eq!(allocation_count, 0);
    }

    #[test]
    fn stale_existing_placement_keeps_committed_worker() {
        let stale_decision = RoomPlacementDecision::UseExisting(placement(7, 1));
        let mut allocation_count = 0;

        let placement = JoinPlacement::planned(
            stale_decision,
            WorkerLoadIndex::new(2, Vec::new()),
            2,
            RoomWorkerPolicy::strict_single_router(),
        )
        .resolve_for_commit(&primary_room(), || {
            allocation_count += 1;
            RouterId(8)
        });

        assert_eq!(placement, primary_placement());
        assert_eq!(allocation_count, 0);
    }

    #[test]
    fn source_fanout_pressure_participates_in_activation() -> Result<(), LocalSpilloverPolicyError>
    {
        let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 99,
            egress_bitrate_threshold: Bitrate::from_bps(0),
            activation_window: 1,
            ..LocalSpilloverPolicyParts::conservative()
        })?;
        let planner = load_planner(policy);
        let room = primary_room();
        let mut state = LoadTriggeredPlacementState::default();
        state.set_source_fanout_pressure(true);

        assert_eq!(
            planner.choose_with_load_state(&room, &WorkerLoadIndex::new(2, Vec::new()), &mut state),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
        assert_reason(&state, RoomPlacementDecisionReason::SourceFanoutPressure);
        Ok(())
    }
}
