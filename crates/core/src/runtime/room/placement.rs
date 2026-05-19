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

/// room-local committed placement ledger
///
/// joins register the selected router and media worker for each committed
/// connection. later transport commands use the same mapping to build stable
/// session keys without deriving placement from several room indexes
#[derive(Debug)]
pub(super) struct RoomPlacementLedger {
    primary_placement: Mutex<LocalRouterRuntimeContext>,
    placement_by_connection: Mutex<BTreeMap<ConnectionId, LocalRouterRuntimeContext>>,
}

impl RoomPlacementLedger {
    #[must_use]
    pub(super) fn new(primary: LocalRouterRuntimeContext) -> Self {
        Self {
            primary_placement: Mutex::new(primary),
            placement_by_connection: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub(super) fn transport_user_key(
        &self,
        instance_id: RoomInstanceId,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        TransportSessionKey::new(
            instance_id,
            self.media_worker_id_for_connection(connection_id),
            connection_id,
            user_id.clone(),
        )
    }

    pub(super) fn register_committed_placement(
        &self,
        connection_id: ConnectionId,
        placement: LocalRouterRuntimeContext,
    ) {
        lock_unpoisoned(&self.placement_by_connection).insert(connection_id, placement);
        let mut primary = lock_unpoisoned(&self.primary_placement);
        if placement.router == primary.router {
            *primary = placement;
        }
    }

    pub(super) fn unregister_committed_placement(&self, connection_id: ConnectionId) {
        lock_unpoisoned(&self.placement_by_connection).remove(&connection_id);
    }

    fn placement_for_connection(&self, connection_id: ConnectionId) -> LocalRouterRuntimeContext {
        if let Some(placement) = lock_unpoisoned(&self.placement_by_connection)
            .get(&connection_id)
            .copied()
        {
            return placement;
        }
        *lock_unpoisoned(&self.primary_placement)
    }

    pub(super) fn media_worker_id_for_connection(&self, connection_id: ConnectionId) -> usize {
        self.placement_for_connection(connection_id).media_worker
    }

    pub(super) fn media_worker_id(&self) -> usize {
        lock_unpoisoned(&self.primary_placement).media_worker
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

impl RoomPlacementDecision {
    pub(super) fn resolve(
        self,
        room: &RoomPlacementUsageSnapshot,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> LocalRouterRuntimeContext {
        match self {
            Self::AssignPrimary { media_worker_id } => LocalRouterRuntimeContext {
                router: room.primary_router(),
                media_worker: media_worker_id,
            },
            Self::UseExisting(placement) => placement,
            Self::AllocateSpillover { media_worker_id } => LocalRouterRuntimeContext {
                router: allocate_spillover_router(),
                media_worker: media_worker_id,
            },
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

    /// whether adding another receiver would cross any spillover trigger
    fn is_overloaded(self, policy: LocalSpilloverPolicy) -> bool {
        self.session_count.saturating_add(1) >= policy.min_receiver_count()
            || self.consumer_count >= policy.max_active_consumers_per_router()
            || policy.egress_bitrate_threshold() > crate::Bitrate::zero()
                && self.pressure.egress_bitrate >= policy.egress_bitrate_threshold()
            || policy.packet_loop_lag_threshold_ms() > 0
                && self.pressure.packet_loop_lag_ms >= policy.packet_loop_lag_threshold_ms()
            || policy.command_backlog_threshold() > 0
                && self.pressure.command_backlog_depth >= policy.command_backlog_threshold()
            || policy.relay_mailbox_depth_threshold() > 0
                && self.pressure.relay_mailbox_depth >= policy.relay_mailbox_depth_threshold()
            || policy.worker_pressure_threshold() > 0
                && self.pressure.worker_pressure_score >= policy.worker_pressure_threshold()
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
        let placement_cap = self
            .policy
            .max_local_routers()
            .min(self.media_worker_count)
            .max(1);
        let score_policy = score_policy(self.policy);
        let assigned_placements = room.assigned_placements();
        if assigned_placements.is_empty() {
            return RoomPlacementDecision::AssignPrimary {
                media_worker_id: load_index.least_loaded_worker(&[], score_policy),
            };
        }
        match self.policy.spillover() {
            RoomSpilloverMode::StrictSingleRouter => {
                RoomPlacementDecision::UseExisting(assigned_placements.first().copied().unwrap_or(
                    LocalRouterRuntimeContext {
                        router: room.primary_router(),
                        media_worker: 0,
                    },
                ))
            }
            RoomSpilloverMode::BoundedLocalSpillover => {
                if assigned_placements.len() < placement_cap {
                    return RoomPlacementDecision::AllocateSpillover {
                        media_worker_id: load_index
                            .least_loaded_worker(assigned_placements, score_policy),
                    };
                }
                RoomPlacementDecision::UseExisting(
                    load_index.least_loaded_placement(assigned_placements, score_policy),
                )
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let placement = load_index.least_loaded_placement(assigned_placements, policy);
                if !load_index
                    .load_for_worker(placement.media_worker)
                    .is_overloaded(policy)
                {
                    return RoomPlacementDecision::UseExisting(placement);
                }
                if assigned_placements.len() < placement_cap {
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
    use crate::{Bitrate, LocalSpilloverPolicyError, LocalSpilloverPolicyParts};

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
        let room = RoomPlacementUsageSnapshot::new(
            RouterId(7),
            true,
            vec![LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 0,
            }],
        );

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
    }

    #[test]
    fn strict_room_reuses_assigned_worker_after_it_becomes_empty() {
        let planner = RoomPlacementPlanner::new(3, RoomWorkerPolicy::strict_single_router());
        let room = RoomPlacementUsageSnapshot::new(
            RouterId(7),
            true,
            vec![LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 2,
            }],
        );

        assert_eq!(
            planner.choose(&room, &WorkerLoadIndex::new(3, Vec::new())),
            RoomPlacementDecision::UseExisting(LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 2,
            })
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
        let planner = RoomPlacementPlanner::new(
            2,
            RoomWorkerPolicy::load_triggered_local_spillover(2, policy),
        );
        let placement = LocalRouterRuntimeContext {
            router: RouterId(7),
            media_worker: 0,
        };
        let room = RoomPlacementUsageSnapshot::new(RouterId(7), true, vec![placement]);

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::UseExisting(placement)
        );
        Ok(())
    }

    #[test]
    fn load_triggered_spillover_allocates_when_existing_worker_is_hot()
    -> Result<(), LocalSpilloverPolicyError> {
        let mut loads = WorkerLoadIndex::new(
            2,
            vec![TransportWorkerPressureSnapshot::new(
                0,
                TransportPlacementPressureSnapshot {
                    egress_bitrate: Bitrate::from_bps(512),
                    ..Default::default()
                },
            )],
        );
        loads.record_session(0);
        let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 99,
            egress_bitrate_threshold: Bitrate::from_bps(256),
            ..LocalSpilloverPolicyParts::conservative()
        })?;
        let planner = RoomPlacementPlanner::new(
            2,
            RoomWorkerPolicy::load_triggered_local_spillover(2, policy),
        );
        let room = RoomPlacementUsageSnapshot::new(
            RouterId(7),
            true,
            vec![LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 0,
            }],
        );

        assert_eq!(
            planner.choose(&room, &loads),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
        Ok(())
    }
}
