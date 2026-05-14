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

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::RouterId;

use super::LocalRouterRuntimeContext;
use crate::{
    LocalSpilloverPolicy, RoomShardingPolicy, RoomSpilloverMode,
    runtime::media_transport::{
        TransportPlacementPressureSnapshot, TransportWorkerPressureSnapshot,
    },
};

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

/// aggregate placement load for one local media worker
///
/// the room manager builds this from committed room mappings plus
/// transport-observed pressure
/// each value is a one-join snapshot rather than a continuously maintained
/// counter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WorkerPlacementLoad {
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
    pub(super) const fn new(
        media_worker_id: usize,
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
#[derive(Debug, Clone)]
pub(super) struct WorkerPlacementLoadSet {
    worker_count: usize,
    loads: BTreeMap<usize, WorkerPlacementLoad>,
}

impl WorkerPlacementLoadSet {
    /// create a complete worker load set from best-effort transport pressure
    ///
    /// missing snapshots are treated as idle workers
    /// snapshot worker ids are normalized by the configured worker count because
    /// fake transports can replay stale test data across differently sized
    /// runtimes
    #[must_use]
    pub(super) fn new(
        worker_count: usize,
        pressure_snapshots: Vec<TransportWorkerPressureSnapshot>,
    ) -> Self {
        let worker_count = worker_count.max(1);
        let mut loads = (0..worker_count)
            .map(|media_worker_id| {
                (
                    media_worker_id,
                    WorkerPlacementLoad::new(
                        media_worker_id,
                        TransportPlacementPressureSnapshot::default(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for snapshot in pressure_snapshots {
            let media_worker_id = snapshot.media_worker_id % worker_count;
            loads.insert(
                media_worker_id,
                WorkerPlacementLoad::new(media_worker_id, snapshot.pressure),
            );
        }
        Self {
            worker_count,
            loads,
        }
    }

    pub(super) fn record_session(&mut self, media_worker_id: usize) {
        let media_worker_id = media_worker_id % self.worker_count;
        if let Some(load) = self.loads.get_mut(&media_worker_id) {
            load.record_session();
        }
    }

    pub(super) fn record_consumer(&mut self, media_worker_id: usize) {
        let media_worker_id = media_worker_id % self.worker_count;
        if let Some(load) = self.loads.get_mut(&media_worker_id) {
            load.record_consumer();
        }
    }

    #[must_use]
    pub(super) fn into_loads(self) -> Vec<WorkerPlacementLoad> {
        self.loads.into_values().collect()
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
    policy: RoomShardingPolicy,
}

impl RoomPlacementPlanner {
    /// create a planner for the configured local worker set and room policy
    #[must_use]
    pub(super) fn new(media_worker_count: usize, policy: RoomShardingPolicy) -> Self {
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
        worker_loads: &[WorkerPlacementLoad],
    ) -> RoomPlacementDecision {
        let load_index = WorkerLoadIndex::new(self.media_worker_count, worker_loads);
        let placement_cap = self
            .policy
            .max_local_routers()
            .min(self.media_worker_count)
            .max(1);
        let score_policy = score_policy(self.policy);
        let assigned_placements = room.assigned_placements();
        if assigned_placements.is_empty() {
            return RoomPlacementDecision::AssignPrimary {
                media_worker_id: load_index.least_loaded_worker(&BTreeSet::new(), score_policy),
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
                            .least_loaded_worker(&used_workers(assigned_placements), score_policy),
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
                            .least_loaded_worker(&used_workers(assigned_placements), policy),
                    };
                }
                RoomPlacementDecision::UseExisting(placement)
            }
        }
    }
}

/// lookup table that gives the planner a stable score for every worker
///
/// callers can pass sparse data
/// the index fills missing workers with idle loads so an unused worker can
/// still win placement
#[derive(Debug)]
struct WorkerLoadIndex {
    media_worker_count: usize,
    loads: BTreeMap<usize, WorkerPlacementLoad>,
}

impl WorkerLoadIndex {
    fn new(media_worker_count: usize, worker_loads: &[WorkerPlacementLoad]) -> Self {
        let media_worker_count = media_worker_count.max(1);
        let mut loads = (0..media_worker_count)
            .map(|media_worker_id| {
                (
                    media_worker_id,
                    WorkerPlacementLoad::new(
                        media_worker_id,
                        TransportPlacementPressureSnapshot::default(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for load in worker_loads {
            let media_worker_id = load.media_worker_id % media_worker_count;
            loads.insert(
                media_worker_id,
                WorkerPlacementLoad {
                    media_worker_id,
                    ..*load
                },
            );
        }
        Self {
            media_worker_count,
            loads,
        }
    }

    fn load_for_worker(&self, media_worker_id: usize) -> WorkerPlacementLoad {
        let media_worker_id = media_worker_id % self.media_worker_count;
        self.loads
            .get(&media_worker_id)
            .copied()
            .unwrap_or_else(|| {
                WorkerPlacementLoad::new(
                    media_worker_id,
                    TransportPlacementPressureSnapshot::default(),
                )
            })
    }

    fn least_loaded_worker(
        &self,
        excluded_workers: &BTreeSet<usize>,
        policy: LocalSpilloverPolicy,
    ) -> usize {
        self.loads
            .values()
            .filter(|load| !excluded_workers.contains(&load.media_worker_id))
            .min_by_key(|load| load.score(policy))
            .or_else(|| self.loads.values().min_by_key(|load| load.score(policy)))
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

fn score_policy(policy: RoomShardingPolicy) -> LocalSpilloverPolicy {
    match policy.spillover() {
        RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => policy,
        RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
            LocalSpilloverPolicy::conservative()
        }
    }
}

fn used_workers(placements: &[LocalRouterRuntimeContext]) -> BTreeSet<usize> {
    placements
        .iter()
        .map(|placement| placement.media_worker)
        .collect()
}

#[cfg(test)]
mod tests {
    use o_sfu_router::RouterId;

    use super::*;
    use crate::{Bitrate, LocalSpilloverPolicyError, LocalSpilloverPolicyParts};

    #[test]
    fn first_join_uses_lowest_load_worker() {
        let mut loads = WorkerPlacementLoadSet::new(2, Vec::new());
        loads.record_session(0);
        let planner = RoomPlacementPlanner::new(2, RoomShardingPolicy::strict_single_router());
        let room = RoomPlacementUsageSnapshot::new(RouterId(7), false, Vec::new());

        assert_eq!(
            planner.choose(&room, &loads.into_loads()),
            RoomPlacementDecision::AssignPrimary { media_worker_id: 1 }
        );
    }

    #[test]
    fn bounded_spillover_allocates_unused_worker_until_cap() {
        let mut loads = WorkerPlacementLoadSet::new(3, Vec::new());
        loads.record_session(0);
        let planner = RoomPlacementPlanner::new(3, RoomShardingPolicy::bounded_local_spillover(2));
        let room = RoomPlacementUsageSnapshot::new(
            RouterId(7),
            true,
            vec![LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 0,
            }],
        );

        assert_eq!(
            planner.choose(&room, &loads.into_loads()),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
    }

    #[test]
    fn strict_room_reuses_assigned_worker_after_it_becomes_empty() {
        let planner = RoomPlacementPlanner::new(3, RoomShardingPolicy::strict_single_router());
        let room = RoomPlacementUsageSnapshot::new(
            RouterId(7),
            true,
            vec![LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 2,
            }],
        );

        assert_eq!(
            planner.choose(
                &room,
                &WorkerPlacementLoadSet::new(3, Vec::new()).into_loads()
            ),
            RoomPlacementDecision::UseExisting(LocalRouterRuntimeContext {
                router: RouterId(7),
                media_worker: 2,
            })
        );
    }

    #[test]
    fn load_triggered_spillover_reuses_capable_room_worker() -> Result<(), LocalSpilloverPolicyError>
    {
        let mut loads = WorkerPlacementLoadSet::new(2, Vec::new());
        loads.record_session(0);
        let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            min_receiver_count: 4,
            ..LocalSpilloverPolicyParts::conservative()
        })?;
        let planner = RoomPlacementPlanner::new(
            2,
            RoomShardingPolicy::load_triggered_local_spillover(2, policy),
        );
        let placement = LocalRouterRuntimeContext {
            router: RouterId(7),
            media_worker: 0,
        };
        let room = RoomPlacementUsageSnapshot::new(RouterId(7), true, vec![placement]);

        assert_eq!(
            planner.choose(&room, &loads.into_loads()),
            RoomPlacementDecision::UseExisting(placement)
        );
        Ok(())
    }

    #[test]
    fn load_triggered_spillover_allocates_when_existing_worker_is_hot()
    -> Result<(), LocalSpilloverPolicyError> {
        let mut loads = WorkerPlacementLoadSet::new(
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
            RoomShardingPolicy::load_triggered_local_spillover(2, policy),
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
            planner.choose(&room, &loads.into_loads()),
            RoomPlacementDecision::AllocateSpillover { media_worker_id: 1 }
        );
        Ok(())
    }
}
