use std::collections::BTreeMap;

use o_sfu_router::RouterId;
pub use o_sfu_router::{
    RouterPlacement, RouterPlacements, RouterPlacementsError, RoutingPlacementSnapshot,
};

use super::{
    Room, RoomJoinError,
    membership::JoinUserRequest,
    state::{JoinUserOutcome, RoomState},
};
use crate::{
    LocalSpilloverPolicy, RoomSpilloverMode, RoomWorkerPolicy,
    engine::{
        MediaWorkerId, RoomInstanceId,
        media_transport::{TransportPlacementPressureSnapshot, TransportWorkerPressureSnapshot},
        sync::lock_unpoisoned,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRuntimeContext {
    instance: RoomInstanceId,
    primary_router: RouterId,
    initial_router_placements: Option<RouterPlacements>,
}

impl RoomRuntimeContext {
    #[must_use]
    pub fn new(
        instance: RoomInstanceId,
        primary: RouterPlacement,
        spillover: Vec<RouterPlacement>,
    ) -> Self {
        Self {
            instance,
            primary_router: primary.router,
            initial_router_placements: Some(RouterPlacements::new(primary, spillover)),
        }
    }

    #[must_use]
    pub const fn new_unassigned(instance: RoomInstanceId, primary_router: RouterId) -> Self {
        Self {
            instance,
            primary_router,
            initial_router_placements: None,
        }
    }

    /// # Errors
    ///
    /// returns [`RouterPlacementsError::Empty`] when `placements` is empty
    pub fn try_from_placements(
        instance: RoomInstanceId,
        placements: Vec<RouterPlacement>,
    ) -> Result<Self, RouterPlacementsError> {
        let routers = RouterPlacements::try_from_vec(placements)?;
        Ok(Self {
            instance,
            primary_router: routers.primary().router,
            initial_router_placements: Some(routers),
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

    #[must_use]
    pub fn initial_router_placements(&self) -> Option<&RouterPlacements> {
        self.initial_router_placements.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomPlacementDecision {
    AssignPrimary { media_worker_id: MediaWorkerId },
    UseExisting(RouterPlacement),
    AllocateSpillover { media_worker_id: MediaWorkerId },
}

#[derive(Debug)]
pub(super) struct PendingJoinPlacement {
    decision: RoomPlacementDecision,
    loads: WorkerLoadIndex,
    policy: RoomWorkerPolicy,
}

#[derive(Debug, Default)]
pub struct LoadTriggeredPlacementState {
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
    pub async fn placement_usage_snapshot(&self) -> RoutingPlacementSnapshot {
        self.state.read().await.placement_usage_snapshot()
    }

    pub async fn record_worker_load(&self, loads: &mut WorkerLoadIndex) {
        self.state.read().await.record_worker_load(loads);
    }

    pub(super) async fn plan_join_placement(
        &self,
        worker_loads: WorkerLoadIndex,
    ) -> PendingJoinPlacement {
        let room_snapshot = self.placement_usage_snapshot().await;
        let policy = self.room_worker_policy();
        let planner = RoomPlacementPlanner::new(policy);
        let decision = match policy.spillover() {
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_) => {
                self.observe_source_fanout_pressure().await;
                let mut load_state = lock_unpoisoned(&self.load_triggered_placement);
                planner.choose_with_load_state(&room_snapshot, &worker_loads, &mut load_state)
            }
            RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
                planner.choose(&room_snapshot, &worker_loads)
            }
        };
        PendingJoinPlacement::new(decision, worker_loads, policy)
    }

    pub async fn observe_source_fanout_pressure(&self) {
        let RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) =
            self.room_worker_policy().spillover()
        else {
            return;
        };
        let policy = policy.parts();
        let pressured = {
            let state = self.state.read().await;
            state.source_fanout_pressure(policy.max_fanout_per_source)
        };
        lock_unpoisoned(&self.load_triggered_placement).set_source_fanout_pressure(pressured);
    }
}

impl RoomState {
    fn record_worker_load(&self, loads: &mut WorkerLoadIndex) {
        let media = self.topology.media();
        let routing = self.topology.routing();
        for user in self.users.values() {
            loads.record_session(routing.media_worker_id_for_connection(user.connection_id));
        }
        for (_, connection_id) in media.committed_consumer_transport_entries() {
            loads.record_consumer(routing.media_worker_id_for_connection(connection_id));
        }
        for user_id in media.pending_consumer_user_ids() {
            let Some(user) = self.users.get(user_id) else {
                continue;
            };
            loads.record_consumer(routing.media_worker_id_for_connection(user.connection_id));
        }
    }
}

impl PendingJoinPlacement {
    fn new(
        decision: RoomPlacementDecision,
        loads: WorkerLoadIndex,
        policy: RoomWorkerPolicy,
    ) -> Self {
        Self {
            decision,
            loads,
            policy,
        }
    }

    pub(super) fn commit_join(
        self,
        state: &mut RoomState,
        request: JoinUserRequest,
        emit_joined_fanout: bool,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> Result<JoinUserOutcome, RoomJoinError> {
        let placement = self.resolve(&state.placement_usage_snapshot(), allocate_spillover_router);
        state.apply_join_on_placement(
            &request.user_id,
            request.permissions,
            request.sender,
            emit_joined_fanout,
            placement,
        )
    }

    fn resolve(
        self,
        room: &RoutingPlacementSnapshot,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> RouterPlacement {
        let Self {
            decision,
            loads,
            policy,
        } = self;
        let assigned_placements = room.assigned_placements();
        let score_policy = score_policy(policy);
        let Some(first_assigned) = assigned_placements.first().copied() else {
            return match decision {
                RoomPlacementDecision::AssignPrimary { media_worker_id }
                | RoomPlacementDecision::AllocateSpillover { media_worker_id } => RouterPlacement {
                    router: room.primary_router(),
                    media_worker: media_worker_id,
                },
                RoomPlacementDecision::UseExisting(placement) => RouterPlacement {
                    router: room.primary_router(),
                    media_worker: placement.media_worker,
                },
            };
        };
        match decision {
            RoomPlacementDecision::AssignPrimary { .. } => {
                loads.least_loaded_placement(assigned_placements, first_assigned, score_policy)
            }
            RoomPlacementDecision::UseExisting(placement) => {
                if let Some(assigned) = assigned_placements
                    .iter()
                    .find(|assigned| assigned.router == placement.router)
                {
                    return *assigned;
                }
                loads.least_loaded_placement(assigned_placements, first_assigned, score_policy)
            }
            RoomPlacementDecision::AllocateSpillover { .. } => {
                let placement_cap = policy.max_local_routers().min(loads.worker_count()).max(1);
                if assigned_placements.len() >= placement_cap {
                    return loads.least_loaded_placement(
                        assigned_placements,
                        first_assigned,
                        score_policy,
                    );
                }
                // router allocation stays in final resolution so stale plans do
                // not reserve spillover the room no longer needs
                RouterPlacement {
                    router: allocate_spillover_router(),
                    media_worker: loads.least_loaded_worker(assigned_placements, score_policy),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerPlacementLoad {
    media_worker_id: MediaWorkerId,
    session_count: usize,
    consumer_count: usize,
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

/// ordered worker score where field order defines the tie-break policy
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

#[derive(Debug)]
pub struct WorkerLoadIndex {
    loads: Vec<WorkerPlacementLoad>,
}

impl WorkerLoadIndex {
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

    #[expect(
        clippy::unreachable,
        reason = "WorkerLoadIndex::new normalizes the worker count to at least one load"
    )]
    fn least_loaded_worker(
        &self,
        excluded_placements: &[RouterPlacement],
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

    fn least_loaded_placement(
        &self,
        placements: &[RouterPlacement],
        fallback: RouterPlacement,
        policy: LocalSpilloverPolicy,
    ) -> RouterPlacement {
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

#[derive(Debug, Clone)]
pub(super) struct RoomPlacementPlanner {
    policy: RoomWorkerPolicy,
}

impl RoomPlacementPlanner {
    #[must_use]
    pub(super) const fn new(policy: RoomWorkerPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub(super) fn choose(
        &self,
        room: &RoutingPlacementSnapshot,
        load_index: &WorkerLoadIndex,
    ) -> RoomPlacementDecision {
        let mut load_state = LoadTriggeredPlacementState::default();
        self.choose_with_load_state(room, load_index, &mut load_state)
    }

    pub(super) fn choose_with_load_state(
        &self,
        room: &RoutingPlacementSnapshot,
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

fn score_policy(policy: RoomWorkerPolicy) -> LocalSpilloverPolicy {
    match policy.spillover() {
        RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => policy,
        RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
            LocalSpilloverPolicy::conservative()
        }
    }
}

#[cfg(test)]
#[path = "TESTS/placement.rs"]
mod tests;
