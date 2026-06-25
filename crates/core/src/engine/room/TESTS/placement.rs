use o_sfu_router::RouterId;

use super::*;
use crate::{
    Bitrate,
    prelude::{LocalSpilloverPolicyError, LocalSpilloverPolicyParts},
};

fn worker_id(raw: usize) -> MediaWorkerId {
    MediaWorkerId::from_raw(raw)
}

fn placement(router: u64, media_worker: usize) -> RouterPlacement {
    RouterPlacement {
        router: RouterId(router),
        media_worker: worker_id(media_worker),
    }
}

fn primary_placement() -> RouterPlacement {
    placement(7, 0)
}

fn room_with(placements: Vec<RouterPlacement>) -> RoutingPlacementSnapshot {
    RoutingPlacementSnapshot::new(RouterId(7), true, placements)
}

fn primary_room() -> RoutingPlacementSnapshot {
    room_with(vec![primary_placement()])
}

fn hot_loads(workers: impl IntoIterator<Item = usize>) -> WorkerLoadIndex {
    WorkerLoadIndex::new(
        2,
        workers
            .into_iter()
            .map(|worker| {
                TransportWorkerPressureSnapshot::new(
                    worker_id(worker),
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
    RoomPlacementPlanner::new(RoomWorkerPolicy::load_triggered_local_spillover(2, policy))
}

fn resolve_stale_placement(
    decision: RoomPlacementDecision,
    loads: WorkerLoadIndex,
    policy: RoomWorkerPolicy,
    placement_usage: &RoutingPlacementSnapshot,
    allocate_spillover_router: impl FnOnce() -> RouterId,
) -> RouterPlacement {
    JoinAdmission {
        decision,
        loads,
        policy,
    }
    .resolve_placement(placement_usage, allocate_spillover_router)
}

#[test]
fn first_join_uses_lowest_load_worker() {
    let mut loads = WorkerLoadIndex::new(2, Vec::new());
    loads.record_session(worker_id(0));
    let planner = RoomPlacementPlanner::new(RoomWorkerPolicy::strict_single_router());
    let room = RoutingPlacementSnapshot::new(RouterId(7), false, Vec::new());

    assert_eq!(
        planner.choose(&room, &loads),
        RoomPlacementDecision::AssignPrimary {
            media_worker_id: worker_id(1)
        }
    );
}

#[test]
fn bounded_spillover_allocates_unused_worker_until_cap() {
    let mut loads = WorkerLoadIndex::new(3, Vec::new());
    loads.record_session(worker_id(0));
    let planner = RoomPlacementPlanner::new(RoomWorkerPolicy::bounded_local_spillover(2));
    let room = primary_room();

    assert_eq!(
        planner.choose(&room, &loads),
        RoomPlacementDecision::AllocateSpillover {
            media_worker_id: worker_id(1)
        }
    );
}

#[test]
fn strict_room_reuses_assigned_worker_after_it_becomes_empty() {
    let planner = RoomPlacementPlanner::new(RoomWorkerPolicy::strict_single_router());
    let room = room_with(vec![placement(7, 2)]);

    assert_eq!(
        planner.choose(&room, &WorkerLoadIndex::new(3, Vec::new())),
        RoomPlacementDecision::UseExisting(placement(7, 2))
    );
}

#[test]
fn load_triggered_spillover_reuses_capable_room_worker() -> Result<(), LocalSpilloverPolicyError> {
    let mut loads = WorkerLoadIndex::new(2, Vec::new());
    loads.record_session(worker_id(0));
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
    loads.record_session(worker_id(0));
    let planner = load_planner(egress_policy(1)?);
    let room = primary_room();

    assert_eq!(
        planner.choose(&room, &loads),
        RoomPlacementDecision::AllocateSpillover {
            media_worker_id: worker_id(1)
        }
    );
    Ok(())
}

#[test]
fn activation_window_delays_load_triggered_allocation() -> Result<(), LocalSpilloverPolicyError> {
    let mut loads = hot_loads([0]);
    loads.record_session(worker_id(0));
    let planner = load_planner(egress_policy(2)?);
    let room = primary_room();
    let mut state = LoadTriggeredPlacementState::default();

    assert_eq!(
        planner.choose_with_load_state(&room, &loads, &mut state),
        RoomPlacementDecision::UseExisting(primary_placement())
    );
    assert_eq!(
        planner.choose_with_load_state(&room, &loads, &mut state),
        RoomPlacementDecision::AllocateSpillover {
            media_worker_id: worker_id(1)
        }
    );
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
    assert_eq!(
        planner.choose_with_load_state(&room, &hot, &mut state),
        RoomPlacementDecision::UseExisting(placement)
    );
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
        RoomPlacementDecision::AllocateSpillover {
            media_worker_id: worker_id(1)
        }
    );
    assert_eq!(
        planner.choose_with_load_state(&room, &loads, &mut state),
        RoomPlacementDecision::UseExisting(placement)
    );
    Ok(())
}

#[test]
fn cap_reached_reuses_existing_placement_after_activation() -> Result<(), LocalSpilloverPolicyError>
{
    let loads = hot_loads([0, 1]);
    let planner = load_planner(egress_policy(1)?);
    let first = primary_placement();
    let room = room_with(vec![first, placement(8, 1)]);
    let mut state = LoadTriggeredPlacementState::default();

    assert_eq!(
        planner.choose_with_load_state(&room, &loads, &mut state),
        RoomPlacementDecision::UseExisting(first)
    );
    Ok(())
}

#[test]
fn stale_spillover_allocation_reuses_existing_placement_at_cap()
-> Result<(), LocalSpilloverPolicyError> {
    let policy = RoomWorkerPolicy::load_triggered_local_spillover(2, egress_policy(1)?);
    let stale_room = primary_room();
    let planner = RoomPlacementPlanner::new(policy);
    let first_decision = planner.choose(&stale_room, &hot_loads([0]));
    let second_decision = planner.choose(&stale_room, &hot_loads([0]));
    let mut allocation_count = 0;

    let first_placement =
        resolve_stale_placement(first_decision, hot_loads([0]), policy, &stale_room, || {
            allocation_count += 1;
            RouterId(8)
        });
    let second_placement = resolve_stale_placement(
        second_decision,
        hot_loads([0]),
        policy,
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
    let stale_decision = RoomPlacementDecision::AssignPrimary {
        media_worker_id: worker_id(1),
    };
    let mut allocation_count = 0;

    let placement = resolve_stale_placement(
        stale_decision,
        WorkerLoadIndex::new(2, Vec::new()),
        RoomWorkerPolicy::strict_single_router(),
        &primary_room(),
        || {
            allocation_count += 1;
            RouterId(8)
        },
    );

    assert_eq!(placement, primary_placement());
    assert_eq!(allocation_count, 0);
}

#[test]
fn stale_existing_placement_keeps_committed_worker() {
    let stale_decision = RoomPlacementDecision::UseExisting(placement(7, 1));
    let mut allocation_count = 0;

    let placement = resolve_stale_placement(
        stale_decision,
        WorkerLoadIndex::new(2, Vec::new()),
        RoomWorkerPolicy::strict_single_router(),
        &primary_room(),
        || {
            allocation_count += 1;
            RouterId(8)
        },
    );

    assert_eq!(placement, primary_placement());
    assert_eq!(allocation_count, 0);
}
#[test]
fn source_fanout_pressure_participates_in_activation() -> Result<(), LocalSpilloverPolicyError> {
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
        RoomPlacementDecision::AllocateSpillover {
            media_worker_id: worker_id(1)
        }
    );
    Ok(())
}
