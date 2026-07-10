use o_sfu_router::{Router, RouterId, rtp::MediaCapabilities};

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

fn room_with(primary: RouterPlacement, spillover: Vec<RouterPlacement>) -> PlacementSnapshot {
    Router::with_placements(
        RouterPlacements::new(primary, spillover),
        MediaCapabilities::new(Vec::new(), Vec::new()),
    )
    .placement_snapshot()
}

fn primary_room() -> PlacementSnapshot {
    room_with(primary_placement(), Vec::new())
}

fn unassigned_room() -> PlacementSnapshot {
    Router::new(RouterId(7), MediaCapabilities::new(Vec::new(), Vec::new())).placement_snapshot()
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
    placement_usage: &PlacementSnapshot,
    allocate_spillover_router: impl FnOnce() -> RouterId,
) -> RouterPlacement {
    JoinPlacementPlan {
        decision,
        loads,
        policy,
    }
    .resolve(placement_usage, allocate_spillover_router)
}

#[test]
fn first_join_uses_lowest_load_worker() {
    let mut loads = WorkerLoadIndex::new(2, Vec::new());
    loads.record_session(worker_id(0));
    let planner = RoomPlacementPlanner::new(RoomWorkerPolicy::strict_single_router());
    let room = unassigned_room();

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
    let room = room_with(placement(7, 2), Vec::new());

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
    let room = room_with(first, vec![placement(8, 1)]);
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
    let mut spillover_allocations = 0;

    let first_placement = resolve_stale_placement(
        planner.choose(&stale_room, &hot_loads([0])),
        hot_loads([0]),
        policy,
        &stale_room,
        || {
            spillover_allocations += 1;
            RouterId(8)
        },
    );
    let second_placement = resolve_stale_placement(
        planner.choose(&stale_room, &hot_loads([0])),
        hot_loads([0]),
        policy,
        &room_with(primary_placement(), vec![placement(8, 1)]),
        || {
            spillover_allocations += 1;
            RouterId(9)
        },
    );

    assert_eq!(first_placement, placement(8, 1));
    assert_eq!(second_placement, placement(8, 1));
    assert_eq!(spillover_allocations, 1);
    Ok(())
}

#[test]
fn stale_primary_assignment_keeps_committed_primary_worker() {
    let mut spillover_allocations = 0;
    let resolved = resolve_stale_placement(
        RoomPlacementDecision::AssignPrimary {
            media_worker_id: worker_id(1),
        },
        WorkerLoadIndex::new(2, Vec::new()),
        RoomWorkerPolicy::strict_single_router(),
        &primary_room(),
        || {
            spillover_allocations += 1;
            RouterId(8)
        },
    );

    assert_eq!(resolved, primary_placement());
    assert_eq!(spillover_allocations, 0);
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
