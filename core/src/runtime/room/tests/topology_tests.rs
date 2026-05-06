use o_sfu_router::{ProducerId as RouterProducerId, RouterError};

use super::fixtures::*;
use crate::{
    LocalSpilloverPolicy,
    runtime::room::{
        LocalRoomRouterPlacements, LocalRoomRouterPlacementsError,
        router_state::RoomRouterStateError,
        topology::{
            LoadPressureReason, RoomTopologyError, RoutedProducerId, TopologyPressureSnapshot,
        },
    },
};

#[test]
fn topology_assigns_the_primary_router_to_joined_users() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);

    assert!(
        topology
            .apply_client_join_with_pressure(&user_id, 42, TopologyPressureSnapshot::default())
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_user(&user_id),
        Some(RouterId(7))
    );
    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_rejoin_does_not_duplicate_router_users() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);

    assert!(
        topology
            .apply_client_join_with_pressure(&user_id, 42, TopologyPressureSnapshot::default())
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(&user_id, 43, TopologyPressureSnapshot::default())
            .is_ok()
    );

    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_returns_router_scoped_entity_handles() {
    let mut topology = RoomTopology::new(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    for (seed, user_id) in [(10, &producer_user_id), (20, &consumer_user_id)] {
        assert!(
            topology
                .apply_client_join_with_pressure(user_id, seed, TopologyPressureSnapshot::default())
                .is_ok()
        );
    }

    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .ok();
    assert!(producer.is_some());
    let Some(producer) = producer else {
        return;
    };

    let consumer = topology
        .add_consumer(
            &consumer_user_id,
            producer,
            RouterMediaKind::Audio,
            ConsumerCapability::Compatible,
        )
        .ok();
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };

    assert_eq!(producer.router_id(), RouterId(9));
    assert_eq!(consumer.router_id(), RouterId(9));
}

#[test]
fn topology_attaches_spillover_router_for_bounded_policy() {
    let mut topology = RoomTopology::new_with_bounded_spillover(RouterId(9), 2);
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &first_user_id,
                0,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &second_user_id,
                1,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_user(&first_user_id),
        Some(RouterId(9))
    );
    assert_eq!(
        topology.home_router_id_for_user(&second_user_id),
        Some(RouterId(10))
    );
    assert_eq!(topology.router_count(), 2);
    assert_eq!(topology.user_count(), 2);
}

#[test]
fn topology_replacement_rehomes_from_the_new_connection_seed() {
    let mut topology = RoomTopology::new_with_bounded_spillover(RouterId(9), 2);
    let user_id = UserId::Integer(10);

    assert!(
        topology
            .apply_client_join_with_pressure(&user_id, 0, TopologyPressureSnapshot::default())
            .is_ok()
    );
    assert_eq!(
        topology.home_router_id_for_user(&user_id),
        Some(RouterId(9))
    );

    assert!(
        topology
            .replace_client_session_with_pressure(&user_id, 1, TopologyPressureSnapshot::default())
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_user(&user_id),
        Some(RouterId(10))
    );
    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_routes_cross_router_consumers_through_source_router() {
    let mut topology = RoomTopology::new_with_bounded_spillover(RouterId(9), 2);
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &producer_user_id,
                0,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &consumer_user_id,
                1,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );

    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .ok();
    assert!(producer.is_some());
    let Some(producer) = producer else {
        return;
    };
    let consumer = topology
        .add_consumer(
            &consumer_user_id,
            producer,
            RouterMediaKind::Audio,
            ConsumerCapability::Compatible,
        )
        .ok();
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };

    assert_eq!(producer.router_id(), RouterId(9));
    assert_eq!(consumer.router_id(), RouterId(9));
    assert_eq!(
        topology.home_router_id_for_user(&consumer_user_id),
        Some(RouterId(10))
    );
}

#[test]
fn topology_prunes_receiver_shadow_when_cross_router_source_leaves_first() {
    let mut topology = RoomTopology::new_with_bounded_spillover(RouterId(9), 2);
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &producer_user_id,
                0,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &consumer_user_id,
                1,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );

    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .ok();
    assert!(producer.is_some());
    let Some(producer) = producer else {
        return;
    };
    let consumer = topology
        .add_consumer(
            &consumer_user_id,
            producer,
            RouterMediaKind::Audio,
            ConsumerCapability::Compatible,
        )
        .ok();
    assert!(consumer.is_some());

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(topology.remove_session(&producer_user_id).is_ok());

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(0)
    );
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(10)),
        Some(1)
    );
    assert_eq!(
        topology.home_router_id_for_user(&consumer_user_id),
        Some(RouterId(10))
    );
}

#[test]
fn topology_keeps_receiver_shadow_until_last_source_router_consumer_is_removed() {
    let mut topology = RoomTopology::new_with_bounded_spillover(RouterId(9), 2);
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    for (seed, user_id) in [(0, &producer_user_id), (1, &consumer_user_id)] {
        assert!(
            topology
                .apply_client_join_with_pressure(user_id, seed, TopologyPressureSnapshot::default())
                .is_ok()
        );
    }
    let first_producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .ok();
    let second_producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .ok();
    assert!(first_producer.is_some());
    assert!(second_producer.is_some());
    let (Some(first_producer), Some(second_producer)) = (first_producer, second_producer) else {
        return;
    };

    for producer in [first_producer, second_producer] {
        assert!(
            topology
                .add_consumer(
                    &consumer_user_id,
                    producer,
                    RouterMediaKind::Audio,
                    ConsumerCapability::Compatible,
                )
                .is_ok()
        );
    }

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(topology.remove_producer(first_producer).is_ok());
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(topology.remove_producer(second_producer).is_ok());
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(1)
    );
}

#[test]
fn topology_rejects_shadow_consumer_without_receiver_home_placement() {
    let mut topology = RoomTopology::new(RouterId(9));
    let missing_consumer_user_id = UserId::Integer(20);

    assert_eq!(
        topology.add_consumer(
            &missing_consumer_user_id,
            RoutedProducerId::new(RouterId(9), RouterProducerId(1)),
            RouterMediaKind::Audio,
            ConsumerCapability::Compatible,
        ),
        Err(RoomTopologyError::MissingSessionPlacement {
            user_id: missing_consumer_user_id,
        })
    );
}

#[test]
fn topology_rejects_consumer_on_unreserved_router() {
    let mut topology = RoomTopology::new(RouterId(9));
    let consumer_user_id = UserId::Integer(20);
    assert!(
        topology
            .apply_client_join_with_pressure(
                &consumer_user_id,
                0,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );

    assert_eq!(
        topology.add_consumer(
            &consumer_user_id,
            RoutedProducerId::new(RouterId(99), RouterProducerId(1)),
            RouterMediaKind::Audio,
            ConsumerCapability::Compatible,
        ),
        Err(RoomTopologyError::UnreservedRouter {
            router_id: RouterId(99),
        })
    );
}

#[test]
fn topology_detaches_idle_spillover_router_after_last_home_session_leaves() {
    let mut topology = RoomTopology::new_with_bounded_spillover(RouterId(9), 2);
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &first_user_id,
                0,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &second_user_id,
                1,
                TopologyPressureSnapshot::default(),
            )
            .is_ok()
    );
    assert_eq!(topology.router_count(), 2);

    assert!(topology.remove_session(&second_user_id).is_ok());

    assert_eq!(topology.router_count(), 1);
    assert_eq!(topology.user_count(), 1);
}

#[test]
fn load_triggered_topology_keeps_small_rooms_on_primary_router() {
    let policy = LocalSpilloverPolicy::conservative()
        .with_min_receiver_count(3)
        .with_activation_window(1);
    let mut topology = RoomTopology::new_with_load_spillover(RouterId(9), 2, policy);
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &first_user_id,
                0,
                TopologyPressureSnapshot {
                    receiver_count: 1,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &second_user_id,
                1,
                TopologyPressureSnapshot {
                    receiver_count: 2,
                    ..Default::default()
                },
            )
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_user(&first_user_id),
        Some(RouterId(9))
    );
    assert_eq!(
        topology.home_router_id_for_user(&second_user_id),
        Some(RouterId(9))
    );
    assert_eq!(topology.router_count(), 1);
}

#[test]
fn load_triggered_topology_attaches_router_after_pressure_window() {
    let policy = LocalSpilloverPolicy::conservative()
        .with_min_receiver_count(2)
        .with_activation_window(1);
    let mut topology = RoomTopology::new_with_load_spillover(RouterId(9), 2, policy);
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &first_user_id,
                0,
                TopologyPressureSnapshot {
                    receiver_count: 1,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &second_user_id,
                1,
                TopologyPressureSnapshot {
                    receiver_count: 2,
                    ..Default::default()
                },
            )
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_user(&first_user_id),
        Some(RouterId(9))
    );
    assert_eq!(
        topology.home_router_id_for_user(&second_user_id),
        Some(RouterId(10))
    );
    assert_eq!(topology.router_count(), 2);
    assert_eq!(topology.active_load_router_count_for_test(), 2);
    assert_eq!(
        topology.last_load_pressure_reason_for_test(),
        Some(LoadPressureReason::ReceiverCount)
    );
}

#[test]
fn load_triggered_topology_honors_activation_window() {
    let policy = LocalSpilloverPolicy::conservative()
        .with_min_receiver_count(2)
        .with_activation_window(2);
    let mut topology = RoomTopology::new_with_load_spillover(RouterId(9), 2, policy);
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);
    let third_user_id = UserId::Integer(30);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &first_user_id,
                0,
                TopologyPressureSnapshot {
                    receiver_count: 1,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &second_user_id,
                1,
                TopologyPressureSnapshot {
                    receiver_count: 2,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &third_user_id,
                3,
                TopologyPressureSnapshot {
                    receiver_count: 3,
                    ..Default::default()
                },
            )
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_user(&first_user_id),
        Some(RouterId(9))
    );
    assert_eq!(
        topology.home_router_id_for_user(&second_user_id),
        Some(RouterId(9))
    );
    assert_eq!(
        topology.home_router_id_for_user(&third_user_id),
        Some(RouterId(10))
    );
}

#[test]
fn load_triggered_topology_waits_for_cooldown_before_idle_detach() {
    let policy = LocalSpilloverPolicy::conservative()
        .with_min_receiver_count(2)
        .with_activation_window(1)
        .with_cooldown_window(2);
    let mut topology = RoomTopology::new_with_load_spillover(RouterId(9), 2, policy);
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);
    let third_user_id = UserId::Integer(30);

    assert!(
        topology
            .apply_client_join_with_pressure(
                &first_user_id,
                0,
                TopologyPressureSnapshot {
                    receiver_count: 1,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &second_user_id,
                1,
                TopologyPressureSnapshot {
                    receiver_count: 2,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join_with_pressure(
                &third_user_id,
                2,
                TopologyPressureSnapshot {
                    receiver_count: 3,
                    ..Default::default()
                },
            )
            .is_ok()
    );
    assert_eq!(topology.router_count(), 2);

    assert!(topology.remove_session(&second_user_id).is_ok());
    assert_eq!(topology.router_count(), 2);

    assert!(topology.remove_session(&third_user_id).is_ok());
    assert_eq!(topology.router_count(), 1);
}

#[test]
fn topology_reports_missing_router_for_user_lookup() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);
    assert!(
        topology
            .apply_client_join_with_pressure(&user_id, 42, TopologyPressureSnapshot::default())
            .is_ok()
    );
    topology.remove_router_for_test(RouterId(7));

    assert_eq!(
        topology.remove_session(&user_id),
        Err(RoomTopologyError::MissingRouterForSession {
            user_id,
            router_id: RouterId(7),
        })
    );
}

#[test]
fn topology_placement_bundle_rejects_empty_router_sets() {
    assert_eq!(
        LocalRoomRouterPlacements::try_from_vec(Vec::new()),
        Err(LocalRoomRouterPlacementsError::Empty)
    );
}

#[test]
fn topology_reports_missing_user_mapping_from_router_state() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);
    assert!(
        topology
            .apply_client_join_with_pressure(&user_id, 42, TopologyPressureSnapshot::default())
            .is_ok()
    );
    topology.remove_session_mapping_for_test(&user_id);
    topology.remove_transport_mapping_for_test(&user_id);

    assert_eq!(
        topology.ensure_session_transports(&user_id),
        Err(RoomTopologyError::RouterState(
            RoomRouterStateError::MissingSessionMapping { user_id }
        ))
    );
}

#[test]
fn topology_preserves_pure_router_errors_without_synthetic_user_ids() {
    let mut topology = RoomTopology::new(RouterId(9));

    assert_eq!(
        topology.remove_producer(RoutedProducerId::new(RouterId(9), RouterProducerId(99),)),
        Err(RoomTopologyError::RouterState(
            RoomRouterStateError::Router(RouterError::MissingProducer(RouterProducerId(99)))
        ))
    );
}
