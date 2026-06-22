#![allow(
    clippy::expect_used,
    reason = "topology tests use direct assertions for clear failure messages"
)]

use o_sfu_model::UserId;

use crate::{
    ids::{ConnectionId, MediaWorkerId, ProducerId as RouterProducerId, RouterId, SessionId},
    rtp::MediaKind as RouterMediaKind,
    state::{ConsumerCapability, RouterError},
    topology::{
        RoutedProducerId, RouterPlacement, RouterPlacements, RouterPlacementsError, RoutingError,
        RoutingTopology,
    },
};

fn placement(router: u64, media_worker: usize) -> RouterPlacement {
    RouterPlacement {
        router: RouterId(router),
        media_worker: MediaWorkerId::from_raw(media_worker),
    }
}

fn join_on_router(
    topology: &mut RoutingTopology,
    user_id: &UserId,
    connection_id_raw: u64,
    router: u64,
    media_worker: usize,
) -> Result<(), RoutingError> {
    topology
        .commit_session_placement(
            user_id,
            ConnectionId::from_raw(connection_id_raw),
            placement(router, media_worker),
            [],
        )
        .map(|_| ())
}

#[test]
fn topology_assigns_the_primary_router_to_joined_users() {
    let mut topology = RoutingTopology::new_for_test(RouterId(7));
    let user_id = UserId::Integer(10);

    assert!(join_on_router(&mut topology, &user_id, 42, 7, 0).is_ok());

    assert_eq!(
        topology.home_router_id_for_user(&user_id),
        Some(RouterId(7))
    );
    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_rejoin_does_not_duplicate_router_users() {
    let mut topology = RoutingTopology::new_for_test(RouterId(7));
    let user_id = UserId::Integer(10);

    assert!(join_on_router(&mut topology, &user_id, 42, 7, 0).is_ok());
    assert!(join_on_router(&mut topology, &user_id, 43, 7, 0).is_ok());

    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_returns_router_scoped_entity_handles() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    for (seed, user_id) in [(10, &producer_user_id), (20, &consumer_user_id)] {
        assert!(join_on_router(&mut topology, user_id, seed, 9, 0).is_ok());
    }

    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .expect("producer should be routed");

    let consumer = topology
        .add_consumer(&consumer_user_id, producer, ConsumerCapability::Compatible)
        .expect("consumer should be routed");

    assert_eq!(producer.router_id(), RouterId(9));
    assert_eq!(consumer.router_id(), RouterId(9));
}

#[test]
fn topology_attaches_spillover_router_for_bounded_policy() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);

    assert!(join_on_router(&mut topology, &first_user_id, 0, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &second_user_id, 1, 10, 1).is_ok());

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
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let user_id = UserId::Integer(10);

    assert!(join_on_router(&mut topology, &user_id, 0, 9, 0).is_ok());
    assert_eq!(
        topology.home_router_id_for_user(&user_id),
        Some(RouterId(9))
    );

    assert!(join_on_router(&mut topology, &user_id, 1, 10, 1).is_ok());

    assert_eq!(
        topology.home_router_id_for_user(&user_id),
        Some(RouterId(10))
    );
    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_rolls_back_replacement_after_duplicate_session_failure() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);
    let colliding_connection = 1;

    assert!(join_on_router(&mut topology, &first_user_id, colliding_connection, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &second_user_id, 2, 10, 1).is_ok());

    assert_eq!(
        topology.commit_session_placement(
            &second_user_id,
            ConnectionId::from_raw(colliding_connection),
            placement(9, 0),
            [],
        ),
        Err(RoutingError::Router(RouterError::DuplicateSession(
            SessionId(colliding_connection),
        )))
    );
    assert_eq!(
        topology.home_router_id_for_user(&first_user_id),
        Some(RouterId(9))
    );
    assert_eq!(
        topology.home_router_id_for_user(&second_user_id),
        Some(RouterId(10))
    );
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(1)
    );
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(10)),
        Some(1)
    );
    assert_eq!(topology.user_count(), 2);
}

#[test]
fn topology_routes_cross_router_consumers_through_source_router() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    assert!(join_on_router(&mut topology, &producer_user_id, 0, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &consumer_user_id, 1, 10, 1).is_ok());

    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .expect("producer should be routed");
    let consumer = topology
        .add_consumer(&consumer_user_id, producer, ConsumerCapability::Compatible)
        .expect("consumer should be routed");

    assert_eq!(producer.router_id(), RouterId(9));
    assert_eq!(consumer.router_id(), RouterId(9));
    assert_eq!(
        topology.home_router_id_for_user(&consumer_user_id),
        Some(RouterId(10))
    );
}

#[test]
fn topology_prunes_receiver_shadow_when_cross_router_source_leaves_first() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    assert!(join_on_router(&mut topology, &producer_user_id, 0, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &consumer_user_id, 1, 10, 1).is_ok());

    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .expect("producer should be routed");
    let consumer = topology
        .add_consumer(&consumer_user_id, producer, ConsumerCapability::Compatible)
        .expect("consumer should be routed");

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(
        topology
            .remove_session(&producer_user_id, [consumer])
            .is_ok()
    );

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
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    assert!(join_on_router(&mut topology, &producer_user_id, 0, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &consumer_user_id, 1, 10, 1).is_ok());
    let first_producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .expect("first producer should be routed");
    let second_producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .expect("second producer should be routed");

    let first_consumer = topology
        .add_consumer(
            &consumer_user_id,
            first_producer,
            ConsumerCapability::Compatible,
        )
        .expect("first consumer should be routed");
    let second_consumer = topology
        .add_consumer(
            &consumer_user_id,
            second_producer,
            ConsumerCapability::Compatible,
        )
        .expect("second consumer should be routed");

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(
        topology
            .remove_producer(first_producer, [first_consumer])
            .is_ok()
    );
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(
        topology
            .remove_producer(second_producer, [second_consumer])
            .is_ok()
    );
    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(1)
    );
}

#[test]
fn topology_remove_consumer_prunes_cross_router_shadow() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    assert!(join_on_router(&mut topology, &producer_user_id, 0, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &consumer_user_id, 1, 10, 1).is_ok());
    let producer = topology
        .add_producer(&producer_user_id, RouterMediaKind::Audio)
        .expect("producer should be accepted");
    let consumer = topology
        .add_consumer(&consumer_user_id, producer, ConsumerCapability::Compatible)
        .expect("consumer should be accepted");

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(2)
    );
    assert!(topology.remove_consumer(consumer).is_ok());

    assert_eq!(
        topology.mapped_session_count_for_router(RouterId(9)),
        Some(1)
    );
    assert_eq!(
        topology.home_router_id_for_user(&consumer_user_id),
        Some(RouterId(10))
    );
}

#[test]
fn topology_rejects_shadow_consumer_without_receiver_home_placement() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let missing_consumer_user_id = UserId::Integer(20);

    assert_eq!(
        topology.add_consumer(
            &missing_consumer_user_id,
            RoutedProducerId::new(RouterId(9), RouterProducerId(1)),
            ConsumerCapability::Compatible,
        ),
        Err(RoutingError::MissingSessionPlacement {
            user_id: missing_consumer_user_id,
        })
    );
}

#[test]
fn topology_rejects_consumer_on_missing_attached_router() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let consumer_user_id = UserId::Integer(20);
    assert!(join_on_router(&mut topology, &consumer_user_id, 0, 9, 0).is_ok());

    assert_eq!(
        topology.add_consumer(
            &consumer_user_id,
            RoutedProducerId::new(RouterId(99), RouterProducerId(1)),
            ConsumerCapability::Compatible,
        ),
        Err(RoutingError::MissingRouter {
            router_id: RouterId(99),
        })
    );
}

#[test]
fn topology_reports_idle_spillover_router_after_last_home_session_leaves() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let first_user_id = UserId::Integer(10);
    let second_user_id = UserId::Integer(20);

    assert!(join_on_router(&mut topology, &first_user_id, 0, 9, 0).is_ok());
    assert!(join_on_router(&mut topology, &second_user_id, 1, 10, 1).is_ok());
    assert_eq!(topology.router_count(), 2);

    assert!(topology.remove_session(&second_user_id, []).is_ok());

    assert_eq!(topology.router_count(), 2);
    assert_eq!(topology.idle_spillover_routers(), vec![RouterId(10)]);
    topology.detach_spillover_routers(&[RouterId(10)]);
    assert_eq!(topology.router_count(), 1);
    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_never_reports_primary_router_as_idle_spillover() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));
    let user_id = UserId::Integer(10);

    assert!(join_on_router(&mut topology, &user_id, 0, 9, 0).is_ok());
    assert!(topology.remove_session(&user_id, []).is_ok());

    assert!(topology.idle_spillover_routers().is_empty());
    topology.detach_spillover_routers(&[RouterId(9)]);
    assert_eq!(topology.router_count(), 1);
}

#[test]
fn topology_reports_missing_router_for_user_lookup() {
    let mut topology = RoutingTopology::new_for_test(RouterId(7));
    let user_id = UserId::Integer(10);
    assert!(join_on_router(&mut topology, &user_id, 42, 7, 0).is_ok());
    topology.remove_router_for_test(RouterId(7));

    assert_eq!(
        topology.remove_session(&user_id, []),
        Err(RoutingError::MissingRouterForSession {
            user_id,
            router_id: RouterId(7),
        })
    );
}

#[test]
fn topology_placement_bundle_rejects_empty_router_sets() {
    assert_eq!(
        RouterPlacements::try_from_vec(Vec::new()),
        Err(RouterPlacementsError::Empty)
    );
}

#[test]
fn topology_reports_missing_user_mapping_from_router_state() {
    let mut topology = RoutingTopology::new_for_test(RouterId(7));
    let user_id = UserId::Integer(10);
    assert!(join_on_router(&mut topology, &user_id, 42, 7, 0).is_ok());
    topology.remove_user_mappings_for_test(&user_id);

    assert_eq!(
        topology.add_producer(&user_id, RouterMediaKind::Audio),
        Err(RoutingError::MissingSessionMapping { user_id })
    );
}

#[test]
fn topology_preserves_pure_router_errors_without_synthetic_user_ids() {
    let mut topology = RoutingTopology::new_for_test(RouterId(9));

    assert_eq!(
        topology.remove_producer(
            RoutedProducerId::new(RouterId(9), RouterProducerId(99),),
            []
        ),
        Err(RoutingError::Router(RouterError::MissingProducer(
            RouterProducerId(99)
        )))
    );
}
