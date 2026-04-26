use o_sfu_router::{ProducerId as RouterProducerId, RouterError};

use super::fixtures::*;
use crate::runtime::room::{router_state::RoomRouterStateError, topology::RoomTopologyError};

#[test]
fn topology_assigns_the_primary_router_to_joined_users() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);

    assert!(topology.apply_client_join(&user_id, 42).is_ok());

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

    assert!(topology.apply_client_join(&user_id, 42).is_ok());
    assert!(topology.apply_client_join(&user_id, 43).is_ok());

    assert_eq!(topology.user_count(), 1);
}

#[test]
fn topology_returns_router_scoped_entity_handles() {
    let mut topology = RoomTopology::new(RouterId(9));
    let producer_user_id = UserId::Integer(10);
    let consumer_user_id = UserId::Integer(20);

    for (seed, user_id) in [(10, &producer_user_id), (20, &consumer_user_id)] {
        assert!(topology.apply_client_join(user_id, seed).is_ok());
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
fn topology_reports_missing_router_for_user_lookup() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);
    assert!(topology.apply_client_join(&user_id, 42).is_ok());
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
fn topology_reports_missing_user_mapping_from_router_state() {
    let mut topology = RoomTopology::new(RouterId(7));
    let user_id = UserId::Integer(10);
    assert!(topology.apply_client_join(&user_id, 42).is_ok());
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
        topology.remove_producer(super::super::topology::RoutedProducerId::new(
            RouterId(9),
            RouterProducerId(99),
        )),
        Err(RoomTopologyError::RouterState(
            RoomRouterStateError::Router(RouterError::MissingProducer(RouterProducerId(99)))
        ))
    );
}
