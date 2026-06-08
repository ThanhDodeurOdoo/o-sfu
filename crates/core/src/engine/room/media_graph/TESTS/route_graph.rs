#![allow(
    clippy::expect_used,
    reason = "route graph tests fail loudly when fixed route reservations are invalid"
)]

use o_sfu_router::{ConsumerId, MediaKind, MediaStream, ProducerId, RouterId};

use super::{
    super::routing::RoutedProducerId,
    ConsumerKey, ConsumerSourceSelection, ConsumerState, ProducerRuntimeId, PublishedProducer,
    consumer_setup::ConsumerSetupTarget,
    route_graph::{ConsumerRouteReservation, RelayRouteEffect, RouteGraph},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
    media_transport::{
        RelayRouteActivity, TransportMediaId, TransportRelayRouteAction, TransportSessionKey,
    },
    room::routing::RoutedConsumerId,
    source_model::{PublishedSourceId, UserStreamId},
};

fn target(
    consumer: UserId,
    connection: ConnectionId,
    source_id: PublishedSourceId,
) -> ConsumerSetupTarget {
    let mut next_producer_id = 1;
    let producer_id = ProducerRuntimeId::allocate(&mut next_producer_id);
    let transport_media_id = TransportMediaId::new(50);
    let producer = PublishedProducer {
        source_id,
        owner_user_id: UserId::Integer(1),
        owner_connection_id: ConnectionId::from_raw(10),
        stream_id: UserStreamId::from("camera"),
        media_kind: MediaKind::Video,
        consumable_rtp_parameters: MediaStream::new(vec![], vec![], vec![]),
        routed_producer_id: RoutedProducerId::new(RouterId(1), ProducerId(10)),
        transport_media_id: Some(transport_media_id),
        active: true,
    };
    ConsumerSetupTarget::new(
        consumer.clone(),
        connection,
        session_key(consumer, connection),
        session_key(producer.owner_user_id.clone(), producer.owner_connection_id),
        producer_id,
        &producer,
        transport_media_id,
    )
}

fn session_key(user: UserId, connection: ConnectionId) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(0),
        MediaWorkerId::from_raw(0),
        connection,
        user,
    )
}

fn consumer_state(id: u64) -> ConsumerState {
    ConsumerState {
        routed_consumer_id: RoutedConsumerId::new(RouterId(1), ConsumerId(id)),
        consumer_connection_id: ConnectionId::from_raw(20 + id),
        source_connection_id: ConnectionId::from_raw(10),
        source_media: TransportMediaId::new(50),
        consumer_media: TransportMediaId::new(100 + id),
    }
}

fn actions(effects: Vec<RelayRouteEffect>) -> Vec<TransportRelayRouteAction> {
    effects.into_iter().map(|effect| effect.action).collect()
}

fn replacement_routes(
    graph: &mut RouteGraph,
    key: &ConsumerKey,
) -> (ConsumerRouteReservation, ConsumerRouteReservation) {
    let stale = graph
        .reserve_consumer_setup(key.clone(), ConsumerSourceSelection::open(false))
        .expect("first pending route should be reserved");
    assert!(graph.remove_key_state(key).is_empty());
    let fresh = graph
        .reserve_consumer_setup(key.clone(), ConsumerSourceSelection::open(true))
        .expect("replacement pending route should be reserved");
    (stale, fresh)
}

#[test]
fn subscription_count_tracks_route_state_transitions() {
    let mut graph = RouteGraph::default();
    let source_id = PublishedSourceId::from_raw(1);
    let stored = ConsumerKey::new(&UserId::Integer(1), source_id);
    let pending = ConsumerKey::new(&UserId::Integer(2), source_id);
    let committed = ConsumerKey::new(&UserId::Integer(3), source_id);
    let released = ConsumerKey::new(&UserId::Integer(4), source_id);

    graph.set_selection(&stored, false);
    assert_eq!(graph.subscription_count(), 0);

    let pending_route = graph
        .reserve_consumer_setup(pending.clone(), ConsumerSourceSelection::open(true))
        .expect("pending route should be reserved");
    assert_eq!(graph.subscription_count(), 1);

    let committed_route = graph
        .reserve_consumer_setup(committed.clone(), ConsumerSourceSelection::open(true))
        .expect("committed route should start as pending");
    assert!(graph.commit(
        &committed_route,
        consumer_state(1),
        ConsumerSourceSelection::open(true),
    ));
    assert_eq!(graph.subscription_count(), 2);

    let released_route = graph
        .reserve_consumer_setup(released, ConsumerSourceSelection::open(true))
        .expect("released route should be reserved");
    assert_eq!(graph.subscription_count(), 3);
    assert!(graph.release_consumer_setup(released_route).is_empty());
    assert_eq!(graph.subscription_count(), 2);

    assert!(graph.commit(
        &pending_route,
        consumer_state(2),
        ConsumerSourceSelection::open(true),
    ));
    assert_eq!(graph.subscription_count(), 2);

    graph.remove_key_state(&pending);
    assert_eq!(graph.subscription_count(), 1);
    graph.remove_key_state(&committed);
    assert_eq!(graph.subscription_count(), 0);
    graph.remove_key_state(&stored);
    assert_eq!(graph.subscription_count(), 0);
}

#[test]
fn remove_key_state_releases_relay_owner() {
    let mut graph = RouteGraph::default();
    let source_id = PublishedSourceId::from_raw(1);
    let owner = target(UserId::Integer(2), ConnectionId::from_raw(20), source_id);
    let key = owner.consumer_key();
    let route = graph
        .reserve_consumer_setup(key.clone(), ConsumerSourceSelection::open(false))
        .expect("relay owner should have a pending route");

    assert_eq!(
        actions(graph.reserve_relay(&route, &owner, MediaWorkerId::from_raw(1), false,)),
        vec![TransportRelayRouteAction::Install]
    );
    assert_eq!(
        actions(graph.remove_key_state(&key)),
        vec![TransportRelayRouteAction::Release]
    );
}

#[test]
fn stale_reservation_cannot_mutate_new_pending_route() {
    let source_id = PublishedSourceId::from_raw(1);
    let key = ConsumerKey::new(&UserId::Integer(2), source_id);

    let mut graph = RouteGraph::default();
    let (stale, fresh) = replacement_routes(&mut graph, &key);
    assert!(graph.release_consumer_setup(stale).is_empty());
    assert_eq!(graph.subscription_count(), 1);
    assert!(graph.release_consumer_setup(fresh).is_empty());
    assert_eq!(graph.subscription_count(), 0);

    let mut graph = RouteGraph::default();
    let (stale, fresh) = replacement_routes(&mut graph, &key);
    assert!(!graph.commit(
        &stale,
        consumer_state(1),
        ConsumerSourceSelection::open(true)
    ));
    assert_eq!(graph.subscription_count(), 1);
    assert!(graph.commit(
        &fresh,
        consumer_state(2),
        ConsumerSourceSelection::open(true)
    ));
    assert_eq!(graph.count(), 1);

    let mut graph = RouteGraph::default();
    let owner = target(UserId::Integer(2), ConnectionId::from_raw(20), source_id);
    let (stale, fresh) = replacement_routes(&mut graph, &key);
    assert!(
        graph
            .reserve_relay(&stale, &owner, MediaWorkerId::from_raw(1), false,)
            .is_empty()
    );
    assert!(graph.release_consumer_setup(fresh).is_empty());
}

#[test]
fn relay_route_stays_installed_until_last_owner_is_released() {
    let mut graph = RouteGraph::default();
    let source_id = PublishedSourceId::from_raw(1);
    let active_owner = target(UserId::Integer(2), ConnectionId::from_raw(20), source_id);
    let inactive_owner = target(UserId::Integer(3), ConnectionId::from_raw(30), source_id);
    let target_worker = MediaWorkerId::from_raw(1);
    let active_key = active_owner.consumer_key();
    let inactive_key = inactive_owner.consumer_key();
    let active_route = graph
        .reserve_consumer_setup(active_key, ConsumerSourceSelection::open(true))
        .expect("active owner should have a pending route");
    let inactive_route = graph
        .reserve_consumer_setup(inactive_key, ConsumerSourceSelection::open(false))
        .expect("inactive owner should have a pending route");

    assert_eq!(
        actions(graph.reserve_relay(&inactive_route, &inactive_owner, target_worker, false,)),
        vec![TransportRelayRouteAction::Install]
    );
    assert!(
        graph
            .reserve_relay(&inactive_route, &inactive_owner, target_worker, false,)
            .is_empty()
    );
    assert_eq!(
        actions(graph.reserve_relay(&active_route, &active_owner, target_worker, true,)),
        vec![TransportRelayRouteAction::SetActivity(
            RelayRouteActivity::Active
        )]
    );
    assert!(
        graph
            .set_relay_active(
                &UserId::Integer(3),
                ConnectionId::from_raw(30),
                source_id,
                RelayRouteActivity::Active,
            )
            .is_empty()
    );
    assert!(
        graph
            .set_relay_active(
                &UserId::Integer(3),
                ConnectionId::from_raw(30),
                source_id,
                RelayRouteActivity::Inactive,
            )
            .is_empty()
    );

    assert_eq!(
        actions(graph.release_consumer_setup(active_route)),
        vec![TransportRelayRouteAction::SetActivity(
            RelayRouteActivity::Inactive
        )]
    );
    assert_eq!(
        actions(graph.release_consumer_setup(inactive_route)),
        vec![TransportRelayRouteAction::Release]
    );
}
