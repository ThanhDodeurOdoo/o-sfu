use o_sfu_router::{ConsumerId, MediaKind, MediaStream, ProducerId, RouterId};

use super::{
    super::routing::RoutedProducerId,
    ConsumerKey, ConsumerSourceSelection, ConsumerState, ProducerRuntimeId, PublishedProducer,
    route_graph::{RelayRouteEffect, RouteGraph},
    subscription::{ConsumerSetupProducerSnapshot, ConsumerSetupTarget},
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{RelayRouteActivity, TransportMediaId, TransportRelayRouteAction},
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
        consumer,
        connection,
        ConsumerSetupProducerSnapshot::from_producer(producer_id, &producer, transport_media_id),
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

#[test]
fn subscription_count_tracks_route_state_transitions() {
    let mut graph = RouteGraph::default();
    let source_id = PublishedSourceId::from_raw(1);
    let stored = ConsumerKey::new(&UserId::Integer(1), source_id);
    let pending = ConsumerKey::new(&UserId::Integer(2), source_id);
    let committed = ConsumerKey::new(&UserId::Integer(3), source_id);

    graph.set_selection(&stored, false);
    assert_eq!(graph.subscription_count(), 0);

    graph.reserve_consumer_setup(pending.clone());
    assert_eq!(graph.subscription_count(), 1);

    assert!(graph.commit(
        committed.clone(),
        consumer_state(1),
        ConsumerSourceSelection::open(true)
    ));
    assert_eq!(graph.subscription_count(), 2);

    assert!(graph.commit(
        pending.clone(),
        consumer_state(2),
        ConsumerSourceSelection::open(true)
    ));
    assert_eq!(graph.subscription_count(), 2);

    graph.release_consumer_setup(&pending);
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
    let key = ConsumerKey::new(owner.consumer_user_id(), source_id);

    assert_eq!(
        actions(graph.reserve_relay(
            &owner,
            ConnectionId::from_raw(10),
            TransportMediaId::new(50),
            MediaWorkerId::from_raw(1),
            false,
        )),
        vec![TransportRelayRouteAction::Install]
    );
    assert_eq!(
        actions(graph.remove_key_state(&key)),
        vec![TransportRelayRouteAction::Release]
    );
}

#[test]
fn relay_route_stays_installed_until_last_owner_is_released() {
    let mut graph = RouteGraph::default();
    let source_id = PublishedSourceId::from_raw(1);
    let active_owner = target(UserId::Integer(2), ConnectionId::from_raw(20), source_id);
    let inactive_owner = target(UserId::Integer(3), ConnectionId::from_raw(30), source_id);
    let source_connection = ConnectionId::from_raw(10);
    let source_media = TransportMediaId::new(50);
    let target_worker = MediaWorkerId::from_raw(1);

    assert_eq!(
        actions(graph.reserve_relay(
            &inactive_owner,
            source_connection,
            source_media,
            target_worker,
            false,
        )),
        vec![TransportRelayRouteAction::Install]
    );
    assert!(
        graph
            .reserve_relay(
                &inactive_owner,
                source_connection,
                source_media,
                target_worker,
                false,
            )
            .is_empty()
    );
    assert_eq!(
        actions(graph.reserve_relay(
            &active_owner,
            source_connection,
            source_media,
            target_worker,
            true,
        )),
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
        actions(graph.release_target(&active_owner)),
        vec![TransportRelayRouteAction::SetActivity(
            RelayRouteActivity::Inactive
        )]
    );
    assert_eq!(
        actions(graph.release_target(&inactive_owner)),
        vec![TransportRelayRouteAction::Release]
    );
}
