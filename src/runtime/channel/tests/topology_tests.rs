use o_sfu_router::{ProducerId as RouterProducerId, RouterError};

use super::fixtures::*;
use crate::runtime::channel::{
    router_state::ChannelRouterStateError, topology::ChannelTopologyError,
};

#[test]
fn topology_assigns_the_primary_router_to_joined_sessions() {
    let mut topology = ChannelTopology::new(RouterId(7));
    let session_id = SessionId::Integer(10);

    assert!(
        topology
            .apply_client_join(
                &session_id,
                42,
                super::super::ChannelSessionPermissions::from(SessionPermissions::default())
                    .router_permissions(),
            )
            .is_ok()
    );

    assert_eq!(
        topology.home_router_id_for_session(&session_id),
        Some(RouterId(7))
    );
    assert_eq!(topology.session_count(), 1);
}

#[test]
fn topology_rejoin_updates_permissions_without_duplicating_router_sessions() {
    let mut topology = ChannelTopology::new(RouterId(7));
    let session_id = SessionId::Integer(10);
    let initial_permissions = SessionPermissions::default();
    let replacement_permissions = SessionPermissions {
        video_recording: Some(true),
        ..SessionPermissions::default()
    };

    assert!(
        topology
            .apply_client_join(
                &session_id,
                42,
                super::super::ChannelSessionPermissions::from(initial_permissions)
                    .router_permissions(),
            )
            .is_ok()
    );
    assert!(
        topology
            .apply_client_join(
                &session_id,
                43,
                super::super::ChannelSessionPermissions::from(replacement_permissions)
                    .router_permissions(),
            )
            .is_ok()
    );

    assert_eq!(topology.session_count(), 1);
    assert_eq!(
        topology.session_permissions(&session_id),
        Some(o_sfu_router::SessionPermissions::from_flags(
            o_sfu_router::SessionPermissionFlags {
                transcription: false,
                audio_recording: false,
                video_recording: true,
            },
        ))
    );
}

#[test]
fn topology_returns_router_scoped_entity_handles() {
    let mut topology = ChannelTopology::new(RouterId(9));
    let producer_session_id = SessionId::Integer(10);
    let consumer_session_id = SessionId::Integer(20);

    for (seed, session_id) in [(10, &producer_session_id), (20, &consumer_session_id)] {
        assert!(
            topology
                .apply_client_join(
                    session_id,
                    seed,
                    super::super::ChannelSessionPermissions::from(SessionPermissions::default())
                        .router_permissions(),
                )
                .is_ok()
        );
    }

    let producer = topology
        .add_producer(
            &producer_session_id,
            RouterMediaKind::Audio,
            RouterStreamType::Audio,
        )
        .ok();
    assert!(producer.is_some());
    let Some(producer) = producer else {
        return;
    };

    let consumer = topology
        .add_consumer(
            &consumer_session_id,
            producer,
            RouterMediaKind::Audio,
            RouterStreamType::Audio,
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
fn topology_reports_missing_router_for_session_lookup() {
    let mut topology = ChannelTopology::new(RouterId(7));
    let session_id = SessionId::Integer(10);
    assert!(
        topology
            .apply_client_join(
                &session_id,
                42,
                super::super::ChannelSessionPermissions::from(SessionPermissions::default())
                    .router_permissions(),
            )
            .is_ok()
    );
    topology.remove_router_for_test(RouterId(7));

    assert_eq!(
        topology.remove_session(&session_id),
        Err(ChannelTopologyError::MissingRouterForSession {
            session_id,
            router_id: RouterId(7),
        })
    );
}

#[test]
fn topology_reports_missing_session_mapping_from_router_state() {
    let mut topology = ChannelTopology::new(RouterId(7));
    let session_id = SessionId::Integer(10);
    assert!(
        topology
            .apply_client_join(
                &session_id,
                42,
                super::super::ChannelSessionPermissions::from(SessionPermissions::default())
                    .router_permissions(),
            )
            .is_ok()
    );
    topology.remove_session_mapping_for_test(&session_id);
    topology.remove_transport_mapping_for_test(&session_id);

    assert_eq!(
        topology.ensure_session_transports(&session_id),
        Err(ChannelTopologyError::RouterState(
            ChannelRouterStateError::MissingSessionMapping { session_id }
        ))
    );
}

#[test]
fn topology_preserves_pure_router_errors_without_synthetic_session_ids() {
    let mut topology = ChannelTopology::new(RouterId(9));

    assert_eq!(
        topology.remove_producer(super::super::topology::RoutedProducerId::new(
            RouterId(9),
            RouterProducerId(99),
        )),
        Err(ChannelTopologyError::RouterState(
            ChannelRouterStateError::Router(RouterError::MissingProducer(RouterProducerId(99)))
        ))
    );
}
