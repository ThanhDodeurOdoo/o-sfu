#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "state-level test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{slice::from_ref, sync::Arc};

use o_sfu_router::{
    MediaKind, RouterId,
    ids::{ConsumerId, ProducerId},
    rtp::MediaStream,
    test_support::rtp_samples::{sample_client_rtp_capabilities, sample_video_rtp_parameters},
    topology::{RoutedConsumerId, RoutedProducerId},
};

use super::*;
use crate::{
    MediaCodecFlags, RoomMediaLimits,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, TestSourceKind, UserPermissions,
        media_transport::{TransportMediaId, TransportRelayRouteAction, TransportSessionKey},
        metrics::RuntimeMetrics,
        room::{
            RoomAdmissionPolicy, RoomRuntimeContext, RouterPlacement, UserOutboundSender,
            media_graph::{
                ConsumerKey, ConsumerSetupOutcome, ConsumerState, ProducerRuntimeId,
                PublishedProducer, PublishedSourceInstall, ResolvedRelayRouteEffect,
            },
            rtp_capabilities::router_rtp_capabilities,
        },
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceDescriptorParts,
            PublishedSourceId, PublishedSourceOwner, SourceEncodingDescriptor,
            SourceEncodingDescriptorParts, SourceEncodingId,
            test_support::{source_publish_intent_for_source, stream_id_for_source},
        },
    },
};

fn test_state() -> RoomState {
    let runtime_context = RoomRuntimeContext::new(
        RoomInstanceId::from_raw(0),
        RouterPlacement {
            router: RouterId(1),
            media_worker: MediaWorkerId::from_raw(0),
        },
        Vec::new(),
    );
    RoomState::new(
        &runtime_context,
        RoomAdmissionPolicy::new(4),
        RoomMediaLimits::default(),
        router_rtp_capabilities(MediaCodecFlags::default()),
    )
}

fn test_sender() -> UserOutboundSender {
    UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default())).0
}

fn join_test_user(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    state
        .apply_join(user_id, UserPermissions::default(), test_sender())
        .expect("test user should join")
        .receipt
        .connection_id
}

fn join_test_user_on_placement(
    state: &mut RoomState,
    user_id: &UserId,
    placement: RouterPlacement,
) -> ConnectionId {
    state
        .apply_join_on_placement(
            user_id,
            UserPermissions::default(),
            test_sender(),
            UserJoinedFanout::Suppress,
            placement,
        )
        .expect("test user should join on placement")
        .receipt
        .connection_id
}

fn install_test_published_producer(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    routed_producer_id: RoutedProducerId,
    transport_media_id: TransportMediaId,
    consumable_rtp_parameters: MediaStream,
) -> (ProducerRuntimeId, PublishedSourceId) {
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let source_id = PublishedSourceId::allocate(&mut state.next_source_id);
    let encoding_id = SourceEncodingId::allocate(&mut state.next_source_encoding_id);
    let intent = source_publish_intent_for_source(stream_type);
    let source = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(user_id.clone()),
        stream_id: intent.stream_id().clone(),
        media_kind: intent.media_kind(),
        policy: intent.policy(),
        mid: None,
        encodings: vec![SourceEncodingDescriptor::new(
            SourceEncodingDescriptorParts {
                encoding_id,
                source_id,
                rid: None,
                primary_ssrc: None,
                repair_ssrc: None,
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
                policy_role: None,
                negotiated_format: None,
            },
        )],
    })
    .expect("test source graph should be valid");
    state
        .topology
        .install_source_for_test(PublishedSourceInstall {
            source_descriptor: source,
            producer_id,
            producer: PublishedProducer {
                source_id,
                owner_user_id: user_id.clone(),
                owner_connection_id: connection_id,
                stream_id: stream_id_for_source(stream_type),
                media_kind: MediaKind::Video,
                consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                active: true,
            },
            transport_media_id,
        });
    (producer_id, source_id)
}

#[derive(Debug)]
struct RelayedSource {
    publisher: UserId,
    publisher_connection: ConnectionId,
    publisher_session: TransportSessionKey,
    source_media: TransportMediaId,
}

fn install_relayed_source(state: &mut RoomState) -> RelayedSource {
    let publisher = UserId::Integer(1);
    let subscriber = UserId::Integer(2);
    let publisher_connection = join_test_user(state, &publisher);
    let subscriber_connection = join_test_user_on_placement(
        state,
        &subscriber,
        RouterPlacement {
            router: RouterId(2),
            media_worker: MediaWorkerId::from_raw(1),
        },
    );
    assert!(
        state
            .set_user_negotiated(
                &subscriber,
                subscriber_connection,
                sample_client_rtp_capabilities()
            )
            .is_some()
    );
    let publisher_session = state.transport_user_key(&publisher, publisher_connection);
    let source_media = TransportMediaId::new(11);
    let consumer_media = TransportMediaId::new(21);
    let routed_producer_id = state
        .topology
        .routing_mut_for_test()
        .add_producer(&publisher, MediaKind::Video)
        .expect("relayed source producer route should be added");
    install_test_published_producer(
        state,
        &publisher,
        publisher_connection,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        source_media,
        sample_video_rtp_parameters(None, 77_777),
    );
    let mut setups = state
        .plan_missing_consumers(&subscriber, subscriber_connection)
        .expect("subscriber should still be current");
    let setup = setups
        .pop()
        .expect("relay consumer setup should be planned");
    assert!(setups.is_empty());
    assert!(
        setup
            .relays()
            .iter()
            .any(|effect| effect.action == TransportRelayRouteAction::Install)
    );
    let setup = setup.declared(consumer_media, None);
    let (_, _, setup_outcome) = state.commit_declared_consumer_setup(setup);
    assert!(matches!(
        setup_outcome,
        ConsumerSetupOutcome::Committed { .. }
    ));
    RelayedSource {
        publisher,
        publisher_connection,
        publisher_session,
        source_media,
    }
}

fn has_source_relay_release(effects: &[ResolvedRelayRouteEffect], relay: &RelayedSource) -> bool {
    effects.iter().any(|effect| {
        effect.source_session_key == relay.publisher_session
            && effect.route.source_user == relay.publisher
            && effect.route.source_connection == relay.publisher_connection
            && effect.route.source_media == relay.source_media
            && effect.action == TransportRelayRouteAction::Release
    })
}

#[test]
fn disconnect_sessions_removes_current_members_and_fanouts_departures() {
    let mut state = test_state();
    let user_a = UserId::Integer(1);
    let user_b = UserId::Integer(2);
    let connection_a = join_test_user(&mut state, &user_a);
    let connection_b = join_test_user(&mut state, &user_b);

    let outcome = state.apply_disconnect_users(&[user_a.clone(), user_b.clone()]);

    assert_eq!(state.users.len(), 0);
    assert_eq!(outcome.close_operations.len(), 2);
    assert!(outcome.close_operations.iter().any(|operation| {
        let session = operation.session_key();
        session.user_id() == &user_a && session.connection_id() == connection_a
    }));
    assert!(outcome.close_operations.iter().any(|operation| {
        let session = operation.session_key();
        session.user_id() == &user_b && session.connection_id() == connection_b
    }));
    assert_eq!(outcome.effects.close_requests.len(), 2);
    assert_eq!(outcome.effects.fanouts.len(), 2);
}

#[test]
fn stale_close_unregisters_committed_placement_and_returns_cleanup() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);
    let connection_id = join_test_user_on_placement(
        &mut state,
        &user_id,
        RouterPlacement {
            router: RouterId(2),
            media_worker: MediaWorkerId::from_raw(1),
        },
    );
    let session_key = state.transport_user_key(&user_id, connection_id);
    state
        .users
        .remove(&user_id)
        .expect("test should leave a stale committed placement");

    let commit = state
        .close_connection(&user_id, connection_id)
        .expect("stale committed placement should close");

    assert!(matches!(
        commit,
        ConnectionCloseCommit::StalePlacement {
            cleanup: TransportCleanupOperation::CloseUser {
                session_key: cleanup_key,
            },
            ..
        } if cleanup_key == session_key
    ));
    assert_eq!(
        state.committed_transport_user_key(&user_id, connection_id),
        None
    );
}

#[test]
fn leave_removes_consumer_routes_for_departed_session() {
    let mut state = test_state();
    let producer_connection_id = join_test_user(&mut state, &UserId::Integer(1));
    let consumer_connection_id = join_test_user(&mut state, &UserId::Integer(2));
    let (_, source_id) = install_test_published_producer(
        &mut state,
        &UserId::Integer(1),
        producer_connection_id,
        TestSourceKind::ScalableVideo,
        RoutedProducerId::for_test(RouterId(1), ProducerId(10)),
        TransportMediaId::new(11),
        MediaStream::new(vec![], vec![], vec![]),
    );
    let consumer_key = ConsumerKey::new(&UserId::Integer(2), source_id);
    assert!(state.topology.commit_consumer_route_for_test(
        consumer_key,
        ConsumerState {
            routed_consumer_id: RoutedConsumerId::for_test(RouterId(1), ConsumerId(20)),
            consumer_connection_id,
            source_connection_id: producer_connection_id,
            source_media: TransportMediaId::new(11),
            consumer_media: TransportMediaId::new(21),
            consumer_mid: "camera-down".to_owned(),
        },
        ConsumerSourceSelection::open(true),
    ));

    let outcome = state.close_connection(&UserId::Integer(2), consumer_connection_id);

    assert!(outcome.is_some());
    assert_eq!(state.consumer_count(), 0);
    assert_eq!(state.producer_count(), 1);
    assert!(state.topology.source(source_id).is_some());
}

#[test]
fn stale_connection_cannot_broadcast() {
    let mut state = test_state();
    join_test_user(&mut state, &UserId::Integer(1));

    let fanout = state.broadcast_fanout(
        &UserId::Integer(1),
        ConnectionId::from_raw(999),
        serde_json::Value::String(String::from("hello")),
    );

    assert!(matches!(fanout, Ok(None)));
}

#[test]
fn presence_update_returns_none_for_stale_connection() {
    let mut state = test_state();
    join_test_user(&mut state, &UserId::Integer(1));

    let outcome = state.apply_presence_update(
        &UserId::Integer(1),
        ConnectionId::from_raw(999),
        &UserInfo::default(),
        RemoteSourceRefresh::OwnerConsumers,
    );

    assert!(outcome.is_none());
}

#[test]
fn disconnect_sessions_ignores_missing_members() {
    let mut state = test_state();
    let outcome = state.apply_disconnect_users(&[UserId::Integer(1)]);
    let (relays, cleanup) = outcome.transport_plan.relays_and_cleanup();

    assert!(relays.is_empty());
    assert!(cleanup.is_empty());
    assert!(outcome.effects.close_requests.is_empty());
    assert!(outcome.effects.fanouts.is_empty());
}

#[test]
fn replacement_join_releases_relay_with_displaced_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state
        .apply_join(&relay.publisher, UserPermissions::default(), test_sender())
        .expect("replacement join should succeed");
    let (relays, cleanup) = outcome.transport_plan.relays_and_cleanup();

    assert!(has_source_relay_release(relays, &relay));
    assert!(cleanup.iter().any(|operation| {
        matches!(
            operation,
            TransportCleanupOperation::CloseUser {
                session_key,
            } if session_key == &relay.publisher_session
        )
    }));
    assert!(!cleanup.iter().any(|operation| {
        matches!(
            operation,
            TransportCleanupOperation::RemoveMedia { session_key, .. }
                if session_key == &relay.publisher_session
        )
    }));
}

#[test]
fn leave_releases_relay_before_forgetting_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state
        .close_connection(&relay.publisher, relay.publisher_connection)
        .expect("publisher leave should succeed");
    let ConnectionCloseCommit::Current { transport_plan, .. } = outcome else {
        panic!("publisher leave should remove the current user");
    };
    let (relays, _) = transport_plan.relays_and_cleanup();

    assert!(has_source_relay_release(relays, &relay));
}

#[test]
fn disconnect_releases_relay_before_forgetting_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state.apply_disconnect_users(from_ref(&relay.publisher));
    let (relays, _) = outcome.transport_plan.relays_and_cleanup();

    assert!(has_source_relay_release(relays, &relay));
}
