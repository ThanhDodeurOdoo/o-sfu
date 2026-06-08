#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "state-level test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{slice::from_ref, sync::Arc};

use o_sfu_router::{
    ConsumerId, MediaKind, MediaStream, ProducerId, RouterId,
    test_support::rtp_samples::{sample_client_rtp_capabilities, sample_video_rtp_parameters},
};

use super::*;
use crate::{
    MediaCodecFlags, RoomMediaLimits,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, TestSourceKind, UserPermissions,
        media_transport::{TransportMediaId, TransportRelayRouteAction, TransportSessionKey},
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        recording::RecordingService,
        room::{
            LocalRouterRuntimeContext, RoomAdmissionPolicy, RoomRuntimeContext, UserOutboundSender,
            media_graph::{
                ConsumerKey, ConsumerSetupOutcome, ConsumerSetupTarget, ConsumerState,
                ProducerRuntimeId, PublishedProducer, PublishedSourceInstall,
                ResolvedRelayRouteEffect,
            },
            routing::{RoutedConsumerId, RoutedProducerId},
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
    let packet_sink_registry = Arc::new(RoomPacketSinkRegistry::default());
    let runtime_context = RoomRuntimeContext::new(
        RoomInstanceId::from_raw(0),
        LocalRouterRuntimeContext {
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
        Arc::new(RecordingService::new(
            RoomInstanceId::from_raw(0),
            packet_sink_registry,
            Arc::new(RuntimeMetrics::default()),
        )),
    )
}

fn test_sender() -> UserOutboundSender {
    UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default())).0
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
                max_temporal_layer_id: None,
                negotiated_format: None,
            },
        )],
    })
    .expect("test source graph should be valid");
    state.media.install_source(PublishedSourceInstall {
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
    assert!(
        state
            .apply_join(
                &publisher,
                None,
                UserPermissions::default(),
                test_sender(),
                false,
            )
            .is_ok()
    );
    assert!(
        state
            .apply_join(
                &subscriber,
                None,
                UserPermissions::default(),
                test_sender(),
                false,
            )
            .is_ok()
    );
    let publisher_connection = state
        .user_connection_id(&publisher)
        .expect("publisher should be joined");
    let subscriber_connection = state
        .user_connection_id(&subscriber)
        .expect("subscriber should be joined");
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
        .routing
        .add_producer(&publisher, MediaKind::Video)
        .expect("relayed source producer route should be added");
    let (producer_id, source_id) = install_test_published_producer(
        state,
        &publisher,
        publisher_connection,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        source_media,
        sample_video_rtp_parameters(None, 77_777),
    );
    let target = {
        let producer = state
            .media
            .producer_for_source(source_id)
            .expect("relayed source producer should exist");
        ConsumerSetupTarget::new(
            subscriber.clone(),
            subscriber_connection,
            state.transport_user_key(&subscriber, subscriber_connection),
            state.transport_user_key(&publisher, publisher_connection),
            producer_id,
            producer,
            source_media,
        )
    };
    let mut setups = state.plan_consumers(vec![target], |connection| {
        if connection == subscriber_connection {
            MediaWorkerId::from_raw(1)
        } else {
            MediaWorkerId::from_raw(0)
        }
    });
    let setup = setups
        .pop()
        .expect("relay consumer setup should be planned");
    assert!(setups.is_empty());
    assert!(
        setup
            .relays
            .iter()
            .any(|effect| effect.action == TransportRelayRouteAction::Install)
    );
    let (_, _, setup_outcome) = state.commit_pending_consumer_setup(setup, consumer_media, None);
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
    let sender_a = test_sender();
    let sender_b = test_sender();
    assert!(
        state
            .apply_join(
                &UserId::Integer(1),
                None,
                UserPermissions::default(),
                sender_a,
                false,
            )
            .is_ok()
    );
    assert!(
        state
            .apply_join(
                &UserId::Integer(2),
                None,
                UserPermissions::default(),
                sender_b,
                false,
            )
            .is_ok()
    );

    let outcome = state.apply_disconnect_users(&[UserId::Integer(1), UserId::Integer(2)]);

    assert_eq!(state.users.len(), 0);
    assert_eq!(outcome.disconnected_users.len(), 2);
    assert!(outcome.disconnected_users.iter().any(|user| {
        user.user_id == UserId::Integer(1) && user.connection_id == ConnectionId::from_raw(0)
    }));
    assert!(outcome.disconnected_users.iter().any(|user| {
        user.user_id == UserId::Integer(2) && user.connection_id == ConnectionId::from_raw(1)
    }));
    assert_eq!(outcome.effects.close_requests.len(), 2);
    assert_eq!(outcome.effects.fanouts.len(), 2);
}

#[test]
fn leave_repairs_missing_topology_router_and_removes_member() {
    let mut state = test_state();
    let sender = test_sender();
    let user_id = UserId::Integer(1);
    assert!(
        state
            .apply_join(&user_id, None, UserPermissions::default(), sender, false,)
            .is_ok()
    );
    let connection_id = state
        .user_connection_id(&user_id)
        .expect("joined user should have a connection id");
    state.routing.remove_router_for_test(RouterId(1));

    let outcome = state.apply_leave(&user_id, connection_id);

    assert!(outcome.is_some());
    assert!(!state.users.contains_key(&user_id));
    assert_eq!(state.routing.home_router_id_for_user(&user_id), None);
}

#[test]
fn disconnect_repairs_missing_topology_router_and_removes_member() {
    let mut state = test_state();
    let sender = test_sender();
    let user_id = UserId::Integer(1);
    assert!(
        state
            .apply_join(&user_id, None, UserPermissions::default(), sender, false,)
            .is_ok()
    );
    state.routing.remove_router_for_test(RouterId(1));

    let outcome = state.apply_disconnect_users(from_ref(&user_id));

    assert_eq!(outcome.disconnected_users.len(), 1);
    assert!(!state.users.contains_key(&user_id));
    assert_eq!(state.routing.home_router_id_for_user(&user_id), None);
}

#[test]
fn leave_removes_consumer_routes_for_departed_session() {
    let mut state = test_state();
    let producer_sender = test_sender();
    let consumer_sender = test_sender();
    assert!(
        state
            .apply_join(
                &UserId::Integer(1),
                None,
                UserPermissions::default(),
                producer_sender,
                false,
            )
            .is_ok()
    );
    assert!(
        state
            .apply_join(
                &UserId::Integer(2),
                None,
                UserPermissions::default(),
                consumer_sender,
                false,
            )
            .is_ok()
    );
    let producer_connection_id = state
        .user_connection_id(&UserId::Integer(1))
        .expect("producer user should exist");
    let consumer_connection_id = state
        .user_connection_id(&UserId::Integer(2))
        .expect("consumer user should exist");
    let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(10));
    let (_, source_id) = install_test_published_producer(
        &mut state,
        &UserId::Integer(1),
        producer_connection_id,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        TransportMediaId::new(11),
        MediaStream::new(vec![], vec![], vec![]),
    );
    let consumer_key = ConsumerKey::new(&UserId::Integer(2), source_id);
    assert!(state.media.commit_consumer(
        consumer_key,
        ConsumerState {
            routed_consumer_id: RoutedConsumerId::new(RouterId(1), ConsumerId(20)),
            consumer_connection_id,
            source_connection_id: producer_connection_id,
            source_media: TransportMediaId::new(11),
            consumer_media: TransportMediaId::new(21),
        },
        ConsumerSourceSelection::open(true),
    ));

    let outcome = state.apply_leave(&UserId::Integer(2), consumer_connection_id);

    assert!(outcome.is_some());
    assert_eq!(state.media.consumer_count(), 0);
    assert_eq!(state.media.producer_count(), 1);
    assert!(state.media.source(source_id).is_some());
}

#[test]
fn stale_connection_cannot_broadcast() {
    let mut state = test_state();
    let sender = test_sender();
    assert!(
        state
            .apply_join(
                &UserId::Integer(1),
                None,
                UserPermissions::default(),
                sender,
                false,
            )
            .is_ok()
    );

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
    let sender = test_sender();
    assert!(
        state
            .apply_join(
                &UserId::Integer(1),
                None,
                UserPermissions::default(),
                sender,
                false,
            )
            .is_ok()
    );

    let outcome = state.apply_presence_update(
        &UserId::Integer(1),
        ConnectionId::from_raw(999),
        &UserInfo::default(),
        false,
    );

    assert!(outcome.is_none());
}

#[test]
fn disconnect_sessions_ignores_missing_members() {
    let mut state = test_state();
    let outcome = state.apply_disconnect_users(&[UserId::Integer(1)]);
    let (relays, cleanup) = outcome.media_effects.into_parts();

    assert!(relays.is_empty());
    assert!(cleanup.is_empty());
    assert!(outcome.effects.close_requests.is_empty());
    assert!(outcome.effects.fanouts.is_empty());
}

#[test]
fn replacement_join_clears_transport_media_owner_index() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);
    let sender = test_sender();
    let replacement_sender = test_sender();
    assert!(
        state
            .apply_join(&user_id, None, UserPermissions::default(), sender, false,)
            .is_ok()
    );
    let connection_id = state
        .user_connection_id(&user_id)
        .expect("user should have a connection id");
    let transport_media_id = TransportMediaId::new(30);
    let routed_producer_id = state
        .routing
        .add_producer(&user_id, MediaKind::Video)
        .expect("replacement test producer route should be added");
    install_test_published_producer(
        &mut state,
        &user_id,
        connection_id,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        transport_media_id,
        MediaStream::new(vec![], vec![], vec![]),
    );

    assert_eq!(
        state.inspect_producer_owner_user_id_for_transport_media_id(transport_media_id),
        Some(user_id.clone())
    );
    assert_eq!(
        state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
        Some(connection_id)
    );

    assert!(
        state
            .apply_join(
                &user_id,
                Some(String::from("replacement")),
                UserPermissions::default(),
                replacement_sender,
                false,
            )
            .is_ok()
    );

    assert_eq!(
        state.inspect_producer_owner_user_id_for_transport_media_id(transport_media_id),
        None
    );
    assert_eq!(
        state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
        None
    );
    assert_eq!(state.media.publication_count(), 0);
    assert!(
        state
            .media
            .source_id_for_owner_stream(
                &user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .is_none()
    );
}

#[test]
fn replacement_join_releases_relay_with_displaced_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state
        .apply_join(
            &relay.publisher,
            Some(String::from("replacement")),
            UserPermissions::default(),
            test_sender(),
            false,
        )
        .expect("replacement join should succeed");
    let (relays, cleanup) = outcome.media_effects.into_parts();

    assert!(has_source_relay_release(&relays, &relay));
    assert!(cleanup.iter().any(|operation| {
        matches!(
            operation,
            TransportCleanupOperation::CloseUser {
                session_key,
                connection_id,
            } if session_key == &relay.publisher_session
                && *connection_id == relay.publisher_connection
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
        .apply_leave(&relay.publisher, relay.publisher_connection)
        .expect("publisher leave should succeed");
    let (relays, _cleanup) = outcome.media_effects.into_parts();

    assert!(has_source_relay_release(&relays, &relay));
}

#[test]
fn disconnect_releases_relay_before_forgetting_source_session() {
    let mut state = test_state();
    let relay = install_relayed_source(&mut state);

    let outcome = state.apply_disconnect_users(from_ref(&relay.publisher));
    let (relays, _cleanup) = outcome.media_effects.into_parts();

    assert!(has_source_relay_release(&relays, &relay));
}
