#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "state-level test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{slice::from_ref, sync::Arc};

use o_sfu_router::{ConsumerId, MediaKind, MediaStream, ProducerId, RouterId};

use super::*;
use crate::{
    MediaCodecFlags, RoomMediaLimits,
    runtime::{
        ConnectionId, RoomInstanceId, TestSourceKind, UserPermissions,
        media_transport::TransportMediaId,
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        recording::RecordingService,
        room::{
            LocalRouterRuntimeContext, RoomAdmissionPolicy, RoomRuntimeContext, UserOutboundSender,
            media_graph::{
                ConsumerKey, ConsumerState, ProducerRuntimeId, PublishedProducer,
                PublishedSourceInstall, SourceKey,
            },
            rtp_capabilities::router_rtp_capabilities,
            topology::{RoutedConsumerId, RoutedProducerId},
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
            media_worker: 0,
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
        source_key: SourceKey::new(user_id, intent.stream_id()),
        source_descriptor: source,
        source_encoding_ids: vec![encoding_id],
        producer_id,
        producer: PublishedProducer {
            source_id,
            owner_user_id: user_id.clone(),
            owner_connection_id: connection_id,
            stream_id: stream_id_for_source(stream_type),
            media_kind: MediaKind::Video,
            consumable_rtp_parameters: MediaStream::new(vec![], vec![], vec![]),
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            active: true,
        },
        transport_media_id,
    });
    (producer_id, source_id)
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
    state.topology.remove_router_for_test(RouterId(1));

    let outcome = state.apply_leave(&user_id, connection_id);

    assert!(outcome.is_some());
    assert!(!state.users.contains_key(&user_id));
    assert_eq!(state.topology.home_router_id_for_user(&user_id), None);
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
    state.topology.remove_router_for_test(RouterId(1));

    let outcome = state.apply_disconnect_users(from_ref(&user_id));

    assert_eq!(outcome.disconnected_users.len(), 1);
    assert!(!state.users.contains_key(&user_id));
    assert_eq!(state.topology.home_router_id_for_user(&user_id), None);
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
    let (producer_id, source_id) = install_test_published_producer(
        &mut state,
        &UserId::Integer(1),
        producer_connection_id,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        TransportMediaId::new(11),
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
    assert_eq!(
        state.media.producer_ids_for_user(&UserId::Integer(1)),
        vec![producer_id]
    );
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

    assert!(outcome.transport_removals.is_empty());
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
        .topology
        .add_producer(&user_id, MediaKind::Video)
        .expect("replacement test producer route should be added");
    install_test_published_producer(
        &mut state,
        &user_id,
        connection_id,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        transport_media_id,
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
    assert!(state.media.publication_state_is_empty());
}
