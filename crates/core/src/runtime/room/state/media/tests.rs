#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{collections::BTreeSet, sync::Arc};

use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, ProducerId, RouterId,
    derive_consumable_rtp_parameters,
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
        sample_video_rtp_parameters,
    },
};

use super::super::{
    ids::ProducerRuntimeId,
    shared::{
        ConsumerKey, ConsumerState, PublishedProducer, RoomState, SourceKey,
        SourceTransportMediaIndexEntry,
    },
};
use crate::{
    Bitrate, MediaCodecFlags,
    runtime::{
        ConnectionId, RoomInstanceId, TestSourceKind, UserId, UserPermissions,
        media_transport::{SessionUploadEncoding, TransportMediaId},
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        recording::RecordingService,
        room::{
            LocalRouterRuntimeContext, RoomAdmissionPolicy, RoomRuntimeContext, UserOutboundSender,
            rtp_capabilities::router_rtp_capabilities,
            topology::{RoutedConsumerId, RoutedProducerId},
            user_negotiation::UserTransportReady,
        },
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceDescriptorParts,
            PublishedSourceId, PublishedSourceOwner, SourceEncodingDescriptor,
            SourceEncodingDescriptorParts, SourceEncodingId, SourceSelector, UploadLayerPolicyRole,
            test_support::{
                TestSubscriptionStates, source_kind_for_stream_id,
                source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
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
        router_rtp_capabilities(MediaCodecFlags::default()),
        crate::RoomWorkerPolicy::strict_single_router(),
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

fn join_test_user(state: &mut RoomState, user_id: &UserId) {
    let sender = test_sender();
    assert!(
        state
            .apply_join(user_id, None, UserPermissions::default(), sender, false,)
            .is_ok()
    );
}

fn test_upload_encodings() -> Vec<SessionUploadEncoding> {
    vec![
        SessionUploadEncoding {
            rid: "lo".to_owned(),
            max_bitrate: Some(Bitrate::from_kbps(180)),
            resolution_scale: Some(4),
            max_framerate: Some(15),
        },
        SessionUploadEncoding {
            rid: "hi".to_owned(),
            max_bitrate: Some(Bitrate::from_kbps(950)),
            resolution_scale: Some(1),
            max_framerate: None,
        },
    ]
}

fn assert_upload_profile_metadata(encodings: &[&SourceEncodingDescriptor]) {
    assert_eq!(encodings[0].resolution_scale(), Some(4));
    assert_eq!(encodings[0].max_framerate(), Some(15));
    assert_eq!(
        encodings[0].policy_role(),
        Some(UploadLayerPolicyRole::Thumbnail)
    );
    assert_eq!(encodings[1].resolution_scale(), Some(1));
    assert_eq!(encodings[1].max_framerate(), None);
    assert_eq!(
        encodings[1].policy_role(),
        Some(UploadLayerPolicyRole::Featured)
    );
}

fn install_test_consumer_route(
    state: &mut RoomState,
    producer_user_id: &UserId,
    consumer_user_id: &UserId,
) -> (ConsumerKey, ConnectionId) {
    let producer_connection_id = state
        .user_connection_id(producer_user_id)
        .expect("producer user should have a connection id");
    let consumer_connection_id = state
        .user_connection_id(consumer_user_id)
        .expect("consumer user should have a connection id");
    let routed_producer_id = state
        .topology
        .add_producer(producer_user_id, RouterMediaKind::Video)
        .unwrap_or_else(|error| panic!("failed to create test producer route: {error:?}"));
    let routed_consumer_id = state
        .topology
        .add_consumer(
            consumer_user_id,
            routed_producer_id,
            RouterMediaKind::Video,
            ConsumerCapability::Compatible,
        )
        .unwrap_or_else(|error| panic!("failed to create test consumer route: {error:?}"));
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let source_id = install_test_source_graph(
        state,
        producer_user_id,
        producer_connection_id,
        TestSourceKind::ScalableVideo,
        producer_id,
        TransportMediaId::new(1),
    );
    state.media.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_user_id: producer_user_id.clone(),
            owner_connection_id: producer_connection_id,
            stream_id: stream_id_for_source(TestSourceKind::ScalableVideo),
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters: sample_video_rtp_parameters(None, 77_777),
            routed_producer_id,
            transport_media_id: Some(TransportMediaId::new(1)),
            active: true,
        },
    );
    state.register_producer_owner(producer_user_id, producer_id);
    let route_key = ConsumerKey::new(consumer_user_id, source_id);
    let consumer_state = ConsumerState {
        routed_consumer_id,
        consumer_connection_id,
        source_connection_id: producer_connection_id,
        source_media: TransportMediaId::new(1),
        consumer_media: TransportMediaId::new(2),
    };
    state
        .media
        .consumer_index
        .insert(route_key.clone(), consumer_state);
    state.register_consumer_key(&route_key);
    (route_key, consumer_connection_id)
}

fn install_test_source_graph(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    producer_id: ProducerRuntimeId,
    transport_media_id: TransportMediaId,
) -> PublishedSourceId {
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
    state.media.sources.insert(source_id, source);
    state
        .media
        .source_ids_by_owner_stream
        .insert(SourceKey::new(user_id, intent.stream_id()), source_id);
    state
        .media
        .producer_id_by_source_id
        .insert(source_id, producer_id);
    state.register_source_owner(user_id, source_id);
    state.media.source_transport_media_index.insert(
        transport_media_id,
        SourceTransportMediaIndexEntry::new(
            source_id,
            vec![encoding_id],
            user_id.clone(),
            connection_id,
            intent.stream_id().clone(),
        ),
    );
    source_id
}

fn install_test_published_producer(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
    transport_media_id: TransportMediaId,
) -> (ProducerRuntimeId, PublishedSourceId) {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("publisher user should have a connection id");
    let routed_producer_id = state
        .topology
        .add_producer(user_id, RouterMediaKind::Video)
        .unwrap_or_else(|error| panic!("failed to create test producer route: {error:?}"));
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let source_id = install_test_source_graph(
        state,
        user_id,
        connection_id,
        stream_type,
        producer_id,
        transport_media_id,
    );
    state.media.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_user_id: user_id.clone(),
            owner_connection_id: connection_id,
            stream_id: stream_id_for_source(stream_type),
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters: sample_video_rtp_parameters(None, 77_777),
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            active: true,
        },
    );
    state.register_producer_owner(user_id, producer_id);
    (producer_id, source_id)
}

fn install_test_consumer_state(
    state: &mut RoomState,
    consumer_user_id: &UserId,
    source_id: PublishedSourceId,
    source_connection_id: ConnectionId,
    source_media: TransportMediaId,
    consumer_media: TransportMediaId,
) -> ConsumerKey {
    let consumer_connection_id = state
        .user_connection_id(consumer_user_id)
        .expect("consumer user should have a connection id");
    let key = ConsumerKey::new(consumer_user_id, source_id);
    state.media.consumer_index.insert(
        key.clone(),
        ConsumerState {
            routed_consumer_id: RoutedConsumerId::new(
                RouterId(1),
                o_sfu_router::ConsumerId(consumer_media.as_u64()),
            ),
            consumer_connection_id,
            source_connection_id,
            source_media,
            consumer_media,
        },
    );
    state.register_consumer_key(&key);
    key
}

#[test]
fn producer_activity_does_not_flip_room_state_when_router_update_fails() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);
    let sender = test_sender();

    let join = state.apply_join(&user_id, None, UserPermissions::default(), sender, false);
    assert!(join.is_ok());
    let connection_id = state
        .user_connection_id(&user_id)
        .unwrap_or(ConnectionId::from_raw(u64::MAX));

    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(777));
    let transport_media_id = TransportMediaId::default();
    let source_id = install_test_source_graph(
        &mut state,
        &user_id,
        connection_id,
        TestSourceKind::ScalableVideo,
        producer_id,
        transport_media_id,
    );
    state.media.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_user_id: user_id.clone(),
            owner_connection_id: connection_id,
            stream_id: stream_id_for_source(TestSourceKind::ScalableVideo),
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters: sample_video_rtp_parameters(None, 77_777),
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            active: true,
        },
    );
    state.register_producer_owner(&user_id, producer_id);

    let producer_target = state
        .producer_route_target(
            &user_id,
            connection_id,
            &stream_id_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("inserted producer should resolve back to a route target");
    let outcome = state.apply_producer_activity(
        &user_id,
        &producer_target,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        false,
    );
    assert!(outcome.is_none());
    assert!(
        state
            .media
            .producers
            .get(&producer_id)
            .is_some_and(|producer| producer.active),
        "room state must keep the previous activity flag when router pause propagation fails"
    );
}

#[test]
fn stale_replaced_connection_cannot_update_download_state() {
    let mut state = test_state();
    let producer_user_id = UserId::Integer(1);
    let consumer_user_id = UserId::Integer(2);
    let replacement_sender = test_sender();

    join_test_user(&mut state, &producer_user_id);
    join_test_user(&mut state, &consumer_user_id);
    let (route_key, stale_connection_id) =
        install_test_consumer_route(&mut state, &producer_user_id, &consumer_user_id);

    assert!(
        state
            .apply_join(
                &consumer_user_id,
                Some(String::from("replacement")),
                UserPermissions::default(),
                replacement_sender,
                false,
            )
            .is_ok()
    );

    let states = TestSubscriptionStates {
        scalable_video: Some(false),
        audio_detector: None,
        readable_video: None,
        ..TestSubscriptionStates::default()
    };
    let intents = subscription_intents_from_test_states(&states);
    let (committed_updates, planned_bootstraps, relay_effects) = state
        .plan_subscription_change(
            &consumer_user_id,
            stale_connection_id,
            &producer_user_id,
            &intents,
        )
        .into_parts();

    assert!(committed_updates.is_empty());
    assert!(planned_bootstraps.is_empty());
    assert!(relay_effects.is_empty());
    assert!(
        state.desired_source_subscription_active(
            &consumer_user_id,
            &producer_user_id,
            &stream_id_for_source(TestSourceKind::ScalableVideo),
        ),
        "stale subscription updates must not overwrite the replacement user's stored preferences"
    );
    assert_eq!(
        state.media.consumer_index.get(&route_key),
        None,
        "replacement join should clear stale consumer routes before the new connection reboots them"
    );
}

#[test]
fn subscription_change_reserves_missing_bootstrap_for_existing_publisher() {
    let mut state = test_state();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let publisher_connection_id = state
        .user_connection_id(&publisher_user_id)
        .expect("publisher should have a connection id");
    let subscriber_connection_id = state
        .user_connection_id(&subscriber_user_id)
        .expect("subscriber should have a connection id");

    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &subscriber_user_id,
                subscriber_connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(
                &subscriber_user_id,
                subscriber_connection_id,
                UserTransportReady::Consume,
            )
            .session_present
    );

    let routed_producer_id = state
        .topology
        .add_producer(&publisher_user_id, RouterMediaKind::Video)
        .expect("publisher route should be added");
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let producer_rtp_parameters = sample_video_rtp_parameters(None, 22_222);
    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &producer_rtp_parameters,
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let source_id = install_test_source_graph(
        &mut state,
        &publisher_user_id,
        publisher_connection_id,
        TestSourceKind::ScalableVideo,
        producer_id,
        TransportMediaId::new(10),
    );
    state.media.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_user_id: publisher_user_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_id: stream_id_for_source(TestSourceKind::ScalableVideo),
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters,
            routed_producer_id,
            transport_media_id: Some(TransportMediaId::new(10)),
            active: true,
        },
    );
    state.register_producer_owner(&publisher_user_id, producer_id);

    let states = TestSubscriptionStates {
        scalable_video: Some(false),
        audio_detector: None,
        readable_video: None,
        ..TestSubscriptionStates::default()
    };
    let intents = subscription_intents_from_test_states(&states);
    let (route_updates, planned_bootstraps, relay_effects) = state
        .plan_subscription_change(
            &subscriber_user_id,
            subscriber_connection_id,
            &publisher_user_id,
            &intents,
        )
        .into_parts();

    assert!(route_updates.is_empty());
    assert!(relay_effects.is_empty());
    assert_eq!(planned_bootstraps.len(), 1);
    let selection = state
        .media
        .consumer_source_selections
        .get(&ConsumerKey::new(&subscriber_user_id, source_id))
        .expect("compat subscription should create a source-level selection");
    assert!(!selection.active());
    assert_eq!(
        selection.selector(),
        SourceSelector::Open,
        "compat downloads default to an unconstrained source selector"
    );
    assert!(
        state.subscription_count() >= 1,
        "planning the bootstrap must reserve the pending consumer slot immediately"
    );
}

#[test]
fn commit_published_track_populates_transport_media_owner_index() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);

    join_test_user(&mut state, &user_id);
    let connection_id = state
        .user_connection_id(&user_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &user_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(&user_id, connection_id, UserTransportReady::Publish,)
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, 42_000),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &user_id,
            connection_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("publish descriptor should validate once the user is publish-ready")
        .into_prepared_track(consumable_rtp_parameters);
    let transport_media_id = TransportMediaId::new(99);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );
    assert_eq!(
        state
            .producer_stream_id_for_transport_media_id(transport_media_id)
            .and_then(|stream_id| source_kind_for_stream_id(&stream_id)),
        Some(TestSourceKind::ScalableVideo),
    );
    assert_eq!(
        state.inspect_producer_owner_user_id_for_transport_media_id(transport_media_id),
        Some(user_id)
    );
    assert_eq!(
        state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
        Some(connection_id)
    );
    assert_eq!(state.media.sources.len(), 1);
    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to a source id");
    assert!(
        state.media.sources.contains_key(&source_id),
        "transport media source id should point into the source registry"
    );
    assert_eq!(
        state
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .expect("transport media should resolve to source encodings")
            .len(),
        1
    );
}

#[test]
fn commit_published_track_registers_all_source_encodings() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);

    join_test_user(&mut state, &user_id);
    let connection_id = state
        .user_connection_id(&user_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &user_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(&user_id, connection_id, UserTransportReady::Publish,)
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_simulcast_video_rtp_parameters(Some("camera-0")),
        &state.router_rtp_capabilities(),
    )
    .expect("simulcast RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &user_id,
            connection_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("publish descriptor should validate once the user is publish-ready")
        .into_prepared_track_with_upload_encodings(
            consumable_rtp_parameters,
            test_upload_encodings(),
        );
    let transport_media_id = TransportMediaId::new(101);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );

    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to the committed source");
    let source = state
        .media
        .sources
        .get(&source_id)
        .expect("source registry should own the committed source");
    assert_eq!(source.owner().user_id(), &user_id);
    assert_eq!(
        source_kind_for_stream_id(source.stream_id()),
        Some(TestSourceKind::ScalableVideo)
    );
    assert_eq!(
        source.mid().map(o_sfu_router::Mid::as_str),
        Some("camera-0")
    );
    let encodings = source.encodings().collect::<Vec<_>>();
    assert_eq!(encodings.len(), 2);
    assert_eq!(
        encodings[0].rid().map(o_sfu_router::Rid::as_str),
        Some("lo")
    );
    assert_eq!(
        encodings[1].rid().map(o_sfu_router::Rid::as_str),
        Some("hi")
    );
    assert_eq!(
        encodings[0].primary_ssrc(),
        Some(o_sfu_router::Ssrc::new(31_001))
    );
    assert_eq!(
        encodings[1].primary_ssrc(),
        Some(o_sfu_router::Ssrc::new(31_002))
    );
    assert_eq!(encodings[0].max_bitrate(), Some(Bitrate::from_kbps(150)));
    assert_eq!(encodings[1].max_bitrate(), Some(Bitrate::from_kbps(900)));
    assert_upload_profile_metadata(&encodings);
    assert_eq!(
        state
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .expect("transport media should resolve to source encoding ids"),
        encodings
            .iter()
            .map(|encoding| encoding.encoding_id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn unpublish_track_clears_transport_media_owner_index() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);

    join_test_user(&mut state, &user_id);
    let connection_id = state
        .user_connection_id(&user_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &user_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(&user_id, connection_id, UserTransportReady::Publish,)
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, 43_000),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &user_id,
            connection_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("publish descriptor should validate once the user is publish-ready")
        .into_prepared_track(consumable_rtp_parameters);
    let transport_media_id = TransportMediaId::new(100);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );
    assert!(
        state
            .unpublish_track(
                &user_id,
                connection_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            )
            .is_some()
    );
    assert_eq!(
        state.producer_stream_id_for_transport_media_id(transport_media_id),
        None
    );
    assert_eq!(
        state.inspect_producer_owner_user_id_for_transport_media_id(transport_media_id),
        None
    );
    assert_eq!(
        state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
        None
    );
    assert!(state.media.sources.is_empty());
    assert!(state.media.source_ids_by_owner_stream.is_empty());
    assert!(state.media.source_ids_by_owner.is_empty());
    assert!(state.media.producer_id_by_source_id.is_empty());
    assert!(state.media.producer_ids_by_owner.is_empty());
}

#[test]
fn unpublish_track_repairs_missing_topology_router_and_clears_state() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);

    join_test_user(&mut state, &user_id);
    let connection_id = state
        .user_connection_id(&user_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &user_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(&user_id, connection_id, UserTransportReady::Publish,)
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, 43_000),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &user_id,
            connection_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("publish descriptor should validate once the user is publish-ready")
        .into_prepared_track(consumable_rtp_parameters);
    let transport_media_id = TransportMediaId::new(100);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );
    state.topology.remove_router_for_test(RouterId(1));
    assert!(
        state
            .unpublish_track(
                &user_id,
                connection_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            )
            .is_some()
    );

    assert_eq!(
        state.producer_stream_id_for_transport_media_id(transport_media_id),
        None
    );
    assert!(state.media.source_ids_by_owner_stream.is_empty());
    assert!(state.media.producer_id_by_source_id.is_empty());
    assert!(state.media.producers.is_empty());
}

#[test]
fn purge_user_media_state_removes_only_indexed_user_and_source_entries() {
    let mut state = test_state();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let other_publisher_id = UserId::Integer(3);

    join_test_user(&mut state, &publisher_id);
    join_test_user(&mut state, &subscriber_id);
    join_test_user(&mut state, &other_publisher_id);

    let publisher_connection_id = state
        .user_connection_id(&publisher_id)
        .expect("publisher should have a connection id");
    let other_publisher_connection_id = state
        .user_connection_id(&other_publisher_id)
        .expect("other publisher should have a connection id");
    let (_publisher_producer_id, publisher_source_id) = install_test_published_producer(
        &mut state,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
    );
    let (other_producer_id, other_source_id) = install_test_published_producer(
        &mut state,
        &other_publisher_id,
        TestSourceKind::ReadableVideo,
        TransportMediaId::new(30),
    );

    let removed_consumer_key = install_test_consumer_state(
        &mut state,
        &subscriber_id,
        publisher_source_id,
        publisher_connection_id,
        TransportMediaId::new(10),
        TransportMediaId::new(20),
    );
    state.media.consumer_source_selections.insert(
        removed_consumer_key.clone(),
        ConsumerSourceSelection::open(true),
    );
    state.register_consumer_key(&removed_consumer_key);
    let pending_removed_key = ConsumerKey::new(&publisher_id, other_source_id);
    state
        .media
        .pending_consumer_bootstraps
        .insert(pending_removed_key.clone());
    state.register_consumer_key(&pending_removed_key);

    let surviving_consumer_key = install_test_consumer_state(
        &mut state,
        &subscriber_id,
        other_source_id,
        other_publisher_connection_id,
        TransportMediaId::new(30),
        TransportMediaId::new(40),
    );
    state.media.consumer_source_selections.insert(
        surviving_consumer_key.clone(),
        ConsumerSourceSelection::open(false),
    );
    state.register_consumer_key(&surviving_consumer_key);

    state.purge_user_media_state(&publisher_id);

    let media = &state.media;
    assert!(!media.sources.contains_key(&publisher_source_id));
    assert!(media.sources.contains_key(&other_source_id));
    assert!(!media.consumer_index.contains_key(&removed_consumer_key));
    assert!(
        !media
            .pending_consumer_bootstraps
            .contains(&pending_removed_key)
    );
    assert!(
        !media
            .consumer_source_selections
            .contains_key(&removed_consumer_key)
    );
    assert!(media.consumer_index.contains_key(&surviving_consumer_key));
    assert!(
        media
            .consumer_source_selections
            .contains_key(&surviving_consumer_key)
    );
    assert!(
        media
            .consumer_keys_by_user
            .get(&subscriber_id)
            .is_some_and(|keys| keys == &BTreeSet::from([surviving_consumer_key.clone()]))
    );
    assert!(
        media
            .consumer_keys_by_source
            .get(&other_source_id)
            .is_some_and(|keys| keys == &BTreeSet::from([surviving_consumer_key]))
    );
    assert_eq!(
        media.producer_ids_by_owner.get(&other_publisher_id),
        Some(&BTreeSet::from([other_producer_id]))
    );
    assert!(!media.producer_ids_by_owner.contains_key(&publisher_id));
    assert!(!media.source_ids_by_owner.contains_key(&publisher_id));
}
