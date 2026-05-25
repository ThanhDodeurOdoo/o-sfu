#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::sync::Arc;

use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, ProducerId, RouterId,
    derive_consumable_rtp_parameters,
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
        sample_video_rtp_parameters,
    },
};

use super::{
    super::{ids::ProducerRuntimeId, shared::RoomState},
    ConsumerKey, ConsumerRouteState, ConsumerState, PublishedProducer, PublishedSourceInstall,
    SourceKey,
};
use crate::{
    Bitrate, MediaCodecFlags, RoomMediaLimits,
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
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceDescriptor,
            PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceSelector, UploadLayerPolicyRole,
            test_support::{
                TestSubscriptionStates, source_kind_for_stream_id,
                source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
    },
};

fn test_state() -> RoomState {
    test_state_with_media_limits(RoomMediaLimits::default())
}

fn test_state_with_media_limits(media_limits: RoomMediaLimits) -> RoomState {
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
        media_limits,
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

fn join_test_user(state: &mut RoomState, user_id: &UserId) {
    let sender = test_sender();
    assert!(
        state
            .apply_join(user_id, None, UserPermissions::default(), sender, false,)
            .is_ok()
    );
}

fn set_test_consumer_ready(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("consumer should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                user_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(user_id, connection_id, UserTransportReady::Consume,)
            .session_present
    );
    connection_id
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
    let source_id = install_test_published_producer_with_route(
        state,
        producer_user_id,
        producer_connection_id,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        sample_video_rtp_parameters(None, 77_777),
        TransportMediaId::new(1),
    )
    .1;
    let route_key = ConsumerKey::new(consumer_user_id, source_id);
    let consumer_state = ConsumerState {
        routed_consumer_id,
        consumer_connection_id,
        source_connection_id: producer_connection_id,
        source_media: TransportMediaId::new(1),
        consumer_media: TransportMediaId::new(2),
    };
    assert!(state.media.commit_consumer(
        route_key.clone(),
        consumer_state,
        ConsumerSourceSelection::open(true),
    ));
    (route_key, consumer_connection_id)
}

fn test_source_descriptor(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
) -> (PublishedSourceDescriptor, Vec<SourceEncodingId>) {
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
    (source, vec![encoding_id])
}

fn install_test_published_producer_with_route(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    routed_producer_id: RoutedProducerId,
    consumable_rtp_parameters: o_sfu_router::MediaStream,
    transport_media_id: TransportMediaId,
) -> (ProducerRuntimeId, PublishedSourceId) {
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let (source_descriptor, source_encoding_ids) =
        test_source_descriptor(state, user_id, stream_type);
    let source_id = source_descriptor.source_id();
    state.media.install_source(PublishedSourceInstall {
        source_key: SourceKey::new(user_id, source_descriptor.stream_id()),
        source_descriptor,
        source_encoding_ids,
        producer_id,
        producer: PublishedProducer {
            source_id,
            owner_user_id: user_id.clone(),
            owner_connection_id: connection_id,
            stream_id: stream_id_for_source(stream_type),
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters,
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            active: true,
        },
        transport_media_id,
    });
    (producer_id, source_id)
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
    install_test_published_producer_with_route(
        state,
        user_id,
        connection_id,
        stream_type,
        routed_producer_id,
        sample_video_rtp_parameters(None, 77_777),
        transport_media_id,
    )
}

fn install_test_consumable_video_producer(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
    transport_media_id: TransportMediaId,
    ssrc: u32,
) -> (ProducerRuntimeId, PublishedSourceId) {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("publisher should have a connection id");
    let routed_producer_id = state
        .topology
        .add_producer(user_id, RouterMediaKind::Video)
        .expect("publisher route should be added");
    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, ssrc),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    install_test_published_producer_with_route(
        state,
        user_id,
        connection_id,
        stream_type,
        routed_producer_id,
        consumable_rtp_parameters,
        transport_media_id,
    )
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
    assert!(state.media.commit_consumer(
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
        ConsumerSourceSelection::open(true),
    ));
    key
}

fn set_test_consumer_policy_pause(state: &mut RoomState, key: &ConsumerKey) {
    let route_ref = state
        .media
        .committed_consumer_route_for_key(key)
        .expect("consumer route should exist")
        .transport_ref();
    assert!(
        state
            .media
            .update_consumer_source_selection(&route_ref, key.source_id, |selection| {
                selection.set_policy_pause_reason(Some(PolicyPauseReason::VideoDownloadLimit));
            })
    );
}

fn two_user_consumer_route() -> (RoomState, UserId, UserId, ConsumerKey, ConnectionId) {
    let mut state = test_state();
    let producer_user_id = UserId::Integer(1);
    let consumer_user_id = UserId::Integer(2);

    join_test_user(&mut state, &producer_user_id);
    join_test_user(&mut state, &consumer_user_id);

    let (key, consumer_connection_id) =
        install_test_consumer_route(&mut state, &producer_user_id, &consumer_user_id);
    (
        state,
        producer_user_id,
        consumer_user_id,
        key,
        consumer_connection_id,
    )
}

#[test]
fn policy_paused_routes_do_not_count_as_effective_delivery() {
    let (mut state, producer_user_id, consumer_user_id, key, consumer_connection_id) =
        two_user_consumer_route();
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    assert!(state.source_fanout_pressure(1, |_| 0));
    assert_eq!(
        state.consumer_route_state(&consumer_user_id, &producer_user_id, &stream_id),
        Some(ConsumerRouteState::Active)
    );
    assert_eq!(
        state
            .active_video_consumer_keyframe_refresh_targets(
                &consumer_user_id,
                consumer_connection_id,
            )
            .expect("consumer should exist")
            .len(),
        1
    );

    set_test_consumer_policy_pause(&mut state, &key);

    assert!(!state.source_fanout_pressure(1, |_| 0));
    assert_eq!(
        state.consumer_route_state(&consumer_user_id, &producer_user_id, &stream_id),
        Some(ConsumerRouteState::Inactive)
    );
    assert!(
        state
            .active_video_consumer_keyframe_refresh_targets(
                &consumer_user_id,
                consumer_connection_id,
            )
            .expect("consumer should exist")
            .is_empty()
    );
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

    let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(777));
    let transport_media_id = TransportMediaId::default();
    let (producer_id, _source_id) = install_test_published_producer_with_route(
        &mut state,
        &user_id,
        connection_id,
        TestSourceKind::ScalableVideo,
        routed_producer_id,
        sample_video_rtp_parameters(None, 77_777),
        transport_media_id,
    );

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
            .producer(producer_id)
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
            |_| 0,
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
        state.media.consumer_state(&route_key),
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

    let subscriber_connection_id = set_test_consumer_ready(&mut state, &subscriber_user_id);
    let (_producer_id, source_id) = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );

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
            |_| 0,
        )
        .into_parts();

    assert!(route_updates.is_empty());
    assert!(relay_effects.is_empty());
    assert_eq!(planned_bootstraps.len(), 1);
    let selection = state
        .media
        .consumer_source_selection(&ConsumerKey::new(&subscriber_user_id, source_id))
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
fn missing_consumer_bootstrap_applies_video_download_cap_before_effects() {
    let mut state = test_state_with_media_limits(RoomMediaLimits::try_new(4, 1).unwrap());
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let subscriber_connection_id = set_test_consumer_ready(&mut state, &subscriber_user_id);
    let (_scalable_producer_id, scalable_source_id) = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );
    let (_readable_producer_id, readable_source_id) = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ReadableVideo,
        TransportMediaId::new(11),
        33_333,
    );
    for source_id in [scalable_source_id, readable_source_id] {
        state.media.ensure_consumer_source_selection(
            &ConsumerKey::new(&subscriber_user_id, source_id),
            ConsumerSourceSelection::open(true),
        );
    }

    let planned_bootstraps = state
        .plan_missing_consumer_bootstraps_for_connection(
            &subscriber_user_id,
            subscriber_connection_id,
            |_| 0,
        )
        .expect("subscriber session should still exist");

    assert_eq!(planned_bootstraps.len(), 2);
    let scalable_selection = state
        .media
        .consumer_source_selection(&ConsumerKey::new(&subscriber_user_id, scalable_source_id))
        .expect("scalable source should have a consumer selection");
    let readable_selection = state
        .media
        .consumer_source_selection(&ConsumerKey::new(&subscriber_user_id, readable_source_id))
        .expect("readable source should have a consumer selection");
    let selections = [scalable_selection, readable_selection];
    assert!(selections.iter().all(|selection| selection.active()));
    assert!(readable_selection.delivery_active());
    assert_eq!(
        scalable_selection.policy_pause_reason(),
        Some(PolicyPauseReason::VideoDownloadLimit)
    );
    assert_eq!(
        selections
            .iter()
            .filter(|selection| selection.delivery_active())
            .count(),
        1
    );
    assert_eq!(
        selections
            .iter()
            .filter(|selection| {
                selection.policy_pause_reason() == Some(PolicyPauseReason::VideoDownloadLimit)
            })
            .count(),
        1
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
    assert_eq!(state.publication_count(), 1);
    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to a source id");
    assert!(
        state.media.contains_source(source_id),
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
        .source(source_id)
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
    assert!(state.media.source_indexes_are_empty());
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
    assert!(state.media.source_indexes_are_empty());
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
    state.media.ensure_consumer_source_selection(
        &removed_consumer_key,
        ConsumerSourceSelection::open(true),
    );
    let pending_removed_key = ConsumerKey::new(&publisher_id, other_source_id);
    state
        .media
        .reserve_consumer_bootstrap(pending_removed_key.clone());

    let surviving_consumer_key = install_test_consumer_state(
        &mut state,
        &subscriber_id,
        other_source_id,
        other_publisher_connection_id,
        TransportMediaId::new(30),
        TransportMediaId::new(40),
    );
    state.media.ensure_consumer_source_selection(
        &surviving_consumer_key,
        ConsumerSourceSelection::open(false),
    );

    state.purge_user_media_state(&publisher_id);

    let media = &state.media;
    assert!(!media.contains_source(publisher_source_id));
    assert!(media.contains_source(other_source_id));
    assert!(!media.contains_consumer(&removed_consumer_key));
    assert!(!media.contains_pending_consumer_bootstrap(&pending_removed_key));
    assert!(!media.contains_consumer_source_selection(&removed_consumer_key));
    assert!(media.contains_consumer(&surviving_consumer_key));
    assert!(media.contains_consumer_source_selection(&surviving_consumer_key));
    assert_eq!(
        media.consumer_keys_for_user(&subscriber_id),
        vec![surviving_consumer_key.clone()]
    );
    assert_eq!(
        media.consumer_keys_for_source(other_source_id),
        vec![surviving_consumer_key]
    );
    assert_eq!(
        media.producer_ids_for_user(&other_publisher_id),
        vec![other_producer_id]
    );
    assert!(media.owner_producer_index_is_empty(&publisher_id));
    assert!(media.owner_source_index_is_empty(&publisher_id));
}
