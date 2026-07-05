#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{collections::BTreeSet, sync::Arc};

use o_sfu_router::{
    MediaKind as RouterMediaKind, RouterId,
    ids::{ConsumerId, ProducerId},
    negotiation::derive_consumable_rtp_parameters,
    rtp::{MediaStream, Mid, Rid, Ssrc},
    state::{ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState},
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
        sample_video_rtp_parameters,
    },
    topology::{RoutedConsumerId, RoutedProducerId},
};

use super::{
    ConsumerKey, ConsumerRouteState, ConsumerRouteTarget, ConsumerRouteTransportRef,
    ConsumerSetupOutcome, ConsumerState, DeclaredConsumerSetup, PendingConsumerSetup,
    ProducerRuntimeId, PublishedProducer, PublishedSourceInstall,
};
use crate::{
    Bitrate, MediaCodecFlags, RoomMediaLimits,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, TestSourceKind, UserId, UserPermissions,
        media_transport::{SessionUploadEncoding, TransportConsumerRoute, TransportMediaId},
        metrics::RuntimeMetrics,
        room::{
            RoomAdmissionPolicy, RoomRuntimeContext, RouterPlacement, UserOutboundSender,
            rtp_capabilities::router_rtp_capabilities, state::RoomState,
        },
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceDescriptor,
            PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceSelector, UploadLayerPolicyRole, UserStreamId,
            test_support::{
                TestSubscriptionStates, source_kind_for_stream_id,
                source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
    },
};

impl PendingConsumerSetup {
    pub(crate) fn declared(
        self,
        media: TransportMediaId,
        mid: Option<String>,
    ) -> DeclaredConsumerSetup {
        let route = self.target.transport_consumer_route(media);
        DeclaredConsumerSetup {
            pending: self,
            route,
            mid,
        }
    }
}

impl ConsumerRouteTarget {
    pub(in crate::engine::room) fn for_test(
        transport_ref: ConsumerRouteTransportRef,
        transport_route: TransportConsumerRoute,
        stream_id: UserStreamId,
        kind: RouterMediaKind,
    ) -> Self {
        Self::new(transport_ref, transport_route, stream_id, kind)
    }
}

fn test_state() -> RoomState {
    test_state_with_media_limits(RoomMediaLimits::default())
}

fn test_state_with_media_limits(media_limits: RoomMediaLimits) -> RoomState {
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
        media_limits,
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

fn set_test_user_ready(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("user should have a connection id");
    assert!(
        state
            .set_user_negotiated(user_id, connection_id, sample_client_rtp_capabilities())
            .is_some()
    );
    connection_id
}

fn scalable_video_states(active: bool) -> TestSubscriptionStates {
    TestSubscriptionStates {
        scalable_video: Some(active),
        audio_detector: None,
        readable_video: None,
        ..TestSubscriptionStates::default()
    }
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
        .routing_mut_for_test()
        .add_producer(producer_user_id, RouterMediaKind::Video)
        .unwrap_or_else(|error| panic!("failed to create test producer route: {error:?}"));
    let routed_consumer_id = state
        .topology
        .routing_mut_for_test()
        .add_consumer_with_route_state(
            consumer_user_id,
            routed_producer_id,
            ConsumerCapability::Compatible,
            RouterConsumerRouteState::Active,
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
    let consumer = ConsumerState {
        routed_consumer_id,
        consumer_connection_id,
        source_connection_id: producer_connection_id,
        source_media: TransportMediaId::new(1),
        consumer_media: TransportMediaId::new(2),
        consumer_mid: "camera-down".to_owned(),
    };
    assert!(state.topology.commit_consumer_route_for_test(
        route_key.clone(),
        consumer,
        ConsumerSourceSelection::open(true),
    ));
    (route_key, consumer_connection_id)
}

fn test_source_descriptor(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
) -> PublishedSourceDescriptor {
    let source_id = PublishedSourceId::allocate(&mut state.next_source_id);
    let encoding_id = SourceEncodingId::allocate(&mut state.next_source_encoding_id);
    let intent = source_publish_intent_for_source(stream_type);
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
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
    .expect("test source graph should be valid")
}

fn install_test_published_producer_with_route(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    routed_producer_id: RoutedProducerId,
    consumable_rtp_parameters: MediaStream,
    transport_media_id: TransportMediaId,
) -> (ProducerRuntimeId, PublishedSourceId) {
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let source_descriptor = test_source_descriptor(state, user_id, stream_type);
    let source_id = source_descriptor.source_id();
    state
        .topology
        .install_source_for_test(PublishedSourceInstall {
            source_descriptor,
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
        .routing_mut_for_test()
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
        .routing_mut_for_test()
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
    assert!(state.topology.commit_consumer_route_for_test(
        key.clone(),
        ConsumerState {
            routed_consumer_id: RoutedConsumerId::for_test(
                RouterId(1),
                ConsumerId(consumer_media.as_u64())
            ),
            consumer_connection_id,
            source_connection_id,
            source_media,
            consumer_media,
            consumer_mid: format!("mid-{}", consumer_media.as_u64()),
        },
        ConsumerSourceSelection::open(true),
    ));
    key
}

fn set_test_consumer_policy_pause(state: &mut RoomState, key: &ConsumerKey) {
    let route_ref = state
        .topology
        .committed_consumer_route_for_key(key)
        .expect("consumer route should exist")
        .transport_ref();
    assert!(state.topology.update_consumer_source_selection(
        &route_ref,
        key.source_id,
        |selection| {
            selection.set_policy_pause_reason(Some(PolicyPauseReason::VideoDownloadLimit));
        }
    ));
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

fn pending_consumer_setup() -> (
    RoomState,
    UserId,
    UserId,
    ConnectionId,
    UserStreamId,
    PendingConsumerSetup,
) {
    let mut state = test_state();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let subscriber_connection_id = set_test_user_ready(&mut state, &subscriber_user_id);
    install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );
    let mut planned_setups = state
        .plan_missing_consumers(&subscriber_user_id, subscriber_connection_id)
        .expect("subscriber session should exist");
    assert_eq!(planned_setups.len(), 1);
    let setup = planned_setups.pop().expect("setup should be planned");

    (
        state,
        publisher_user_id,
        subscriber_user_id,
        subscriber_connection_id,
        stream_id,
        setup,
    )
}

fn commit_setup(state: &mut RoomState, setup: PendingConsumerSetup) -> ConsumerSetupOutcome {
    let setup = setup.declared(TransportMediaId::new(20), Some(String::from("m0")));
    let (_, _, outcome) = state.commit_declared_consumer_setup(setup);
    outcome
}

#[test]
fn policy_paused_routes_do_not_count_as_effective_delivery() {
    let (mut state, producer_user_id, consumer_user_id, key, consumer_connection_id) =
        two_user_consumer_route();
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    assert!(state.source_fanout_pressure(1));
    assert_eq!(
        state.consumer_route_state(&consumer_user_id, &producer_user_id, &stream_id),
        Some(ConsumerRouteState::Active)
    );
    assert_eq!(
        state
            .active_video_keyframe_targets(&consumer_user_id, consumer_connection_id,)
            .expect("consumer should exist")
            .len(),
        1
    );

    set_test_consumer_policy_pause(&mut state, &key);

    assert!(!state.source_fanout_pressure(1));
    assert_eq!(
        state.consumer_route_state(&consumer_user_id, &producer_user_id, &stream_id),
        Some(ConsumerRouteState::Inactive)
    );
    assert!(
        state
            .active_video_keyframe_targets(&consumer_user_id, consumer_connection_id,)
            .expect("consumer should exist")
            .is_empty()
    );
}

#[test]
fn producer_activity_does_not_flip_room_state_when_router_update_fails() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);
    let connection_id = join_test_user(&mut state, &user_id);

    let (producer_id, _source_id) = install_test_published_producer_with_route(
        &mut state,
        &user_id,
        connection_id,
        TestSourceKind::ScalableVideo,
        RoutedProducerId::for_test(RouterId(1), ProducerId(777)),
        sample_video_rtp_parameters(None, 77_777),
        TransportMediaId::default(),
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
            .topology
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

    join_test_user(&mut state, &producer_user_id);
    join_test_user(&mut state, &consumer_user_id);
    let (route_key, stale_connection_id) =
        install_test_consumer_route(&mut state, &producer_user_id, &consumer_user_id);

    join_test_user(&mut state, &consumer_user_id);

    let intents = subscription_intents_from_test_states(&scalable_video_states(false));
    let change = state.plan_receiver_route_work(
        &consumer_user_id,
        stale_connection_id,
        &producer_user_id,
        &intents,
    );
    assert!(!change.route_graph_changed());
    assert!(
        state.desired_source_active(
            &consumer_user_id,
            &producer_user_id,
            &stream_id_for_source(TestSourceKind::ScalableVideo),
        ),
        "stale subscription updates must not overwrite the replacement user's stored preferences"
    );
    assert!(
        !state.topology.has_consumer_setup_or_route(&route_key),
        "replacement join should clear stale consumer routes before the new connection reboots them"
    );
}

#[test]
fn subscription_change_reserves_missing_setup_for_existing_publisher() {
    let mut state = test_state();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let subscriber_connection_id = set_test_user_ready(&mut state, &subscriber_user_id);
    let (_, source_id) = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );

    let intents = subscription_intents_from_test_states(&scalable_video_states(false));
    let change = state.plan_receiver_route_work(
        &subscriber_user_id,
        subscriber_connection_id,
        &publisher_user_id,
        &intents,
    );
    assert!(change.activities.is_empty());
    assert!(change.relays.is_empty());
    assert_eq!(change.setups.len(), 1);
    let selection = state
        .topology
        .consumer_source_selection(&ConsumerKey::new(&subscriber_user_id, source_id))
        .expect("compat subscription should create a source-level selection");
    assert!(!selection.active());
    assert_eq!(
        selection.selector(),
        SourceSelector::Open,
        "compat downloads default to an unconstrained source selector"
    );
    assert_eq!(state.media_counts().subscriptions, 1);
}

#[test]
fn consumer_setup_commit_uses_latest_room_state() {
    let (
        mut state,
        publisher_user_id,
        subscriber_user_id,
        subscriber_connection_id,
        stream_id,
        setup,
    ) = pending_consumer_setup();

    let publisher_connection_id = state
        .user_connection_id(&publisher_user_id)
        .expect("publisher should have a connection id");
    let producer_target = state
        .producer_route_target(&publisher_user_id, publisher_connection_id, &stream_id)
        .expect("producer route target should exist");
    assert!(
        state
            .apply_producer_activity(&publisher_user_id, &producer_target, &stream_id, false)
            .is_some()
    );

    let intents = subscription_intents_from_test_states(&scalable_video_states(false));
    let change = state.plan_receiver_route_work(
        &subscriber_user_id,
        subscriber_connection_id,
        &publisher_user_id,
        &intents,
    );
    assert!(change.setups.is_empty());

    let ConsumerSetupOutcome::Committed {
        snapshot,
        transport_activity_update,
        ..
    } = commit_setup(&mut state, setup)
    else {
        panic!("current room state should still accept the setup");
    };
    assert!(
        snapshot
            .sources
            .iter()
            .any(|source| !source.producer_active)
    );
    let transport_activity_update =
        transport_activity_update.expect("transport declaration should be corrected");
    assert!(!transport_activity_update);
    assert_eq!(
        state.consumer_route_state(&subscriber_user_id, &publisher_user_id, &stream_id),
        Some(ConsumerRouteState::Inactive)
    );
}

#[test]
fn consumer_setup_commit_releases_stale_receiver_plan() {
    let (
        mut state,
        _publisher_user_id,
        subscriber_user_id,
        _subscriber_connection_id,
        _stream_id,
        setup,
    ) = pending_consumer_setup();
    assert_eq!(state.media_counts().subscriptions, 1);

    state
        .users
        .get_mut(&subscriber_user_id)
        .expect("subscriber should exist")
        .connection_id = ConnectionId::from_raw(900);
    let ConsumerSetupOutcome::Released(..) = commit_setup(&mut state, setup) else {
        panic!("stale receiver setup should be released");
    };

    assert_eq!(state.media_counts().subscriptions, 0);
}

#[test]
fn consumer_setup_commit_releases_stale_producer_plan() {
    let (
        mut state,
        publisher_user_id,
        _subscriber_user_id,
        _subscriber_connection_id,
        stream_id,
        setup,
    ) = pending_consumer_setup();
    assert_eq!(state.media_counts().subscriptions, 1);

    let source_id = state
        .topology
        .source_id_for_owner_stream(&publisher_user_id, &stream_id)
        .expect("publisher source should exist");
    assert!(state.topology.remove_source_for_test(source_id));
    let ConsumerSetupOutcome::Released(..) = commit_setup(&mut state, setup) else {
        panic!("stale producer setup should be released");
    };

    assert_eq!(state.media_counts().subscriptions, 0);
}

#[test]
fn consumer_setup_commit_rolls_back_routed_consumer_after_route_graph_rejection() {
    let (
        mut state,
        publisher_user_id,
        subscriber_user_id,
        _subscriber_connection_id,
        stream_id,
        setup,
    ) = pending_consumer_setup();
    assert_eq!(state.media_counts().subscriptions, 1);

    let source_id = state
        .topology
        .source_id_for_owner_stream(&publisher_user_id, &stream_id)
        .expect("publisher source should exist");
    let key = ConsumerKey::new(&subscriber_user_id, source_id);
    assert!(state.topology.has_consumer_setup_or_route(&key));
    state.topology.remove_route_graph_entry_for_test(&key);
    assert!(!state.topology.has_consumer_setup_or_route(&key));

    let ConsumerSetupOutcome::Released(..) = commit_setup(&mut state, setup) else {
        panic!("setup with rejected route graph commit should be released");
    };

    assert_eq!(state.consumer_count(), 0);
    assert_eq!(state.media_counts().subscriptions, 0);
    assert!(
        state
            .topology
            .routing_mut_for_test()
            .remove_consumer(RoutedConsumerId::for_test(RouterId(1), ConsumerId(1)))
            .is_err(),
        "routed consumer created before route graph rejection must be rolled back"
    );
}

#[test]
fn missing_consumer_setup_applies_video_download_cap_before_effects() {
    let mut state = test_state_with_media_limits(RoomMediaLimits::try_new(4, 1).unwrap());
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let subscriber_connection_id = set_test_user_ready(&mut state, &subscriber_user_id);
    let (_, scalable_source_id) = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );
    let (_, readable_source_id) = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ReadableVideo,
        TransportMediaId::new(11),
        33_333,
    );
    for source_id in [scalable_source_id, readable_source_id] {
        state.topology.ensure_selection_for_test(
            &ConsumerKey::new(&subscriber_user_id, source_id),
            ConsumerSourceSelection::open(true),
        );
    }

    let planned_setups = state
        .plan_missing_consumers(&subscriber_user_id, subscriber_connection_id)
        .expect("subscriber session should still exist");

    assert_eq!(planned_setups.len(), 2);
    let scalable_selection = state
        .topology
        .consumer_source_selection(&ConsumerKey::new(&subscriber_user_id, scalable_source_id))
        .expect("scalable source should have a consumer selection");
    let readable_selection = state
        .topology
        .consumer_source_selection(&ConsumerKey::new(&subscriber_user_id, readable_source_id))
        .expect("readable source should have a consumer selection");
    assert!(scalable_selection.active());
    assert!(readable_selection.active());
    assert!(!scalable_selection.delivery_active());
    assert!(readable_selection.delivery_active());
    assert_eq!(
        scalable_selection.policy_pause_reason(),
        Some(PolicyPauseReason::VideoDownloadLimit)
    );
    assert_eq!(readable_selection.policy_pause_reason(), None);
}

#[test]
fn commit_publish_reservation_registers_all_source_encodings() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);

    join_test_user(&mut state, &user_id);
    let connection_id = set_test_user_ready(&mut state, &user_id);

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_simulcast_video_rtp_parameters(Some("camera-0")),
        &state.router_rtp_capabilities(),
    )
    .expect("simulcast RTP parameters should derive consumable router parameters");
    let validated_publish = state
        .validate_publish(
            &user_id,
            connection_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("publish descriptor should validate once the user is publish-ready");
    let transport_media_id = TransportMediaId::new(101);
    let upload_encodings = test_upload_encodings();

    assert!(
        state
            .commit_publish_reservation(
                validated_publish,
                consumable_rtp_parameters,
                &upload_encodings,
                transport_media_id,
            )
            .is_some()
    );

    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to the committed source");
    let source = state
        .topology
        .source(source_id)
        .expect("source registry should own the committed source");
    assert_eq!(source.owner().user_id(), &user_id);
    assert_eq!(
        source_kind_for_stream_id(source.stream_id()),
        Some(TestSourceKind::ScalableVideo)
    );
    assert_eq!(source.mid().map(Mid::as_str), Some("camera-0"));
    let encodings = source.encodings().collect::<Vec<_>>();
    assert_eq!(encodings.len(), 2);
    assert_eq!(encodings[0].rid().map(Rid::as_str), Some("lo"));
    assert_eq!(encodings[1].rid().map(Rid::as_str), Some("hi"));
    assert_eq!(encodings[0].primary_ssrc(), Some(Ssrc::new(31_001)));
    assert_eq!(encodings[1].primary_ssrc(), Some(Ssrc::new(31_002)));
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
fn transport_removals_for_departing_users_deduplicate_overlapping_consumer_routes() {
    let mut state = test_state();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let source_media = TransportMediaId::new(10);
    let consumer_media = TransportMediaId::new(20);

    join_test_user(&mut state, &publisher_id);
    join_test_user(&mut state, &subscriber_id);

    let publisher_connection_id = state
        .user_connection_id(&publisher_id)
        .expect("publisher should have a connection id");
    let (_, source_id) = install_test_published_producer(
        &mut state,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        source_media,
    );
    install_test_consumer_state(
        &mut state,
        &subscriber_id,
        source_id,
        publisher_connection_id,
        source_media,
        consumer_media,
    );

    let transport_removals = state
        .topology
        .transport_removals_for_users_for_test(&BTreeSet::from([
            publisher_id.clone(),
            subscriber_id.clone(),
        ]));

    let mut removed_media = transport_removals
        .iter()
        .map(|removal| (removal.user.clone(), removal.transport_media))
        .collect::<Vec<_>>();
    removed_media.sort_unstable();
    assert_eq!(
        removed_media,
        vec![
            (publisher_id, source_media),
            (subscriber_id, consumer_media)
        ]
    );
}
