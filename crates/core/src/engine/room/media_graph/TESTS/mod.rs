#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{mem, sync::Arc};

use o_sfu_router::{
    MediaKind as RouterMediaKind, RouterId,
    negotiation::derive_consumable_rtp_parameters,
    rtp::{MediaStream, Mid, Rid, Ssrc},
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
        sample_video_rtp_parameters,
    },
};

use super::{
    ConsumerKey, ConsumerRouteState, ConsumerRouteTarget, ConsumerRouteTransportRef,
    ConsumerSetupOrigin, ConsumerSetupOutcome, DeclaredConsumerSetup, PendingConsumerSetup,
    ProducerId, ValidatedPublish,
};
use crate::{
    Bitrate, MediaCodecFlags, RoomMediaLimits,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, TestSourceKind, UserId, UserPermissions,
        media_transport::{SessionUploadEncoding, TransportConsumerRoute, TransportMediaId},
        metrics::RuntimeMetrics,
        room::{
            RoomAdmissionPolicy, RoomRuntimeContext, RouterPlacement, UserOutboundSender,
            cleanup::TransportCleanupOperation, rtp_capabilities::router_rtp_capabilities,
            state::RoomState,
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
    let consumer_connection_id = set_test_user_ready(state, consumer_user_id);
    let source_id = install_test_published_producer(
        state,
        producer_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(1),
    );
    let route_key = ConsumerKey::new(consumer_user_id, source_id);
    let mut setups = state
        .plan_missing_consumers(consumer_user_id, consumer_connection_id)
        .expect("consumer session should exist");
    assert_eq!(setups.len(), 1);
    let setup = setups.pop().expect("consumer setup should be planned");
    assert!(matches!(
        commit_setup_with_media(
            state,
            setup,
            TransportMediaId::new(2),
            Some(String::from("camera-down")),
        ),
        ConsumerSetupOutcome::Committed { .. }
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

fn install_test_published_producer_with_rtp(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    consumable_rtp_parameters: MediaStream,
    transport_media_id: TransportMediaId,
) -> PublishedSourceId {
    let producer_id = ProducerId::allocate(&mut state.next_producer_id);
    let source_descriptor = test_source_descriptor(state, user_id, stream_type);
    let source_id = source_descriptor.source_id();
    state
        .topology
        .publish_source(
            ValidatedPublish {
                owner_user_id: user_id.clone(),
                owner_connection_id: connection_id,
                session_key: state.transport_user_key(user_id, connection_id),
                stream_id: source_descriptor.stream_id().clone(),
                media_kind: source_descriptor.media_kind(),
                policy: source_descriptor.policy(),
                presence: None,
            },
            producer_id,
            source_descriptor,
            consumable_rtp_parameters,
            transport_media_id,
        )
        .expect("test producer route should be published");
    source_id
}

fn install_test_published_producer(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
    transport_media_id: TransportMediaId,
) -> PublishedSourceId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("publisher user should have a connection id");
    install_test_published_producer_with_rtp(
        state,
        user_id,
        connection_id,
        stream_type,
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
) -> PublishedSourceId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("publisher should have a connection id");
    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, ssrc),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    install_test_published_producer_with_rtp(
        state,
        user_id,
        connection_id,
        stream_type,
        consumable_rtp_parameters,
        transport_media_id,
    )
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

fn commit_setup_with_media(
    state: &mut RoomState,
    setup: PendingConsumerSetup,
    media: TransportMediaId,
    mid: Option<String>,
) -> ConsumerSetupOutcome {
    let setup = setup.declared(media, mid);
    let (_, _, outcome) =
        state.commit_declared_consumer_setup(setup, ConsumerSetupOrigin::Subscribe);
    outcome
}

fn commit_setup(state: &mut RoomState, setup: PendingConsumerSetup) -> ConsumerSetupOutcome {
    commit_setup_with_media(
        state,
        setup,
        TransportMediaId::new(20),
        Some(String::from("m0")),
    )
}

#[test]
fn consumer_setup_mid_uses_transport_then_negotiated_then_id() {
    for (negotiated, transport, expected) in [
        (None, None, "consumer-1"),
        (Some("negotiated-mid"), None, "negotiated-mid"),
        (
            Some("negotiated-mid"),
            Some(String::from("transport-mid")),
            "transport-mid",
        ),
    ] {
        let (mut state, publisher, subscriber, _, stream, mut setup) = pending_consumer_setup();
        if let Some(mid) = negotiated {
            setup.rtp = mem::take(&mut setup.rtp).with_mid(mid);
        }
        assert!(matches!(
            commit_setup_with_media(&mut state, setup, TransportMediaId::new(20), transport),
            ConsumerSetupOutcome::Committed { .. }
        ));
        let source = state
            .topology
            .source_id_for_owner_stream(&publisher, &stream)
            .expect("published source should exist");
        let key = ConsumerKey::new(&subscriber, source);

        assert_eq!(
            state
                .topology
                .committed_consumer_route_for_key(&key)
                .map(|route| route.state.consumer_mid.as_str()),
            Some(expected)
        );
    }
}

#[test]
fn policy_paused_routes_do_not_count_as_effective_delivery() {
    let mut state = test_state();
    let producer_user_id = UserId::Integer(1);
    let consumer_user_id = UserId::Integer(2);
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    join_test_user(&mut state, &producer_user_id);
    join_test_user(&mut state, &consumer_user_id);
    let (key, _) = install_test_consumer_route(&mut state, &producer_user_id, &consumer_user_id);

    assert!(state.source_fanout_pressure(1));
    assert_eq!(
        state.consumer_route_state(&consumer_user_id, &producer_user_id, &stream_id),
        Some(ConsumerRouteState::Active)
    );

    set_test_consumer_policy_pause(&mut state, &key);

    assert!(!state.source_fanout_pressure(1));
    assert_eq!(
        state.consumer_route_state(&consumer_user_id, &producer_user_id, &stream_id),
        Some(ConsumerRouteState::Inactive)
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
    let source_id = install_test_consumable_video_producer(
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
            .apply_producer_activity(&publisher_user_id, &producer_target, false)
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
fn consumer_setup_releases_route_graph_rejection() {
    let (
        mut state,
        publisher_user_id,
        subscriber_user_id,
        subscriber_connection_id,
        stream_id,
        setup,
    ) = pending_consumer_setup();
    let consumer = setup.consumer;
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

    let mut retries = state
        .plan_missing_consumers(&subscriber_user_id, subscriber_connection_id)
        .expect("subscriber session should exist");
    assert_eq!(retries.len(), 1);
    let retry = retries.pop().expect("consumer setup should be retried");
    let routed = state
        .topology
        .add_consumer_route(&retry.target, consumer)
        .expect("router consumer from rejected commit should be rolled back");
    state
        .topology
        .rollback_consumer_route(&retry.target, routed);
    state.release_pending_consumer_setup(retry);
    assert_eq!(state.consumer_count(), 0);
}

#[test]
fn missing_consumer_setup_applies_video_download_cap_before_effects() {
    let mut state = test_state_with_media_limits(RoomMediaLimits::try_new(4, 1).unwrap());
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let subscriber_connection_id = set_test_user_ready(&mut state, &subscriber_user_id);
    let scalable_source_id = install_test_consumable_video_producer(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );
    let readable_source_id = install_test_consumable_video_producer(
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

    let subscriber_connection_id = set_test_user_ready(&mut state, &subscriber_id);
    install_test_published_producer(
        &mut state,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        source_media,
    );
    let mut setups = state
        .plan_missing_consumers(&subscriber_id, subscriber_connection_id)
        .expect("subscriber should still be current");
    assert_eq!(setups.len(), 1);
    let setup = setups.pop().expect("consumer setup should be planned");
    assert!(matches!(
        commit_setup_with_media(
            &mut state,
            setup,
            consumer_media,
            Some(String::from("camera-down")),
        ),
        ConsumerSetupOutcome::Committed { .. }
    ));

    let outcome = state.apply_disconnect_users(&[publisher_id.clone(), subscriber_id.clone()]);
    let (_, cleanup) = outcome.transport_plan.relays_and_cleanup();

    let mut removed_media = cleanup
        .iter()
        .filter_map(|operation| match operation {
            TransportCleanupOperation::RemoveMedia {
                session_key,
                transport_media_id,
            } => Some((session_key.user_id(), *transport_media_id)),
            TransportCleanupOperation::CloseUser { .. }
            | TransportCleanupOperation::ReleaseRelayRoute { .. } => None,
        })
        .collect::<Vec<_>>();
    removed_media.sort_unstable();
    assert_eq!(
        removed_media,
        vec![
            (&publisher_id, source_media),
            (&subscriber_id, consumer_media)
        ]
    );
}
