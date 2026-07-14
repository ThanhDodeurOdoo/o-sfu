#![allow(
    clippy::expect_used,
    reason = "transition tests fail loudly when fixed room setup is invalid"
)]

use std::{collections::BTreeMap, sync::Arc};

use o_sfu_router::test_support::rtp_samples::{
    sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
};
use o_sfu_telemetry::schema::event as telemetry_event;

use super::super::super::{
    Room, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerConfig, RoomRuntimePolicy,
    UserOutbound, UserOutboundReceiver, UserOutboundSender, media_graph::ConsumerRouteState,
    transition::PublishStageOutcome,
};
use crate::{
    MediaCodecFlags, RoomWorkerPolicy, RuntimeFeatureFlags,
    engine::{
        ConnectionId, TestSourceKind, UserId, UserPermissions,
        media_transport::{
            AppliedSessionAnswer, MediaTransport, TransportMediaId, TransportRelayRouteAction,
            TransportTeardown,
            test_support::{test_media_transport, test_rtc_port_range},
        },
        metrics::RuntimeMetrics,
        source_model::{
            SourceSubscriptionIntent, UserStreamId,
            test_support::{source_publish_intent_for_source, stream_id_for_source},
        },
    },
};

fn media_transport() -> MediaTransport {
    let rtc_port_range = test_rtc_port_range(4).expect("test ports should be available");
    test_media_transport(4, rtc_port_range).expect("test media transport config should be valid")
}

fn test_sender() -> UserOutboundSender {
    test_outbound().0
}

fn test_outbound() -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default()))
}

fn pause_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    BTreeMap::from([(
        stream_id_for_source(TestSourceKind::ScalableVideo),
        SourceSubscriptionIntent::new(Some(false), None),
    )])
}

fn active_scalable_video_intents() -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    BTreeMap::from([(
        stream_id_for_source(TestSourceKind::ScalableVideo),
        SourceSubscriptionIntent::new(Some(true), None),
    )])
}

async fn join_negotiated_user(
    room: &Arc<Room>,
    media_transport: &MediaTransport,
    user_id: &UserId,
    create_transport_session: bool,
) -> ConnectionId {
    join_negotiated_user_with_sender(
        room,
        media_transport,
        user_id,
        create_transport_session,
        test_sender(),
    )
    .await
}

async fn join_negotiated_user_with_sender(
    room: &Arc<Room>,
    media_transport: &MediaTransport,
    user_id: &UserId,
    create_transport_session: bool,
    sender: UserOutboundSender,
) -> ConnectionId {
    let connection_id = room
        .test_api()
        .lifecycle()
        .join_user(user_id.clone(), None, UserPermissions::default(), sender)
        .await
        .expect("test user should join");
    if create_transport_session {
        let session_key = room.transport_user_key(user_id, connection_id).await;
        media_transport
            .create_initial_session_offer(&session_key)
            .await
            .expect("test session should create an initial offer");
    }
    assert_eq!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            sample_client_rtp_capabilities(),
            media_transport,
        )
        .await,
        Some(())
    );
    connection_id
}

fn drain_setup_track(rx: &mut UserOutboundReceiver) -> bool {
    let mut found = false;
    while let Ok(message) = rx.try_recv() {
        found |= matches!(message, UserOutbound::RemoteSources(_));
    }
    found
}

async fn setup_subscription_room(
    create_subscriber_transport_session: bool,
) -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    setup_subscription_room_with_manager(
        RoomManager::for_test(),
        "issuer-transition-subscription",
        create_subscriber_transport_session,
    )
    .await
}

async fn setup_spillover_subscription_room() -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    let setup = setup_subscription_room_with_manager(
        RoomManager::for_test_with_config(RoomManagerConfig::new(
            2,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(100),
                RuntimeFeatureFlags::default(),
                super::super::super::rtp_capabilities::router_rtp_capabilities(
                    MediaCodecFlags::default(),
                ),
            )
            .with_room_worker_policy(RoomWorkerPolicy::bounded_local_spillover(2)),
        )),
        "issuer-transition-subscription-spillover",
        true,
    )
    .await;
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup;
    assert_ne!(
        room.transport_user_key(&publisher_id, publisher_connection_id)
            .await
            .media_worker_id(),
        room.transport_user_key(&subscriber_id, subscriber_connection_id)
            .await
            .media_worker_id()
    );
    (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    )
}

async fn setup_subscription_room_with_manager(
    manager: RoomManager,
    issuer: &str,
    create_subscriber_transport_session: bool,
) -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    setup_subscription_room_with_sender(
        manager,
        issuer,
        create_subscriber_transport_session,
        test_sender(),
    )
    .await
}

async fn setup_subscription_room_with_sender(
    manager: RoomManager,
    issuer: &str,
    create_subscriber_transport_session: bool,
    subscriber_sender: UserOutboundSender,
) -> (
    Arc<Room>,
    MediaTransport,
    UserId,
    ConnectionId,
    UserId,
    ConnectionId,
) {
    let room = manager
        .serve_room(issuer, "room", &RoomConfig::default(), None)
        .await;
    let media_transport = media_transport();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let publisher_connection_id =
        join_negotiated_user(&room, &media_transport, &publisher_id, true).await;
    let subscriber_connection_id = join_negotiated_user_with_sender(
        &room,
        &media_transport,
        &subscriber_id,
        create_subscriber_transport_session,
        subscriber_sender,
    )
    .await;
    (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    )
}

async fn publish_scalable_video(
    room: &Room,
    media_transport: &MediaTransport,
    publisher_id: &UserId,
    publisher_connection_id: ConnectionId,
) -> TransportMediaId {
    let transport_media_id =
        stage_scalable_video(room, media_transport, publisher_id, publisher_connection_id).await;
    commit_scalable_video(
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        transport_media_id,
    )
    .await;
    transport_media_id
}

async fn stage_scalable_video(
    room: &Room,
    media_transport: &MediaTransport,
    publisher_id: &UserId,
    publisher_connection_id: ConnectionId,
) -> TransportMediaId {
    assert_eq!(
        room.user_operation(publisher_id, publisher_connection_id, media_transport)
            .stage_negotiated_publish(&source_publish_intent_for_source(
                TestSourceKind::ScalableVideo,
            ))
            .await
            .expect("stage publish should not fail"),
        PublishStageOutcome::Staged
    );
    room.staged_media_id(
        publisher_id,
        publisher_connection_id,
        TestSourceKind::ScalableVideo,
    )
    .await
    .expect("test publish should be staged")
}

async fn commit_scalable_video(
    room: &Room,
    media_transport: &MediaTransport,
    publisher_id: &UserId,
    publisher_connection_id: ConnectionId,
    transport_media_id: TransportMediaId,
) {
    room.user_operation(publisher_id, publisher_connection_id, media_transport)
        .commit_staged_publishes(&AppliedSessionAnswer::from_negotiated_producers([(
            transport_media_id,
            sample_simulcast_video_rtp_parameters(None),
        )]))
        .await;
    assert_eq!(room.test_api().inspect().producer_count().await, 1);
}

async fn destination_state(
    media_transport: &MediaTransport,
    source_media_id: TransportMediaId,
    user_id: &UserId,
) -> Option<(TransportMediaId, bool)> {
    media_transport
        .test_api()
        .route_entry_by_media_id(source_media_id)
        .await?
        .destinations
        .into_iter()
        .find(|destination| destination.dest_session.user_id() == user_id)
        .map(|destination| (destination.dest_transport_media_id, destination.active))
}

async fn destination_active(
    media_transport: &MediaTransport,
    source_media_id: TransportMediaId,
    user_id: &UserId,
) -> Option<bool> {
    destination_state(media_transport, source_media_id, user_id)
        .await
        .map(|(_media_id, active)| active)
}

#[tokio::test]
async fn stored_receiver_intent_applies_to_future_consumer_setup() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_subscription_room(true).await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &pause_scalable_video_intents())
            .await,
        Some(())
    );
    publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert_eq!(
        room.test_api()
            .inspect()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Inactive)
    );
}

#[tokio::test]
async fn receiver_intent_updates_transport_route_activity() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_spillover_subscription_room().await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    let source_media_id = publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(
        destination_active(&media_transport, source_media_id, &subscriber_id).await,
        Some(true)
    );
    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &pause_scalable_video_intents())
            .await,
        Some(())
    );
    assert_eq!(
        room.test_api()
            .inspect()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Inactive)
    );
    let (consumer_media_id, active) =
        destination_state(&media_transport, source_media_id, &subscriber_id)
            .await
            .expect("subscriber route should still exist");
    assert!(!active);
    let subscriber_worker = room
        .transport_user_key(&subscriber_id, subscriber_connection_id)
        .await
        .media_worker_id()
        .as_usize();
    let events = room
        .diagnostics
        .user_recent_events(room.uuid(), &subscriber_id);
    let event = events
        .iter()
        .find(|event| {
            event.event == telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED
                && event.connection_id == Some(subscriber_connection_id.as_u64())
                && event.transport_media_id == Some(consumer_media_id.as_u64())
        })
        .expect("expected subscription activity diagnostics event");
    assert_eq!(event.media_worker_id, Some(subscriber_worker));
    assert_eq!(
        event.fields.get("active"),
        Some(&serde_json::Value::Bool(false))
    );

    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &active_scalable_video_intents())
            .await,
        Some(())
    );
    assert_eq!(
        room.test_api()
            .inspect()
            .consumer_route_state(&subscriber_id, &publisher_id, &stream_id)
            .await,
        Some(ConsumerRouteState::Active)
    );
    assert_eq!(
        destination_active(&media_transport, source_media_id, &subscriber_id).await,
        Some(true)
    );
}

#[tokio::test]
async fn transport_consume_failure_releases_pending_setup_for_retry() {
    let (subscriber_sender, mut subscriber_rx) = test_outbound();
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_subscription_room_with_sender(
        RoomManager::for_test(),
        "issuer-transition-subscription-outbound",
        false,
        subscriber_sender,
    )
    .await;
    assert!(!drain_setup_track(&mut subscriber_rx));
    let source_media_id = publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(
        room.test_api()
            .inspect()
            .consumer_route_state(
                &subscriber_id,
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            )
            .await,
        Some(ConsumerRouteState::Absent)
    );
    assert!(!drain_setup_track(&mut subscriber_rx));
    let subscriber_session_key = room
        .transport_user_key(&subscriber_id, subscriber_connection_id)
        .await;
    media_transport
        .create_initial_session_offer(&subscriber_session_key)
        .await
        .expect("retry session should create an initial offer");

    assert_eq!(
        room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
            .apply_session_refreshed()
            .await,
        Some(())
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert!(drain_setup_track(&mut subscriber_rx));
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
            .is_some_and(|entry| !entry.destinations.is_empty())
    );
}

#[tokio::test]
async fn relay_setup_failure_releases_pending_setup_for_retry() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        subscriber_connection_id,
    ) = setup_spillover_subscription_room().await;
    let source_media_id = stage_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;
    let publisher_session_key = room
        .transport_user_key(&publisher_id, publisher_connection_id)
        .await;
    media_transport
        .teardown([TransportTeardown::CloseSession {
            session_key: publisher_session_key,
        }])
        .await;

    commit_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
        source_media_id,
    )
    .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    let release_relays = {
        let mut state = room.state.write().await;
        let mut setups = state
            .refresh_consumer_readiness(&subscriber_id, subscriber_connection_id)
            .expect("subscriber session should still be current")
            .work
            .setups;
        let setup = setups.pop().expect("retry setup should be planned");
        assert!(setups.is_empty());
        let (_, _, relays) = state.release_pending_consumer_setup(setup);
        relays
    };
    assert_eq!(release_relays.len(), 1);
    assert_eq!(
        release_relays.first().map(|effect| effect.action),
        Some(TransportRelayRouteAction::Release)
    );
}

#[tokio::test]
async fn stale_receiver_subscription_update_is_rejected() {
    let (room, media_transport, publisher_id, _, subscriber_id, stale_connection_id) =
        setup_subscription_room(true).await;
    let _current_connection_id =
        join_negotiated_user(&room, &media_transport, &subscriber_id, true).await;

    assert_eq!(
        room.user_operation(&subscriber_id, stale_connection_id, &media_transport)
            .apply_receiver_intent(&publisher_id, &pause_scalable_video_intents())
            .await,
        None
    );
}

#[tokio::test]
async fn committed_consumer_reaches_graph_topology_and_transport() {
    let (
        room,
        media_transport,
        publisher_id,
        publisher_connection_id,
        subscriber_id,
        _subscriber_connection_id,
    ) = setup_subscription_room(true).await;
    let source_media_id = publish_scalable_video(
        &room,
        &media_transport,
        &publisher_id,
        publisher_connection_id,
    )
    .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert_eq!(
        room.test_api()
            .inspect()
            .consumer_route_state(
                &subscriber_id,
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            )
            .await,
        Some(ConsumerRouteState::Active)
    );
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await
            .is_some_and(|entry| !entry.destinations.is_empty())
    );
}
