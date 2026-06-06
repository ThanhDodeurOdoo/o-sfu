use std::collections::BTreeMap;

use super::super::{
    RoomUserOperation, SourcePolicyEvent,
    effects::{RoomCommit, RoomEffectContext},
    media_graph::ConsumerSetupOrigin,
};
use crate::{
    SubscriptionUpdateOutcome,
    engine::{
        UserId,
        source_model::{SourceSubscriptionIntent, UserStreamId},
    },
};

impl RoomUserOperation<'_> {
    pub(crate) async fn setup_missing_consumers(self) -> bool {
        let room = self.room();
        let mut state = room.state.write().await;
        let before = state.media_counts();
        let worker_lookup = state.worker_lookup();
        let Some(setups) =
            state.plan_missing_consumers(self.user_id(), self.connection_id(), worker_lookup)
        else {
            return false;
        };
        let after = state.media_counts();
        drop(state);
        RoomCommit::new()
            .with_media_count_delta(before, after)
            .with_consumer_setups(setups, ConsumerSetupOrigin::LateJoin)
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
            .execute(room, RoomEffectContext::runtime(self.media_transport()))
            .await;
        true
    }

    pub(crate) async fn update_subscription(
        self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> SubscriptionUpdateOutcome {
        let room = self.room();
        let effects = {
            let mut state = room.state.write().await;
            if state
                .user_for_connection(self.user_id(), self.connection_id())
                .is_none()
            {
                return SubscriptionUpdateOutcome::StaleConnection;
            }
            let before = state.media_counts();
            let worker_lookup = state.worker_lookup();
            let media_worker_id = state.media_worker_id_for_connection(self.connection_id());
            let change = state.plan_subscription_change(
                self.user_id(),
                self.connection_id(),
                target_user_id,
                intents,
                worker_lookup,
            );
            let source_policy_event = if change.touches_route_graph() {
                SourcePolicyEvent::RouteGraphChanged
            } else {
                SourcePolicyEvent::ReceiverIntentChanged
            };
            let after = state.media_counts();
            let (updates, setups, relays) = change.into_parts();
            let commit = RoomCommit::new()
                .with_media_count_delta(before, after)
                .with_relay_effects(relays)
                .with_route_updates(
                    &state,
                    room,
                    self.user_id(),
                    self.connection_id(),
                    media_worker_id,
                    updates,
                )
                .with_consumer_setups(setups, ConsumerSetupOrigin::Subscribe)
                .with_source_policy_event(source_policy_event);
            drop(state);
            commit
        };
        effects
            .execute(room, RoomEffectContext::runtime(self.media_transport()))
            .await;
        SubscriptionUpdateOutcome::Applied
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "transition tests fail loudly when fixed room setup is invalid"
    )]

    use std::{collections::BTreeMap, sync::Arc};

    use o_sfu_router::test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
    };

    use super::super::super::{
        Room, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerConfig, RoomRuntimePolicy,
        UserOutboundSender, media_graph::ConsumerRouteState,
    };
    use crate::{
        MediaCodecFlags, PublishStageOutcome, RoomWorkerPolicy, RuntimeFeatureFlags,
        SessionNegotiationOutcome, SubscriptionUpdateOutcome,
        engine::{
            ConnectionId, TestSourceKind, UserId, UserPermissions,
            media_transport::{
                AppliedSessionAnswer, MediaTransport, TransportMediaId,
                test_support::{test_media_transport_builder, test_rtc_port_range},
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
        test_media_transport_builder(rtc_port_range)
            .worker_count(4)
            .build()
            .expect("test media transport config should be valid")
    }

    fn test_sender() -> UserOutboundSender {
        UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default())).0
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
        let connection_id = room
            .test_api()
            .lifecycle()
            .join_user(
                user_id.clone(),
                None,
                UserPermissions::default(),
                test_sender(),
            )
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
            SessionNegotiationOutcome::Applied
        );
        connection_id
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
        let room = manager
            .serve_room(issuer, "room", &RoomConfig::default(), None)
            .await;
        let media_transport = media_transport();
        let publisher_id = UserId::Integer(1);
        let subscriber_id = UserId::Integer(2);
        let publisher_connection_id =
            join_negotiated_user(&room, &media_transport, &publisher_id, true).await;
        let subscriber_connection_id = join_negotiated_user(
            &room,
            &media_transport,
            &subscriber_id,
            create_subscriber_transport_session,
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
            stage_scalable_video(room, media_transport, publisher_id, publisher_connection_id)
                .await;
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
        let committed = room
            .user_operation(publisher_id, publisher_connection_id, media_transport)
            .commit_staged_publishes(&AppliedSessionAnswer::from_negotiated_producers([(
                transport_media_id,
                sample_simulcast_video_rtp_parameters(None),
            )]))
            .await;
        assert_eq!(
            committed,
            vec![stream_id_for_source(TestSourceKind::ScalableVideo)]
        );
    }

    async fn destination_active(
        media_transport: &MediaTransport,
        source_media_id: TransportMediaId,
        user_id: &UserId,
    ) -> Option<bool> {
        media_transport
            .test_api()
            .route_entry_by_media_id(source_media_id)
            .await?
            .destinations
            .into_iter()
            .find(|destination| destination.dest_session.user_id() == user_id)
            .map(|destination| destination.active)
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
                .update_subscription(&publisher_id, &pause_scalable_video_intents())
                .await,
            SubscriptionUpdateOutcome::Applied
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
            room.state
                .read()
                .await
                .consumer_route_state(&subscriber_id, &publisher_id, &stream_id),
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
        ) = setup_subscription_room(true).await;
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
                .update_subscription(&publisher_id, &pause_scalable_video_intents())
                .await,
            SubscriptionUpdateOutcome::Applied
        );
        assert_eq!(
            room.state
                .read()
                .await
                .consumer_route_state(&subscriber_id, &publisher_id, &stream_id,),
            Some(ConsumerRouteState::Inactive)
        );
        assert_eq!(
            destination_active(&media_transport, source_media_id, &subscriber_id).await,
            Some(false)
        );

        assert_eq!(
            room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
                .update_subscription(&publisher_id, &active_scalable_video_intents())
                .await,
            SubscriptionUpdateOutcome::Applied
        );
        assert_eq!(
            room.state
                .read()
                .await
                .consumer_route_state(&subscriber_id, &publisher_id, &stream_id,),
            Some(ConsumerRouteState::Active)
        );
        assert_eq!(
            destination_active(&media_transport, source_media_id, &subscriber_id).await,
            Some(true)
        );
    }

    #[tokio::test]
    async fn transport_consume_failure_releases_pending_setup_for_retry() {
        let (
            room,
            media_transport,
            publisher_id,
            publisher_connection_id,
            subscriber_id,
            subscriber_connection_id,
        ) = setup_subscription_room(false).await;
        let source_media_id = publish_scalable_video(
            &room,
            &media_transport,
            &publisher_id,
            publisher_connection_id,
        )
        .await;

        assert_eq!(room.test_api().inspect().consumer_count().await, 0);
        let subscriber_session_key = room
            .transport_user_key(&subscriber_id, subscriber_connection_id)
            .await;
        media_transport
            .create_initial_session_offer(&subscriber_session_key)
            .await
            .expect("retry session should create an initial offer");

        assert!(
            room.user_operation(&subscriber_id, subscriber_connection_id, &media_transport)
                .setup_missing_consumers()
                .await
        );
        assert_eq!(room.test_api().inspect().consumer_count().await, 1);
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
            .close_session(&publisher_session_key)
            .await
            .expect("source session should close before relay install");

        commit_scalable_video(
            &room,
            &media_transport,
            &publisher_id,
            publisher_connection_id,
            source_media_id,
        )
        .await;

        assert_eq!(room.test_api().inspect().consumer_count().await, 0);
        let retry_relays = {
            let mut state = room.state.write().await;
            let worker_lookup = state.worker_lookup();
            let mut setups = state
                .plan_missing_consumers(&subscriber_id, subscriber_connection_id, worker_lookup)
                .expect("subscriber session should still be current");
            assert_eq!(setups.len(), 1);
            let setup = setups.pop().expect("retry setup should be planned");
            state.release_consumer_setup_plan(setup)
        };
        assert_eq!(retry_relays.len(), 1);
    }

    #[tokio::test]
    async fn stale_receiver_subscription_update_is_rejected() {
        let (room, media_transport, publisher_id, _, subscriber_id, stale_connection_id) =
            setup_subscription_room(true).await;
        let _current_connection_id =
            join_negotiated_user(&room, &media_transport, &subscriber_id, true).await;

        assert_eq!(
            room.user_operation(&subscriber_id, stale_connection_id, &media_transport)
                .update_subscription(&publisher_id, &pause_scalable_video_intents())
                .await,
            SubscriptionUpdateOutcome::StaleConnection
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
            room.state.read().await.consumer_route_state(
                &subscriber_id,
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
            ),
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
}
