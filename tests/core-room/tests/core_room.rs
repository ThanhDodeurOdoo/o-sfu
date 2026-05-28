#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

use std::collections::BTreeMap;

use anyhow::Result;
use o_sfu_core::{
    prelude::{
        PublicationActivity, PublicationActivityOutcome, RoomWorkerPolicy, SfuCore,
        SourceSubscriptionIntent, SubscriptionUpdateOutcome, TransportEffectOutcome,
        UnpublishOutcome, UserStreamId,
    },
    server::{
        room::{
            JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerJoinError,
            test_support::{
                TestPlacementReason, TestSourceKind, TestSubscriptionStates, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
        session::{UserId, UserPermissions},
    },
};
mod support;

use support::{
    TEST_ROOM_KEY, close_user, home_worker, join_ready_users, join_user, load_triggered_policy,
    load_triggered_policy_with_cap, manager_with_policy, manager_with_policy_and_worker_count,
    media_transport, publish_audio_and_camera, publish_track, router_count,
    seed_source_fanout_pressure, serve_room, test_sender, test_video_rtp_parameters,
    user_connection_id,
};

type SubscriptionIntents = BTreeMap<UserStreamId, SourceSubscriptionIntent>;

fn pause_scalable_video_intents() -> SubscriptionIntents {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(false),
        ..TestSubscriptionStates::default()
    })
}

fn resume_scalable_video_intents() -> SubscriptionIntents {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        scalable_video: Some(true),
        ..TestSubscriptionStates::default()
    })
}

fn pause_audio_and_scalable_video_intents() -> SubscriptionIntents {
    subscription_intents_from_test_states(&TestSubscriptionStates {
        audio_detector: Some(false),
        scalable_video: Some(false),
        ..TestSubscriptionStates::default()
    })
}

#[tokio::test]
async fn room_manager_is_idempotent_by_issuer() {
    let manager = RoomManager::for_test();
    let config = RoomConfig::default();
    let first = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &config, None)
        .await;
    let second = manager
        .serve_room("issuer-a", "ignored", &config, None)
        .await;
    let third = manager
        .serve_room("issuer-b", TEST_ROOM_KEY, &config, None)
        .await;
    assert_eq!(first.uuid(), second.uuid());
    assert_ne!(first.uuid(), third.uuid());
}

#[tokio::test]
async fn room_manager_join_user_reports_missing_room() -> Result<()> {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = media_transport()?;
    let (sender, _receiver) = test_sender();
    let result = manager
        .join_user(
            "missing-room",
            JoinUserRequest {
                user_id: UserId::Integer(1),
                label: None,
                permissions: UserPermissions::default(),
                sender,
            },
            &media_transport,
        )
        .await;
    assert!(matches!(result, Err(RoomManagerJoinError::MissingRoom)));
    Ok(())
}

#[tokio::test]
async fn manager_leave_user_removes_empty_room() -> Result<()> {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-leave-empty").await;
    let room_id = room.uuid();
    let connection_id = join_user(&manager, &room, 1, &media_transport).await?;

    manager
        .close_session(
            room_id,
            &UserId::Integer(1),
            connection_id,
            &media_transport,
        )
        .await;

    assert!(manager.get_by_uuid(room_id).await.is_none());
    Ok(())
}

#[tokio::test]
async fn manager_disconnect_users_removes_empty_room() -> Result<()> {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-disconnect-empty").await;
    let room_id = room.uuid();
    join_user(&manager, &room, 1, &media_transport).await?;

    manager
        .disconnect_users(room_id, &[UserId::Integer(1)], &media_transport)
        .await;

    assert!(manager.get_by_uuid(room_id).await.is_none());
    Ok(())
}

#[tokio::test]
async fn load_triggered_join_keeps_small_rooms_on_primary_worker() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(4, 1, 1, 48)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-small").await;

    join_user(&manager, &room, 1, &media_transport).await?;
    join_user(&manager, &room, 2, &media_transport).await?;

    assert_eq!(home_worker(&room, 1).await, Some(0));
    assert_eq!(home_worker(&room, 2).await, Some(0));
    Ok(())
}

#[tokio::test]
async fn load_triggered_large_room_reaches_but_does_not_exceed_local_router_cap() -> Result<()> {
    const LOCAL_ROUTER_CAP: usize = 4;
    const MIN_RECEIVER_COUNT: usize = 3;
    const MAX_ACTIVE_CONSUMERS_PER_ROUTER: usize = 2;
    const ACTIVATION_WINDOW: usize = 1;
    const COOLDOWN_WINDOW: usize = 1;
    const MAX_FANOUT_PER_SOURCE: usize = 2;

    let manager = manager_with_policy_and_worker_count(
        load_triggered_policy_with_cap(
            LOCAL_ROUTER_CAP,
            MIN_RECEIVER_COUNT,
            MAX_ACTIVE_CONSUMERS_PER_ROUTER,
            ACTIVATION_WINDOW,
            COOLDOWN_WINDOW,
            MAX_FANOUT_PER_SOURCE,
        )?,
        LOCAL_ROUTER_CAP,
    );
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-large-room-cap").await;

    for user_id in 1..=12 {
        join_user(&manager, &room, user_id, &media_transport).await?;
        assert!(router_count(&room).await <= LOCAL_ROUTER_CAP);
    }

    assert_eq!(router_count(&room).await, LOCAL_ROUTER_CAP);
    for (user_id, media_worker) in [
        (1, 0),
        (2, 0),
        (3, 1),
        (4, 1),
        (5, 2),
        (6, 2),
        (7, 3),
        (8, 3),
    ] {
        assert_eq!(home_worker(&room, user_id).await, Some(media_worker));
    }
    assert_eq!(
        room.test_api()
            .inspect()
            .load_triggered_last_decision_reason(),
        Some(TestPlacementReason::LocalRouterCapReached)
    );
    Ok(())
}

#[tokio::test]
async fn bounded_spillover_still_detaches_idle_router_immediately() -> Result<()> {
    let manager = manager_with_policy(RoomWorkerPolicy::bounded_local_spillover(2));
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-bounded-detach").await;
    join_user(&manager, &room, 1, &media_transport).await?;
    let second_connection = join_user(&manager, &room, 2, &media_transport).await?;
    assert_eq!(router_count(&room).await, 2);

    close_user(&manager, &room, 2, second_connection, &media_transport).await?;

    assert_eq!(router_count(&room).await, 1);
    Ok(())
}

#[tokio::test]
async fn load_triggered_cooldown_delays_idle_spillover_detach() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(2, 1, 3, 48)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-cooldown").await;
    join_user(&manager, &room, 1, &media_transport).await?;
    let second_connection = join_user(&manager, &room, 2, &media_transport).await?;
    assert_eq!(router_count(&room).await, 2);

    close_user(&manager, &room, 2, second_connection, &media_transport).await?;
    assert_eq!(router_count(&room).await, 2);

    manager.drain_cleanup_retries(&media_transport).await;
    assert_eq!(router_count(&room).await, 2);
    manager.drain_cleanup_retries(&media_transport).await;
    assert_eq!(router_count(&room).await, 1);
    Ok(())
}

#[tokio::test]
async fn load_triggered_activity_resets_spillover_cooldown() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(2, 1, 3, 48)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-cooldown-reset").await;
    join_user(&manager, &room, 1, &media_transport).await?;
    let second_connection = join_user(&manager, &room, 2, &media_transport).await?;

    close_user(&manager, &room, 2, second_connection, &media_transport).await?;
    assert_eq!(router_count(&room).await, 2);
    let third_connection = join_user(&manager, &room, 3, &media_transport).await?;
    assert_eq!(home_worker(&room, 3).await, Some(1));

    close_user(&manager, &room, 3, third_connection, &media_transport).await?;
    manager.drain_cleanup_retries(&media_transport).await;

    assert_eq!(router_count(&room).await, 2);
    Ok(())
}

#[tokio::test]
async fn source_fanout_pressure_places_next_join_on_spillover_worker() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(99, 1, 1, 1)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-fanout").await;
    seed_source_fanout_pressure(&manager, &room, &media_transport).await?;

    join_user(&manager, &room, 3, &media_transport).await?;

    assert_eq!(home_worker(&room, 3).await, Some(1));
    assert_eq!(
        room.test_api()
            .inspect()
            .load_triggered_last_decision_reason(),
        Some(TestPlacementReason::SourceFanoutPressure)
    );
    Ok(())
}

#[tokio::test]
async fn source_fanout_pressure_clears_after_unpublish() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(99, 1, 1, 1)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-fanout-clear").await;
    let stream_id = seed_source_fanout_pressure(&manager, &room, &media_transport).await?;
    let publisher_id = UserId::Integer(1);
    let publisher_connection = user_connection_id(&room, &publisher_id).await?;
    let core = SfuCore::new(media_transport.clone());

    assert_eq!(
        core.session(&room, &publisher_id, publisher_connection)
            .publication()
            .unpublish(&stream_id)
            .await,
        UnpublishOutcome::Unpublished {
            cleanup: TransportEffectOutcome::Applied
        }
    );
    join_user(&manager, &room, 3, &media_transport).await?;

    assert_eq!(home_worker(&room, 3).await, Some(0));
    Ok(())
}

#[tokio::test]
async fn source_fanout_pressure_clears_after_receiver_leave() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(99, 1, 1, 1)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-fanout-leave").await;
    seed_source_fanout_pressure(&manager, &room, &media_transport).await?;
    let receiver_connection = user_connection_id(&room, &UserId::Integer(2)).await?;

    close_user(&manager, &room, 2, receiver_connection, &media_transport).await?;
    join_user(&manager, &room, 3, &media_transport).await?;

    assert_eq!(home_worker(&room, 3).await, Some(0));
    Ok(())
}

#[tokio::test]
async fn source_fanout_pressure_clears_after_receiver_replacement() -> Result<()> {
    let manager = manager_with_policy(load_triggered_policy(99, 2, 1, 1)?);
    let media_transport = media_transport()?;
    let room = serve_room(&manager, "issuer-load-fanout-replace").await;
    seed_source_fanout_pressure(&manager, &room, &media_transport).await?;

    join_user(&manager, &room, 2, &media_transport).await?;
    join_user(&manager, &room, 3, &media_transport).await?;

    assert_eq!(home_worker(&room, 3).await, Some(0));
    Ok(())
}

#[tokio::test]
async fn subscription_change_pauses_and_resumes_consumer_silently() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    publish_track(
        &ready.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &ready.media_transport,
    )
    .await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;

    let core = SfuCore::new(ready.media_transport.clone());
    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&ready.room, &subscriber_id).await?;
    assert_eq!(
        core.session(&ready.room, &subscriber_id, subscriber_connection_id)
            .subscription()
            .update(&publisher_id, &pause_scalable_video_intents())
            .await,
        SubscriptionUpdateOutcome::Applied
    );
    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;
    assert_eq!(ready.room.test_api().inspect().consumer_count().await, 1);

    assert_eq!(
        core.session(&ready.room, &subscriber_id, subscriber_connection_id)
            .subscription()
            .update(&publisher_id, &resume_scalable_video_intents())
            .await,
        SubscriptionUpdateOutcome::Applied
    );
    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;
    assert_eq!(ready.room.test_api().inspect().consumer_count().await, 1);
    Ok(())
}

#[tokio::test]
async fn subscription_change_persists_preference_for_future_consumer_bootstrap() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;
    let core = SfuCore::new(ready.media_transport.clone());
    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&ready.room, &subscriber_id).await?;
    assert_eq!(
        core.session(&ready.room, &subscriber_id, subscriber_connection_id)
            .subscription()
            .update(&publisher_id, &pause_audio_and_scalable_video_intents())
            .await,
        SubscriptionUpdateOutcome::Applied
    );
    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;

    publish_track(
        &ready.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &ready.media_transport,
    )
    .await?;

    assert_eq!(ready.room.test_api().inspect().consumer_count().await, 1);
    Ok(())
}

#[tokio::test]
async fn subscription_change_handles_multiple_stream_types() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    publish_audio_and_camera(&ready.room, &UserId::Integer(1), &ready.media_transport).await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;

    let core = SfuCore::new(ready.media_transport.clone());
    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&ready.room, &subscriber_id).await?;
    assert_eq!(
        core.session(&ready.room, &subscriber_id, subscriber_connection_id)
            .subscription()
            .update(&publisher_id, &pause_audio_and_scalable_video_intents())
            .await,
        SubscriptionUpdateOutcome::Applied
    );

    ready.assert_no_outbound(1)?;
    ready.assert_no_outbound(2)?;
    assert_eq!(ready.room.test_api().inspect().consumer_count().await, 2);
    Ok(())
}

#[tokio::test]
async fn publication_activity_after_source_owner_leave_is_a_noop() -> Result<()> {
    let mut ready = join_ready_users(&[1, 2]).await?;
    publish_track(
        &ready.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        test_video_rtp_parameters(),
        &ready.media_transport,
    )
    .await?;
    ready.drain_user(1)?;
    ready.drain_user(2)?;
    let publisher_id = UserId::Integer(1);
    let publisher_connection = user_connection_id(&ready.room, &publisher_id).await?;

    close_user(
        &ready.manager,
        &ready.room,
        1,
        publisher_connection,
        &ready.media_transport,
    )
    .await?;
    ready.drain_user(2)?;

    let core = SfuCore::new(ready.media_transport.clone());
    let subscriber_id = UserId::Integer(2);
    let subscriber_connection = user_connection_id(&ready.room, &subscriber_id).await?;
    assert_eq!(
        core.session(&ready.room, &subscriber_id, subscriber_connection)
            .subscription()
            .update(&publisher_id, &pause_scalable_video_intents())
            .await,
        SubscriptionUpdateOutcome::Applied
    );
    ready.assert_no_outbound(2)?;

    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    assert_eq!(
        core.session(&ready.room, &publisher_id, publisher_connection)
            .publication()
            .set_activity(&stream_id, PublicationActivity::Inactive)
            .await,
        PublicationActivityOutcome::MissingPublication
    );
    Ok(())
}
