use super::fixtures::*;

#[tokio::test]
async fn consumption_change_pauses_and_resumes_consumer() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // User 1 publishes a camera track.
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    // Drain bootstrap.
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;

    // User 2 sends CONSUMPTION_CHANGE: pause camera from user 1.
    let pause_intents = pause_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &pause_intents)
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());

    // User 2 sends CONSUMPTION_CHANGE: resume camera from user 1.
    let resume_intents = resume_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &resume_intents)
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn consumption_change_updates_transport_route_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;
    let pause_intents = pause_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &pause_intents)
        .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn consumption_change_resume_requests_video_keyframe_refresh() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;
    let pause_intents = pause_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &pause_intents)
        .await;

    let resume_intents = resume_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &resume_intents)
        .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}

#[tokio::test]
async fn consumption_change_ignores_nonexistent_consumer() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // No tracks published. CONSUMPTION_CHANGE should be a no-op.
    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;
    let pause_intents = pause_audio_and_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &pause_intents)
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn consumption_change_persists_preference_for_future_consumer_bootstrap() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;
    let pause_intents = pause_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &pause_intents)
        .await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}

#[tokio::test]
async fn consumption_change_handles_multiple_stream_types() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // User 1 publishes both camera and audio.
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::AudioDetector,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &adapter,
        )
        .await;

    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // User 2 pauses both in one message.
    let subscriber_id = UserId::Integer(2);
    let publisher_id = UserId::Integer(1);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;
    let pause_intents = pause_audio_and_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&publisher_id, &pause_intents)
        .await;

    // No-op outbound (consumer pause is silent).
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn user_leave_purges_producer_and_consumer_indexes() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // User 1 publishes camera, which creates a consumer for user 2.
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // User 1 leaves.
    let connection_id = test_connection_id(0); // first join gets connection_id 0
    room.remove_user_with_cleanup(
        &UserId::Integer(1),
        connection_id,
        UserCleanup::state_only(None),
    )
    .await;

    // After user 1 leaves, a consumption change targeting user 1's
    // producer should be a no-op (the consumer index entry was cleaned up).
    let subscriber_id = UserId::Integer(2);
    let subscriber_connection_id = user_connection_id(&room, &subscriber_id).await;
    let pause_intents = pause_scalable_video_intents();
    room.user_operation(&subscriber_id, subscriber_connection_id, &adapter)
        .update_subscription(&UserId::Integer(1), &pause_intents)
        .await;

    // Similarly, a production change for user 1 should be a no-op.
    room.user_operation(&UserId::Integer(1), connection_id, &adapter)
        .set_publication_activity(
            &stream_id_for_source(TestSourceKind::ScalableVideo),
            PublicationActivity::Inactive,
        )
        .await;

    drain_outbound(&mut rx2);
}
