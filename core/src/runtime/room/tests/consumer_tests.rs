use super::fixtures::*;

#[tokio::test]
async fn consumption_change_pauses_and_resumes_consumer() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // User 1 publishes a camera track.
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    // Drain bootstrap.
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // User 2 sends CONSUMPTION_CHANGE: pause camera from user 1.
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    // No outbound messages expected — consumer pause is silent (matches Node SFU).
    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());

    // User 2 sends CONSUMPTION_CHANGE: resume camera from user 1.
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(true),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    // Still no outbound — resume is also silent.
    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn consumption_change_updates_transport_route_activity() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerActivityUpdated {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn consumption_change_resume_requests_video_keyframe_refresh() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerActivityUpdated {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;

    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(true),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerKeyframeRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
            }
        )
    })
    .await;
}

#[tokio::test]
async fn consumption_change_ignores_nonexistent_consumer() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // No tracks published. CONSUMPTION_CHANGE should be a no-op.
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: Some(false),
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn consumption_change_persists_preference_for_future_consumer_bootstrap() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerActivityUpdated {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn consumption_change_handles_multiple_stream_types() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // User 1 publishes both camera and audio.
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Audio,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &adapter,
        )
        .await;

    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // User 2 pauses both in one message.
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: Some(false),
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
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
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // User 1 leaves.
    let connection_id = test_connection_id(0); // first join gets connection_id 0
    room.test_api()
        .lifecycle()
        .leave_user(&UserId::Integer(1), connection_id)
        .await;

    // After user 1 leaves, a consumption change targeting user 1's
    // producer should be a no-op (the consumer index entry was cleaned up).
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    // Similarly, a production change for user 1 should be a no-op.
    room.test_api()
        .media()
        .set_publication_active(&UserId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    // No crashes, no stale state — both operations are silent no-ops.
    drain_outbound(&mut rx2);
}
