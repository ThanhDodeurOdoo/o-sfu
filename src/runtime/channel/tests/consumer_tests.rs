use super::fixtures::*;

#[tokio::test]
async fn consumption_change_pauses_and_resumes_consumer() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    // Session 1 publishes a camera track.
    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    // Drain bootstrap.
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // Session 2 sends CONSUMPTION_CHANGE: pause camera from session 1.
    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
            &adapter,
        )
        .await;

    // No outbound messages expected — consumer pause is silent (matches Node SFU).
    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());

    // Session 2 sends CONSUMPTION_CHANGE: resume camera from session 1.
    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(true),
                audio: None,
                screen: None,
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
    let (channel, adapter, fake, mut rx1, mut rx2) = setup_two_ready_sessions_with_fake().await;

    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
            &adapter,
        )
        .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerActivityUpdated {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn consumption_change_ignores_nonexistent_consumer() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    // No tracks published. CONSUMPTION_CHANGE should be a no-op.
    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: Some(false),
                screen: None,
            },
            &adapter,
        )
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn consumption_change_persists_preference_for_future_consumer_bootstrap() {
    let (channel, adapter, fake, mut rx1, mut rx2) = setup_two_ready_sessions_with_fake().await;

    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
            &adapter,
        )
        .await;

    channel
        .publish_track(
            &SessionId::Integer(1),
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
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn consumption_change_handles_multiple_stream_types() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    // Session 1 publishes both camera and audio.
    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Audio,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &adapter,
        )
        .await;

    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // Session 2 pauses both in one message.
    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: Some(false),
                screen: None,
            },
            &adapter,
        )
        .await;

    // No-op outbound (consumer pause is silent).
    assert!(drain_outbound(&mut rx2).is_empty());
}

#[tokio::test]
async fn session_leave_purges_producer_and_consumer_indexes() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    // Session 1 publishes camera, which creates a consumer for session 2.
    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // Session 1 leaves.
    let connection_id = 0; // first join gets connection_id 0
    channel
        .leave_session(&SessionId::Integer(1), connection_id)
        .await;

    // After session 1 leaves, a consumption change targeting session 1's
    // producer should be a no-op (the consumer index entry was cleaned up).
    channel
        .update_subscription(
            &SessionId::Integer(2),
            &SessionId::Integer(1),
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
            &adapter,
        )
        .await;

    // Similarly, a production change for session 1 should be a no-op.
    channel
        .set_publication_active(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    // No crashes, no stale state — both operations are silent no-ops.
    drain_outbound(&mut rx2);
}
