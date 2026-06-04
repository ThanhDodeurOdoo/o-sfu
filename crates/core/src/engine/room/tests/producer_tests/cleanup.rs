use super::support::*;

#[tokio::test]
async fn explicit_unpublish_removes_state_and_transport_media() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users().await;
    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &media_transport,
    )
    .await;
    drain_outbound(&mut publisher_rx);
    assert_bootstrap_for_stream(
        &drain_outbound(&mut subscriber_rx),
        TestSourceKind::AudioDetector,
    );
    let connection_id = user_connection_id(&room, &UserId::Integer(1)).await;
    let transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            connection_id,
            TestSourceKind::AudioDetector,
        )
        .await
        .expect("published audio should expose a transport media id");

    assert_eq!(
        room.user_operation(&UserId::Integer(1), connection_id, &media_transport)
            .unpublish(&stream_id_for_source(TestSourceKind::AudioDetector))
            .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert_transport_media_mapping_is_missing(&room, transport_media_id).await;
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn explicit_unpublish_queues_cleanup_when_real_transport_owner_is_gone() {
    let mut scenario = Box::pin(setup_real_rtc_refresh_scenario()).await;

    publish_track(
        &scenario.room,
        &scenario.publisher_user_id,
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &scenario.media_transport,
    )
    .await;
    drain_outbound(&mut scenario.publisher_rx);
    drain_outbound(&mut scenario.subscriber_rx);
    let connection_id = user_connection_id(&scenario.room, &scenario.publisher_user_id).await;
    let transport_media_id = scenario
        .room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &scenario.publisher_user_id,
            connection_id,
            TestSourceKind::AudioDetector,
        )
        .await
        .expect("published audio should expose a transport media id");
    let transport_user_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, connection_id)
        .await;
    scenario
        .media_transport
        .close_session(&transport_user_key)
        .await
        .expect("closing the publisher transport should succeed");

    assert_eq!(
        scenario
            .room
            .user_operation(
                &scenario.publisher_user_id,
                connection_id,
                &scenario.media_transport,
            )
            .unpublish(&stream_id_for_source(TestSourceKind::AudioDetector))
            .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Failed
        }
    );

    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert_transport_media_mapping_is_missing(&scenario.room, transport_media_id).await;
    assert!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count()
            > 0
    );
}

#[tokio::test]
async fn late_join_bootstrap_commits_one_consumer_after_subscriber_readiness() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);
    set_consume_transport_ready(&room, &UserId::Integer(2)).await;

    assert!(
        apply_client_rtp_capabilities(
            &room,
            &UserId::Integer(2),
            user_connection_id(&room, &UserId::Integer(2)).await,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await
    );

    assert_bootstrap_for_stream(
        &drain_outbound(&mut subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}
