use super::support::*;

#[tokio::test]
async fn staged_negotiated_publish_rollback_cleans_transport_media_without_committing_state() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_media_id(TestSourceKind::ScalableVideo)
        .await;

    assert_eq!(
        scenario.rollback_scalable_video().await,
        Some(crate::TransportEffectOutcome::Applied)
    );

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert!(
        !scenario
            .route_for_staged_media_exists(transport_media_id)
            .await
    );
    scenario.assert_no_outbound();
}

#[tokio::test]
async fn duplicate_staged_publish_is_ignored_before_transport_reservation() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Duplicate
    );
    assert_eq!(scenario.staged_count().await, 1);
    assert_eq!(
        scenario.rollback_scalable_video().await,
        Some(crate::TransportEffectOutcome::Applied)
    );
}

#[tokio::test]
async fn staged_publish_duplicate_after_transport_reservation_cleans_second_media() {
    let scenario = StagedPublishScenario::new().await;
    let session_key = scenario
        .room
        .transport_user_key(&scenario.user_id, scenario.connection_id)
        .await;
    let pre_reserved_media_id = scenario
        .adapter
        .publish_media(
            &session_key,
            MediaKind::Video,
            &test_simulcast_video_rtp_parameters(),
        )
        .await
        .expect("test reservation should allocate transport media");
    scenario
        .room
        .stage_next_duplicate_for_test(pre_reserved_media_id);

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::DuplicateAfterReservation
    );
    let cleanup_target = scenario
        .room
        .duplicate_cleanup_target_for_test()
        .expect("test hook should record duplicate cleanup target");

    assert_eq!(scenario.staged_count().await, 1);
    assert_eq!(
        scenario
            .staged_media_id(TestSourceKind::ScalableVideo)
            .await,
        pre_reserved_media_id
    );
    assert!(
        scenario
            .adapter
            .transport_media_mid(&session_key, pre_reserved_media_id)
            .await
            .is_some()
    );
    assert!(
        scenario
            .adapter
            .transport_media_mid(&session_key, cleanup_target)
            .await
            .is_none()
    );
    assert_eq!(
        scenario.rollback_scalable_video().await,
        Some(crate::TransportEffectOutcome::Applied)
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_moves_through_room_owned_transaction() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_media_id(TestSourceKind::ScalableVideo)
        .await;

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(scenario.scalable_video_is_published().await);
    assert!(
        scenario
            .room
            .state
            .read()
            .await
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .is_some()
    );
    assert!(scenario.drain_publisher().is_empty());
    let subscriber_output = scenario.drain_subscriber();
    assert!(
        !subscriber_output.iter().any(|message| {
            remote_source_snapshot(message).is_some_and(|snapshot| {
                snapshot.requires_negotiation && snapshot.sources.is_empty()
            })
        }),
        "pending consumer routes must not emit empty remote source snapshots"
    );
    assert_remote_source_snapshot_for_stream(&subscriber_output, TestSourceKind::ScalableVideo);
}

#[tokio::test]
async fn staged_publish_connection_cleanup_rolls_back_every_staged_stream() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(
        scenario.stage_source(TestSourceKind::ReadableVideo).await,
        PublishStageOutcome::Staged
    );

    scenario.close_user().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    let publisher_output = scenario.drain_publisher();
    assert!(
        matches!(
            publisher_output.as_slice(),
            [UserOutbound::Close(UserCloseReason::RemovedByRuntime)]
        ),
        "publisher should only receive the normal close output: {publisher_output:?}"
    );
    let subscriber_output = scenario.drain_subscriber();
    assert!(
        matches!(
            subscriber_output.as_slice(),
            [UserOutbound::Message(RoomEventMessage::UserDeparted { user_id })]
                if user_id == &scenario.user_id
        ),
        "subscriber should only receive the normal departure output: {subscriber_output:?}"
    );
}
