use super::support::*;

#[tokio::test]
async fn staged_negotiated_publish_rollback_cleans_transport_media_without_committing_state() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;

    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
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
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
}

#[tokio::test]
async fn staged_publish_duplicate_after_transport_reservation_cleans_second_media() {
    let scenario = StagedPublishScenario::new().await;
    let session_key = scenario
        .room
        .transport_user_key(&scenario.user_id, scenario.connection_id);
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
        .stage_duplicate_after_next_publish_reservation_for_test(pre_reserved_media_id);

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::DuplicateAfterReservation {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
    let cleanup_target = scenario
        .room
        .duplicate_staged_publish_cleanup_target_for_test()
        .expect("test hook should record duplicate cleanup target");

    assert_eq!(scenario.staged_count().await, 1);
    assert_eq!(
        scenario
            .staged_transport_media_id(TestSourceKind::ScalableVideo)
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
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
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
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(scenario.scalable_video_is_published().await);
    assert!(
        scenario
            .room
            .test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await
            .is_some()
    );
    assert!(scenario.drain_publisher().is_empty());
    assert_bootstrap_for_stream(&scenario.drain_subscriber(), TestSourceKind::ScalableVideo);
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

    scenario.rollback_connection().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    scenario.assert_no_outbound();
}
