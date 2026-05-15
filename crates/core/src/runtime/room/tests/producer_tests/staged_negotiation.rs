use super::support::*;

#[tokio::test]
async fn staged_negotiated_publish_rollback_cleans_transport_media_without_committing_state() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(scenario.staged_count().await, 1);

    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    scenario.assert_no_outbound();

    assert!(
        scenario.publish_media_requested_count() > 0,
        "staging should declare producer media on the transport"
    );
    assert!(
        scenario.removed_media_count() > 0,
        "rolling back a staged publish should remove the staged transport media"
    );
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
        scenario.publish_media_requested_count(),
        1,
        "the pre-await duplicate check should avoid reserving a second transport media"
    );
    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
}

#[tokio::test]
async fn staged_publish_rollback_reports_cleanup_failure_without_state_ownership() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    scenario
        .fake
        .fail_remove_media_until_allowed(transport_media_id);

    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Failed
        }
    );
    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert_eq!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count(),
        1
    );
    assert!(!scenario.has_removed_media(transport_media_id));

    scenario.fake.allow_remove_media(transport_media_id);
    scenario
        .room
        .test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&scenario.adapter)
        .await;

    assert_eq!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count(),
        0
    );
    assert!(scenario.has_removed_media(transport_media_id));
}

#[tokio::test]
async fn staged_negotiated_publish_commit_moves_through_room_owned_transaction() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(scenario.staged_count().await, 1);

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(
        scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    assert!(scenario.drain_publisher().is_empty());
    assert_bootstrap_for_stream(&scenario.drain_subscriber(), TestSourceKind::ScalableVideo);
    assert!(
        scenario.removed_media_count() == 0,
        "successful commit should not compensate the staged producer media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_materializes_all_negotiated_encodings() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    scenario.fake.set_negotiated_producer_parameters(
        transport_media_id,
        test_simulcast_video_rtp_parameters(),
    );

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(
        scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    assert_eq!(
        scenario
            .room
            .test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await
            .expect("transport media should resolve to source encodings")
            .len(),
        2
    );
    assert!(scenario.drain_publisher().is_empty());
    let subscriber_messages = scenario.drain_subscriber();
    assert_bootstrap_for_stream(&subscriber_messages, TestSourceKind::ScalableVideo);
    assert!(
        subscriber_messages.iter().any(|message| matches!(
            message,
            UserOutbound::Request(request)
                if matches!(
                    request.as_ref(),
                    RoomEventRequest::BootstrapRemoteTrack(payload)
                        if payload.source_descriptor().encodings().count() == 2
                )
        )),
        "consumer bootstrap should carry the full committed source encoding graph"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_cleans_up_when_transport_parameters_are_missing() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    scenario
        .fake
        .clear_negotiated_producer_parameters(transport_media_id);

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(
        !scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    scenario.assert_no_outbound();
    assert!(
        scenario.has_removed_media(transport_media_id),
        "commit should clean up the staged transport media when negotiated parameters are unavailable"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_cleans_up_when_user_state_rejects_it() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert!(
        scenario
            .room
            .remove_user_with_cleanup(
                &scenario.user_id,
                scenario.connection_id,
                UserCleanup::state_only(Some(&scenario.adapter)),
            )
            .await
    );
    let _ = scenario.drain_publisher();
    let _ = scenario.drain_subscriber();

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert!(
        !scenario
            .room
            .test_api()
            .inspect()
            .has_session(&scenario.user_id)
            .await
    );
    scenario.assert_no_outbound();
    assert!(
        scenario.removed_media_count() > 0,
        "commit rejection should clean up the staged transport media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_rejects_replaced_connection() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    let (replacement_sender, _replacement_rx) = test_sender();
    let replacement_connection_id = scenario
        .room
        .test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            scenario.user_id.clone(),
            None,
            UserPermissions::default(),
            replacement_sender,
            &scenario.adapter,
        )
        .await
        .expect("replacement user should join");
    let _ = scenario.drain_publisher();
    let _ = scenario.drain_subscriber();

    scenario.commit().await;

    assert_ne!(replacement_connection_id, scenario.connection_id);
    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert!(
        !scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    assert_eq!(
        scenario
            .room
            .test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await,
        None
    );
    scenario.assert_no_outbound();
    assert!(
        scenario.has_removed_media(transport_media_id),
        "stale replaced publish commit should clean up the staged transport media"
    );
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
    assert_eq!(scenario.staged_count().await, 2);

    scenario.rollback_connection().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    scenario.assert_no_outbound();
    assert_eq!(
        scenario.removed_media_count(),
        2,
        "connection cleanup should remove every staged publish transport media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_duplicate_race_keeps_one_staged_entry_and_one_cleanup() {
    let scenario = StagedPublishScenario::new().await;
    scenario
        .fake
        .set_publish_media_delay(Some(Duration::from_millis(200)));
    let (first_stage, second_stage) = tokio::join!(
        scenario.stage_scalable_video(),
        scenario.stage_scalable_video(),
    );

    let outcomes = [first_stage, second_stage];
    assert!(outcomes.contains(&PublishStageOutcome::Staged));
    assert!(
        outcomes.contains(&PublishStageOutcome::DuplicateAfterReservation {
            cleanup: crate::TransportEffectOutcome::Applied,
        })
    );
    assert_eq!(scenario.staged_count().await, 1);

    assert_eq!(
        scenario.publish_media_requested_count(),
        2,
        "both racing stage attempts should declare transport media before the post-await duplicate re-check"
    );
    assert_eq!(
        scenario.removed_media_count(),
        1,
        "the duplicate staged transport media should be compensated exactly once"
    );
    assert!(matches!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack { .. }
    ));
}

#[tokio::test]
async fn cancelled_staged_publish_does_not_create_pending_owner() {
    let scenario = StagedPublishScenario::new().await;
    scenario
        .fake
        .set_publish_media_delay(Some(Duration::from_millis(200)));

    let stage_task = tokio::spawn({
        let room = Arc::clone(&scenario.room);
        let adapter = scenario.adapter.clone();
        let user_id = scenario.user_id.clone();
        let connection_id = scenario.connection_id;
        async move {
            room.stage_negotiated_publish(
                &user_id,
                connection_id,
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
        }
    });

    wait_for_fake_event(&scenario.fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;
    stage_task.abort();
    let join_error = stage_task
        .await
        .expect_err("aborted staged publish task should report cancellation");
    assert!(join_error.is_cancelled());

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(
        scenario.publish_media_requested_count(),
        1,
        "the publish intent reached the transport boundary before cancellation"
    );
    assert_eq!(
        scenario.removed_media_count(),
        0,
        "cancellation before transport returns must not invent cleanup work"
    );
}
