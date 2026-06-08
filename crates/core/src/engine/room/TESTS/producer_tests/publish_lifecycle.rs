use super::support::*;

#[tokio::test]
async fn production_change_pauses_producer_and_broadcasts_track_binding() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    assert_remote_track_setup_for_stream(&drain_outbound(&mut rx2), TestSourceKind::ScalableVideo);
    assert!(drain_outbound(&mut rx1).is_empty());

    let publisher_id = UserId::Integer(1);
    assert!(
        room.test_api()
            .media()
            .set_publication_active(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                false,
                &adapter,
            )
            .await
    );

    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert_eq!(msgs1.len(), 1, "user 1 should get track binding update");
    assert_eq!(msgs2.len(), 1, "user 2 should get track binding update");
    assert_track_binding_activity_update(
        &msgs1[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(false),
    );
    assert_track_binding_activity_update(
        &msgs2[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(false),
    );

    assert!(
        room.test_api()
            .media()
            .set_publication_active(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                true,
                &adapter,
            )
            .await
    );

    let msgs1 = drain_outbound(&mut rx1);
    assert_track_binding_activity_update(
        &msgs1[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(true),
    );
}

#[tokio::test]
async fn duplicate_publish_intent_reactivates_committed_stream() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let publisher_id = UserId::Integer(1);

    publish_track(
        &room,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    assert!(
        room.test_api()
            .media()
            .set_publication_active(&publisher_id, &stream_id, false, &adapter)
            .await
    );
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let outcome = room
        .test_api()
        .media()
        .start_publish_intent(
            &publisher_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
            true,
            &adapter,
        )
        .await
        .expect("duplicate publish intent should target a live publisher");

    assert_eq!(outcome, TestPublishIntentOutcome::Activated);
    assert_eq!(
        room.test_api().inspect().producer_count().await,
        1,
        "duplicate publish should reuse the committed producer"
    );
    assert_eq!(
        room.test_api()
            .media()
            .staged_publish_count(
                &publisher_id,
                user_connection_id(&room, &publisher_id).await
            )
            .await,
        0
    );
    assert_track_binding_activity_update(
        &drain_outbound(&mut rx1)[0],
        &publisher_id,
        TestSourceKind::ScalableVideo,
        Some(true),
    );
}

#[tokio::test]
async fn explicit_unpublish_removes_published_track_and_consumer_routes() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::SetupRemoteTrack(_)))
    );
    let Some(transport_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            TestSourceKind::ScalableVideo,
        )
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    assert!(
        room.test_api()
            .media()
            .unpublish_track(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                TestSourceKind::ScalableVideo,
            )
            .await
    );
    assert_transport_media_mapping_is_missing(&room, transport_media_id).await;
    assert_transport_media_owner_mapping_is_missing(&room, transport_media_id).await;

    let publisher_messages = drain_outbound(&mut publisher_rx);
    let subscriber_messages = drain_outbound(&mut subscriber_rx);
    assert!(publisher_messages.iter().any(|message| matches!(
        message,
        UserOutbound::TrackBindingUpdate(update)
            if update.user_id == UserId::Integer(1)
                && update.stream_id == stream_id_for_source(TestSourceKind::ScalableVideo)
                && update.active.is_none()
    )));
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        UserOutbound::TrackBindingUpdate(update)
            if update.user_id == UserId::Integer(1)
                && update.stream_id == stream_id_for_source(TestSourceKind::ScalableVideo)
                && update.active.is_none()
    )));
    assert!(
        adapter
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn publish_track_uses_negotiated_consumer_rtp_parameters() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let subscriber_id = UserId::Integer(2);
    assert!(
        room.test_api()
            .lifecycle()
            .mark_session_ready(
                &subscriber_id,
                test_client_rtp_capabilities_without_video_rtx(),
                &adapter,
            )
            .await
    );

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    let track = drain_outbound(&mut rx2)
        .into_iter()
        .find_map(|message| match message {
            UserOutbound::SetupRemoteTrack(track) => Some(*track),
            UserOutbound::Message(_)
            | UserOutbound::TrackBindingUpdate(_)
            | UserOutbound::Close(_) => None,
        })
        .expect("subscriber should receive INIT_CONSUMER");
    let formats = track.rtp.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 1);
    assert_eq!(formats[0].codec_name(), "VP8");
    assert_eq!(formats[0].payload_type(), 96);
}

#[tokio::test]
async fn user_replacement_purges_stale_published_media_state() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::SetupRemoteTrack(_)))
    );
    let published_transport_media_id = room
        .test_api()
        .inspect()
        .first_published_transport_media_id()
        .await;
    assert!(published_transport_media_id.is_some());

    assert_eq!(room.test_api().inspect().producer_count().await, 1);
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert!(
        room.test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                TestSourceKind::ScalableVideo,
            )
            .await
    );

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        room.test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(1),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    let published_transport_media_id =
        published_transport_media_id.expect("published track should have a transport id");
    assert_transport_media_mapping_is_missing(&room, published_transport_media_id).await;
    assert_transport_media_owner_mapping_is_missing(&room, published_transport_media_id).await;
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                TestSourceKind::ScalableVideo,
            )
            .await
    );
}

#[tokio::test]
async fn user_replacement_purges_all_published_stream_mappings() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &adapter,
    )
    .await;
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert_eq!(
        drain_outbound(&mut subscriber_rx)
            .into_iter()
            .filter(|message| matches!(message, UserOutbound::SetupRemoteTrack(_)))
            .count(),
        2,
        "subscriber should receive one setup request per published stream"
    );

    let camera_transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            TestSourceKind::ScalableVideo,
        )
        .await;
    let audio_transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            TestSourceKind::AudioDetector,
        )
        .await;
    assert!(camera_transport_media_id.is_some());
    assert!(audio_transport_media_id.is_some());

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        room.test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(1),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert_transport_media_mapping_is_missing(
        &room,
        camera_transport_media_id.expect("camera producer should expose a transport id"),
    )
    .await;
    assert_transport_media_mapping_is_missing(
        &room,
        audio_transport_media_id.expect("audio producer should expose a transport id"),
    )
    .await;
    assert_user_has_no_producer_route_target(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        TestSourceKind::ScalableVideo,
    )
    .await;
    assert_user_has_no_producer_route_target(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        TestSourceKind::AudioDetector,
    )
    .await;
}

#[tokio::test]
async fn production_change_updates_screen_track_binding_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let publisher_id = UserId::Integer(1);
    assert!(
        room.test_api()
            .media()
            .set_publication_active(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ReadableVideo),
                false,
                &adapter,
            )
            .await
    );

    let msgs = drain_outbound(&mut rx1);
    assert_track_binding_activity_update(
        &msgs[0],
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        Some(false),
    );
}

#[tokio::test]
async fn production_change_updates_transport_route_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let publisher_id = UserId::Integer(1);
    assert!(
        room.test_api()
            .media()
            .publication_activity(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                false,
                &adapter,
            )
            .await
    );
    let transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &publisher_id,
            user_connection_id(&room, &publisher_id).await,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose a transport media id");
    let route = adapter
        .test_api()
        .route_entry_by_media_id(transport_media_id)
        .await
        .expect("published camera should still have a route entry");
    assert!(!route.source_active);
}

#[tokio::test]
async fn production_change_commits_user_state_before_transport_update_finishes() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    assert!(
        room.test_api()
            .media()
            .set_publication_active(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                false,
                &adapter,
            )
            .await
    );

    let Some((_, info)) = room
        .test_api()
        .inspect()
        .user_info_snapshot(&UserId::Integer(1))
        .await
    else {
        panic!("publisher user should still be present");
    };
    assert_eq!(info.is_camera_on, Some(false));
}

#[tokio::test]
async fn production_change_ignores_unknown_stream_type() {
    let (room, adapter, mut rx1, mut _rx2) = setup_two_ready_users().await;

    let publisher_id = UserId::Integer(1);
    assert!(
        !room
            .test_api()
            .media()
            .set_publication_active(
                &publisher_id,
                &stream_id_for_source(TestSourceKind::AudioDetector),
                false,
                &adapter,
            )
            .await
    );

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn explicit_unpublish_missing_publication_is_a_domain_noop() {
    let scenario = StagedPublishScenario::new().await;

    assert!(
        !scenario
            .room
            .test_api()
            .media()
            .unpublish_track(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &scenario.adapter,
            )
            .await
    );
}
