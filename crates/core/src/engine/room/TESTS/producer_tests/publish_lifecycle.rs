use super::support::*;
use crate::{
    UnpublishIntentOutcome,
    engine::{
        UserInfo,
        source_model::{SourcePolicy, SourcePublishIntent, SourceUnpublishIntent, UserStreamId},
    },
};

#[tokio::test]
async fn production_change_pauses_producer_and_updates_remote_source_snapshot() {
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

    assert_remote_source_snapshot_for_stream(
        &drain_outbound(&mut rx2),
        TestSourceKind::ScalableVideo,
    );
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
    assert!(
        msgs1.is_empty(),
        "publisher should not get a remote source snapshot"
    );
    assert_eq!(msgs2.len(), 1, "subscriber should get a source snapshot");
    assert_remote_source_activity_snapshot(
        &msgs2[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        false,
        false,
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

    let msgs2 = drain_outbound(&mut rx2);
    assert_remote_source_activity_snapshot(
        &msgs2[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        true,
        false,
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

    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&publisher_id)
        .await
        .expect("publisher should have a connection id");
    assert!(matches!(
        room.user_operation(&publisher_id, connection_id, &adapter)
            .start_publish(
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
                true,
            )
            .await,
        Ok(PublishIntentOutcome::Activated)
    ));
    assert_eq!(
        room.test_api().inspect().producer_count().await,
        1,
        "duplicate publish should reuse the committed producer"
    );
    assert!(
        !room
            .has_staged_publish(&publisher_id, connection_id, &stream_id)
            .await
    );
    assert_remote_source_activity_snapshot(
        &drain_outbound(&mut rx2)[0],
        &publisher_id,
        TestSourceKind::ScalableVideo,
        true,
        false,
    );
}

#[tokio::test]
async fn duplicate_camera_publish_intent_applies_presence_before_remote_snapshot() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let publisher_id = UserId::Integer(1);
    let camera_stream = UserStreamId::new("camera");
    let camera_off_intent = SourcePublishIntent::new(
        camera_stream.clone(),
        MediaKind::Video,
        SourcePolicy::hidden(),
    )
    .with_presence(Some(UserInfo {
        is_camera_on: Some(false),
        ..UserInfo::default()
    }));

    assert!(
        room.test_api()
            .media()
            .publish_intent(
                &publisher_id,
                &camera_off_intent,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    assert!(
        room.test_api()
            .media()
            .set_publication_active(&publisher_id, &camera_stream, false, &adapter)
            .await
    );
    drain_outbound(&mut rx2);

    let connection_id = user_connection_id(&room, &publisher_id).await;
    let camera_on_intent = SourcePublishIntent::new(
        camera_stream.clone(),
        MediaKind::Video,
        SourcePolicy::hidden(),
    )
    .with_presence(Some(UserInfo {
        is_camera_on: Some(true),
        ..UserInfo::default()
    }));
    assert!(matches!(
        room.user_operation(&publisher_id, connection_id, &adapter)
            .start_publish(&camera_on_intent, true)
            .await,
        Ok(PublishIntentOutcome::Activated)
    ));

    let source_snapshots = drain_remote_source_snapshots(&mut rx2);
    assert_eq!(
        source_snapshots.len(),
        1,
        "duplicate publish should emit one final source snapshot"
    );
    let [projection] = source_snapshots[0].sources.as_slice() else {
        panic!("subscriber should receive the camera source");
    };
    assert_eq!(projection.source.stream_id(), &camera_stream);
    assert_eq!(projection.owner_info.is_camera_on, Some(true));
    assert!(projection.producer_active);
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
            .any(|message| remote_source_snapshot(message).is_some())
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
    let removal_message = subscriber_messages
        .iter()
        .find(|message| remote_source_snapshot(message).is_some())
        .expect("subscriber should receive a removal snapshot");
    assert_remote_source_removed_snapshot(
        removal_message,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    );
    assert!(publisher_messages.is_empty());
    assert!(
        adapter
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn late_join_receives_remote_source_snapshot_from_route_state() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let adapter = real_adapter();
    let publisher_id = UserId::Integer(1);
    let subscriber_id = UserId::Integer(2);
    let (publisher_tx, mut publisher_rx) = test_sender();
    let (subscriber_tx, mut subscriber_rx) = test_sender();

    join_user_with_sender(&room, publisher_id.clone(), publisher_tx).await;
    make_session_ready_with_transport(&room, &publisher_id, &adapter).await;
    publish_track(
        &room,
        &publisher_id,
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    assert!(drain_outbound(&mut publisher_rx).is_empty());

    join_user_with_sender(&room, subscriber_id, subscriber_tx).await;
    make_session_ready_with_transport(&room, &UserId::Integer(2), &adapter).await;

    let snapshot = drain_remote_source_snapshots(&mut subscriber_rx)
        .into_iter()
        .next()
        .expect("late subscriber should receive a source snapshot");
    assert!(snapshot.requires_negotiation);
    assert_eq!(snapshot.sources.len(), 1);
    assert_eq!(snapshot.sources[0].source.owner().user_id(), &publisher_id);
    assert_eq!(
        snapshot.sources[0].source.stream_id(),
        &stream_id_for_source(TestSourceKind::ScalableVideo)
    );
}

#[tokio::test]
async fn publish_track_emits_remote_source_snapshot_with_committed_mid() {
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

    assert!(drain_outbound(&mut rx1).is_empty());
    let snapshot = drain_remote_source_snapshots(&mut rx2)
        .into_iter()
        .next()
        .expect("subscriber should receive a remote source snapshot");
    let [projection] = snapshot.sources.as_slice() else {
        panic!("subscriber should receive one remote source");
    };
    assert!(!projection.consumer_mid.is_empty());
    assert_eq!(
        projection.source.stream_id(),
        &stream_id_for_source(TestSourceKind::ScalableVideo)
    );
}

#[tokio::test]
async fn camera_user_info_update_refreshes_remote_source_owner_info() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let publisher_id = UserId::Integer(1);
    let camera_stream = UserStreamId::new("camera");
    let camera_intent = SourcePublishIntent::new(
        camera_stream.clone(),
        MediaKind::Video,
        SourcePolicy::hidden(),
    );

    assert!(
        room.test_api()
            .media()
            .publish_intent(
                &publisher_id,
                &camera_intent,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let connection_id = user_connection_id(&room, &publisher_id).await;
    room.update_user_info(
        &publisher_id,
        connection_id,
        &adapter,
        UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::default()
        },
    )
    .await;

    let snapshot = drain_remote_source_snapshots(&mut rx2)
        .into_iter()
        .next()
        .expect("subscriber should receive a refreshed camera source snapshot");
    assert!(!snapshot.requires_negotiation);
    assert!(snapshot.sources.iter().any(|projection| {
        projection.source.stream_id() == &camera_stream
            && projection.owner_info.is_camera_on == Some(false)
    }));
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
            .any(|message| remote_source_snapshot(message).is_some())
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
            .filter(|message| remote_source_snapshot(message).is_some())
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
async fn production_change_updates_screen_remote_source_activity() {
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

    let msgs = drain_outbound(&mut rx2);
    assert_remote_source_activity_snapshot(
        &msgs[0],
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        false,
        false,
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
            .set_publication_active(
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

    let user_id = UserId::Integer(1);
    let connection_id = user_connection_id(&room, &user_id).await;
    let intent = SourceUnpublishIntent::new(stream_id_for_source(TestSourceKind::ScalableVideo))
        .with_presence(Some(UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::default()
        }));
    assert_ne!(
        room.user_operation(&user_id, connection_id, &adapter)
            .stop_publish(&intent)
            .await,
        UnpublishIntentOutcome::Noop
    );

    let Some((_, info)) = room.test_api().inspect().user_info_snapshot(&user_id).await else {
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
