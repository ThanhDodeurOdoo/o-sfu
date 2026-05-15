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

    let bootstrap_msgs = drain_outbound(&mut rx2);
    assert!(
        bootstrap_msgs
            .iter()
            .any(|m| matches!(m, UserOutbound::Request(..))),
        "user 2 should have received a bootstrap remote track request"
    );
    assert!(drain_outbound(&mut rx1).is_empty());

    let publisher_id = UserId::Integer(1);
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

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

    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        PublicationActivity::Active,
        &adapter,
    )
    .await;

    let msgs1 = drain_outbound(&mut rx1);
    assert_track_binding_activity_update(
        &msgs1[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(true),
    );
}

#[tokio::test]
async fn explicit_unpublish_removes_published_track_and_consumer_routes() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

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
            .any(|message| matches!(message, UserOutbound::Request(_)))
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

    assert_eq!(
        room.unpublish_track(
            &UserId::Integer(1),
            test_connection_id(0),
            &stream_id_for_source(TestSourceKind::ScalableVideo),
            &adapter,
        )
        .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Applied
        }
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
    let removed_media_events = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| matches!(event, FakeMediaTransportEvent::MediaRemoved { .. }))
        .count();
    assert_eq!(removed_media_events, 2);
}

#[tokio::test]
async fn publish_track_uses_negotiated_consumer_rtp_parameters() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    assert!(
        set_client_rtp_capabilities(
            &room,
            &UserId::Integer(2),
            test_client_rtp_capabilities_without_video_rtx(),
        )
        .await
        .session_present
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
    let request = drain_outbound(&mut rx2)
        .into_iter()
        .find_map(|message| match message {
            UserOutbound::Request(request) => Some(*request),
            UserOutbound::Message(_)
            | UserOutbound::TrackBindingUpdate(_)
            | UserOutbound::Close(_) => None,
        })
        .expect("subscriber should receive INIT_CONSUMER");
    let RoomEventRequest::BootstrapRemoteTrack(payload) = request;
    let codecs = payload.rtp_parameters().codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 1);
    assert_eq!(codecs[0].codec_name(), "VP8");
    assert_eq!(codecs[0].payload_type(), 96);
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
            .any(|message| matches!(message, UserOutbound::Request(_)))
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
            .filter(|message| matches!(message, UserOutbound::Request(_)))
            .count(),
        2,
        "subscriber should receive one bootstrap per published stream"
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
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ReadableVideo),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

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
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

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
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ProducerActivityUpdated {
                user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_commits_user_state_before_transport_update_finishes() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

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

    fake.set_producer_active_delay(Some(Duration::from_millis(200)));

    let update_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = adapter.clone();
        let publisher_connection_id = user_connection_id(&room, &UserId::Integer(1)).await;
        async move {
            room.set_publication_active_runtime(
                &UserId::Integer(1),
                publisher_connection_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                PublicationActivity::Inactive,
                &adapter,
            )
            .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ProducerActivityUpdated {
                user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;

    let Some((_, info)) = room
        .test_api()
        .inspect()
        .user_info_snapshot(&UserId::Integer(1))
        .await
    else {
        panic!("publisher user should still be present");
    };
    assert_eq!(info.is_camera_on, Some(false));

    update_task.await.unwrap();
}

#[tokio::test]
async fn production_change_ignores_unknown_stream_type() {
    let (room, adapter, mut rx1, mut _rx2) = setup_two_ready_users().await;

    // No producer published for audio. PRODUCTION_CHANGE should be a no-op.
    let publisher_id = UserId::Integer(1);
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::AudioDetector),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn explicit_unpublish_missing_publication_is_a_domain_noop() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.unpublish_scalable_video().await,
        UnpublishOutcome::MissingPublication
    );
}
