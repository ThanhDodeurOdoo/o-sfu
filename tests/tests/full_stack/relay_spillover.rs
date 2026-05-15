use super::support::*;

#[tokio::test]
async fn fake_rtc_cross_worker_vp8_selected_rid_survives_relay() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-vp8-selected-rid",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(182);
    let subscriber_user_id = UserId::Integer(183);

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut high_source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    assert!(publisher.publish_track(&high_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, publisher_user_id.clone()).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &publisher_user_id,
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;

    let mut low_source = FakeMediaSource::vp8_camera_with_rid("lo");
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut subscriber,
        &mut low_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_cross_worker_h264_selected_rid_requires_idr_after_relay() {
    let _guard = full_stack_test_guard().await;
    let mut config = cross_worker_test_config();
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let room_server = spawn_room_server_with_config(
        config,
        "issuer-cross-worker-h264-selected-rid",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(184);
    let subscriber_user_id = UserId::Integer(185);

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticH264Stream::with_idr(false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, publisher_user_id.clone()).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &publisher_user_id,
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock)
        .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_relays_vp8_after_threshold() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        load_triggered_spillover_test_config(),
        "issuer-load-spillover-vp8-selected-rid",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(190);
    let local_subscriber_user_id = UserId::Integer(191);
    let spillover_subscriber_user_id = UserId::Integer(192);

    let setup = Box::pin(connect_load_triggered_spillover_rtc_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id,
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, _local_subscriber, mut spillover_subscriber)) = setup else {
        return;
    };

    let mut high_source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    assert!(publisher.publish_track(&high_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut spillover_subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(
        spillover_subscriber
            .complete_next_negotiation()
            .await
            .is_some()
    );
    assert_video_subscription_enabled(&mut spillover_subscriber, publisher_user_id.clone()).await;
    assert_consumer_route_active(
        &server,
        &room,
        &spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                spillover_subscriber.user_id(),
                &publisher_user_id,
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut spillover_subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut spillover_subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;

    let mut low_source = FakeMediaSource::vp8_camera_with_rid("lo");
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut spillover_subscriber,
        &mut low_source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_releases_remote_route_after_subscriber_leaves() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        load_triggered_spillover_test_config(),
        "issuer-load-spillover-release-route",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(193);
    let local_subscriber_user_id = UserId::Integer(194);
    let spillover_subscriber_user_id = UserId::Integer(195);

    let setup = Box::pin(connect_load_triggered_spillover_rtc_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        local_subscriber_user_id.clone(),
        spillover_subscriber_user_id.clone(),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut local_subscriber, spillover_subscriber)) = setup else {
        return;
    };

    Box::pin(assert_load_triggered_spillover_release_route_flow(
        &server,
        &room,
        &mut publisher,
        &mut local_subscriber,
        spillover_subscriber,
        &publisher_user_id,
        &spillover_subscriber_user_id,
    ))
    .await;
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_preserves_download_mute_after_subscriber_replacement() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        load_triggered_spillover_test_config(),
        "issuer-load-spillover-replacement-mute",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(196);
    let local_subscriber_user_id = UserId::Integer(197);
    let spillover_subscriber_user_id = UserId::Integer(198);

    let setup = Box::pin(connect_load_triggered_spillover_rtc_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id.clone(),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, _local_subscriber, mut spillover_subscriber)) = setup else {
        return;
    };

    Box::pin(assert_load_triggered_spillover_replacement_mute_flow(
        &server,
        &room,
        &mut publisher,
        &mut spillover_subscriber,
        publisher_user_id,
        spillover_subscriber_user_id,
    ))
    .await;
}
