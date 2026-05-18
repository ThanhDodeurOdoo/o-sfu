use super::support::*;

#[tokio::test]
async fn fake_rtc_peers_forward_vp8_high_rid_keyframe_without_browsers() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-vp8-synthetic").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(92),
        UserId::Integer(93),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(92),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(92)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(92),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_requires_keyframe_before_forwarding() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-vp8-selected-rid-keyframe").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(82),
        UserId::Integer(83),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(82),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(82)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(82),
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &UserId::Integer(82),
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
async fn fake_rtc_vp8_selected_rid_drops_other_rids_after_activation() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-vp8-selected-rid-filter").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(84),
        UserId::Integer(85),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut high_source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&high_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(84),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(84)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(84),
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &UserId::Integer(84),
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
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
async fn fake_rtc_peers_forward_h264_high_rid_idr_without_browsers() {
    let _guard = full_stack_test_guard().await;
    let mut config = test_config(1_000, 10);
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let room_server =
        spawn_room_server_with_config(config, "issuer-h264-synthetic", TEST_ROOM_KEY).await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(94),
        UserId::Integer(95),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::h264_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(94),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(94)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(94),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_h264_selected_rid_requires_idr_before_forwarding() {
    let _guard = full_stack_test_guard().await;
    let mut config = test_config(1_000, 10);
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let room_server =
        spawn_room_server_with_config(config, "issuer-h264-selected-rid-idr", TEST_ROOM_KEY).await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(78),
        UserId::Integer(79),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::new(SyntheticH264Stream::with_idr(false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(78),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(78)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(78),
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &UserId::Integer(78),
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
async fn fake_rtc_peer_rejects_invalid_synthetic_send_paths_without_panics() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-invalid-synthetic-send").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(96),
        UserId::Integer(97),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(96),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(96)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(96),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    let mut unsupported_codec = FakeMediaSource::unsupported_camera_codec();
    let mut missing_rid = FakeMediaSource::vp8_camera_with_rid("missing");
    assert!(
        publisher
            .send_rtp_packet(&mut unsupported_codec, &mut clock)
            .await
            .is_none()
    );
    assert!(
        publisher
            .send_rtp_packet(&mut missing_rid, &mut clock)
            .await
            .is_none()
    );
}
