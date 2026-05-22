use super::support::*;

#[tokio::test]
async fn fake_rtc_peers_forward_vp8_high_rid_keyframe_without_browsers() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-vp8-synthetic",
        UserId::Integer(92),
        UserId::Integer(93),
    )
    .await?;

    let mut source = FakeMediaSource::vp8_camera_high();
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(92),
        &source,
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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_requires_keyframe_before_forwarding() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-vp8-selected-rid-keyframe",
        UserId::Integer(82),
        UserId::Integer(83),
    )
    .await?;

    let mut source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(82),
        &source,
    )
    .await;
    assert_video_subscription_selected_rid(&server, &room, &subscriber, &UserId::Integer(82), "hi")
        .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_drops_other_rids_after_activation() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-vp8-selected-rid-filter",
        UserId::Integer(84),
        UserId::Integer(85),
    )
    .await?;

    let mut high_source = FakeMediaSource::vp8_camera_high();
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(84),
        &high_source,
    )
    .await;
    assert_video_subscription_selected_rid(&server, &room, &subscriber, &UserId::Integer(84), "hi")
        .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_forward_h264_high_rid_idr_without_browsers() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let mut config = test_config(1_000, 10);
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers_with_config(
        config,
        "issuer-h264-synthetic",
        UserId::Integer(94),
        UserId::Integer(95),
    )
    .await?;

    let mut source = FakeMediaSource::h264_camera_high();
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(94),
        &source,
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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_h264_selected_rid_requires_idr_before_forwarding() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let mut config = test_config(1_000, 10);
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers_with_config(
        config,
        "issuer-h264-selected-rid-idr",
        UserId::Integer(78),
        UserId::Integer(79),
    )
    .await?;

    let mut source = FakeMediaSource::new(SyntheticH264Stream::with_idr(false));
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(78),
        &source,
    )
    .await;
    assert_video_subscription_selected_rid(&server, &room, &subscriber, &UserId::Integer(78), "hi")
        .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peer_rejects_invalid_synthetic_send_paths_without_panics() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-invalid-synthetic-send",
        UserId::Integer(96),
        UserId::Integer(97),
    )
    .await?;

    let source = FakeMediaSource::vp8_camera_high();
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(96),
        &source,
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
    Ok(())
}
