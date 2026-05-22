use super::support::*;

#[tokio::test]
async fn fake_rtc_cross_worker_vp8_selected_rid_survives_relay() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let publisher_user_id = UserId::Integer(182);
    let subscriber_user_id = UserId::Integer(183);
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-vp8-selected-rid",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    )
    .await?;
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut high_source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &high_source,
    )
    .await;
    assert_video_subscription_selected_rid(&server, &room, &subscriber, &publisher_user_id, "hi")
        .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_cross_worker_h264_selected_rid_requires_idr_after_relay() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let mut config = cross_worker_test_config();
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let publisher_user_id = UserId::Integer(184);
    let subscriber_user_id = UserId::Integer(185);
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers_with_config(
        config,
        "issuer-cross-worker-h264-selected-rid",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    )
    .await?;
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticH264Stream::with_idr(false));
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &source,
    )
    .await;
    assert_video_subscription_selected_rid(&server, &room, &subscriber, &publisher_user_id, "hi")
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
async fn fake_rtc_load_triggered_spillover_relays_vp8_after_threshold() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let publisher_user_id = UserId::Integer(190);
    let local_subscriber_user_id = UserId::Integer(191);
    let spillover_subscriber_user_id = UserId::Integer(192);
    let SpilloverRoomFakePeers {
        server,
        room,
        mut publisher,
        local_subscriber: _local_subscriber,
        mut spillover_subscriber,
    } = spillover_room_fake_peers(
        "issuer-load-spillover-vp8-selected-rid",
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id,
    )
    .await?;

    let mut high_source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut spillover_subscriber,
        &publisher_user_id,
        &high_source,
    )
    .await;
    assert_video_subscription_selected_rid(
        &server,
        &room,
        &spillover_subscriber,
        &publisher_user_id,
        "hi",
    )
    .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_releases_remote_route_after_subscriber_leaves()
-> TestResult {
    let _guard = full_stack_test_guard().await;
    let publisher_user_id = UserId::Integer(193);
    let local_subscriber_user_id = UserId::Integer(194);
    let spillover_subscriber_user_id = UserId::Integer(195);
    let SpilloverRoomFakePeers {
        server,
        room,
        mut publisher,
        mut local_subscriber,
        spillover_subscriber,
    } = spillover_room_fake_peers(
        "issuer-load-spillover-release-route",
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id.clone(),
    )
    .await?;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_preserves_download_mute_after_subscriber_replacement()
-> TestResult {
    let _guard = full_stack_test_guard().await;
    let publisher_user_id = UserId::Integer(196);
    let local_subscriber_user_id = UserId::Integer(197);
    let spillover_subscriber_user_id = UserId::Integer(198);
    let SpilloverRoomFakePeers {
        server,
        room,
        mut publisher,
        local_subscriber: _local_subscriber,
        mut spillover_subscriber,
    } = spillover_room_fake_peers(
        "issuer-load-spillover-replacement-mute",
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id.clone(),
    )
    .await?;

    Box::pin(assert_load_triggered_spillover_replacement_mute_flow(
        &server,
        &room,
        &mut publisher,
        &mut spillover_subscriber,
        publisher_user_id,
        spillover_subscriber_user_id,
    ))
    .await;
    Ok(())
}
