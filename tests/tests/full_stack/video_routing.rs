use super::support::{self as s, media as m, setup as st};

#[tokio::test]
async fn fake_rtc_peers_forward_vp8_high_rid_keyframe_without_browsers() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    Box::pin(assert_video_source_forwarded(
        None,
        "issuer-vp8-synthetic",
        s::UserId::Integer(92),
        s::UserId::Integer(93),
        s::FakeMediaSource::vp8_camera_high(),
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_requires_keyframe_before_forwarding() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    Box::pin(assert_selected_rid_requires_refresh(
        None,
        "issuer-vp8-selected-rid-keyframe",
        s::UserId::Integer(82),
        s::UserId::Integer(83),
        s::FakeMediaSource::new(s::SyntheticVp8Stream::with_next_keyframe(false)),
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_drops_other_rids_after_activation() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-vp8-selected-rid-filter", 84, 85).await?;

    let mut high_source = s::FakeMediaSource::vp8_camera_high();
    m::publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &s::UserId::Integer(84),
        &high_source,
    )
    .await;
    m::assert_video_subscription_selected_rid(
        &server,
        &room,
        &subscriber,
        &s::UserId::Integer(84),
        "hi",
    )
    .await;

    let mut clock = s::FakeClock::default();
    m::assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;

    let mut low_source = s::FakeMediaSource::vp8_camera_with_rid("lo");
    m::assert_packet_dropped(&mut publisher, &mut subscriber, &mut low_source, &mut clock).await;
    m::assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_forward_h264_high_rid_idr_without_browsers() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    Box::pin(assert_video_source_forwarded(
        Some(h264_config()),
        "issuer-h264-synthetic",
        s::UserId::Integer(94),
        s::UserId::Integer(95),
        s::FakeMediaSource::h264_camera_high(),
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_h264_selected_rid_requires_idr_before_forwarding() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    Box::pin(assert_selected_rid_requires_refresh(
        Some(h264_config()),
        "issuer-h264-selected-rid-idr",
        s::UserId::Integer(78),
        s::UserId::Integer(79),
        s::FakeMediaSource::new(s::SyntheticH264Stream::with_idr(false)),
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peer_rejects_invalid_synthetic_send_paths_without_panics() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-invalid-synthetic-send", 96, 97).await?;

    let source = s::FakeMediaSource::vp8_camera_high();
    m::publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &s::UserId::Integer(96),
        &source,
    )
    .await;

    let mut clock = s::FakeClock::default();
    let mut unsupported_codec = s::FakeMediaSource::unsupported_camera_codec();
    let mut missing_rid = s::FakeMediaSource::vp8_camera_with_rid("missing");
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

async fn assert_video_source_forwarded(
    config: Option<s::Config>,
    issuer: &str,
    publisher_user_id: s::UserId,
    subscriber_user_id: s::UserId,
    mut source: s::FakeMediaSource,
) -> s::TestResult {
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = video_room(
        config,
        issuer,
        publisher_user_id.clone(),
        subscriber_user_id,
    )
    .await?;
    m::publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &source,
    )
    .await;
    let mut clock = s::FakeClock::default();
    m::assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    Ok(())
}

async fn assert_selected_rid_requires_refresh(
    config: Option<s::Config>,
    issuer: &str,
    publisher_user_id: s::UserId,
    subscriber_user_id: s::UserId,
    mut source: s::FakeMediaSource,
) -> s::TestResult {
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = video_room(
        config,
        issuer,
        publisher_user_id.clone(),
        subscriber_user_id,
    )
    .await?;
    m::publish_video_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &source,
    )
    .await;
    m::assert_video_subscription_selected_rid(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        "hi",
    )
    .await;

    let mut clock = s::FakeClock::default();
    m::assert_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    m::assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    Ok(())
}

async fn video_room(
    config: Option<s::Config>,
    issuer: &str,
    publisher_user_id: s::UserId,
    subscriber_user_id: s::UserId,
) -> s::TestResult<st::ReadyRoomFakePeers> {
    match config {
        Some(config) => {
            st::ready_room_fake_peers_with_config(
                config,
                issuer,
                publisher_user_id,
                subscriber_user_id,
            )
            .await
        }
        None => st::ready_room_fake_peers(issuer, publisher_user_id, subscriber_user_id).await,
    }
}

fn h264_config() -> s::Config {
    let mut config = s::test_config(1_000, 10);
    config.codecs.flags = s::MediaCodecFlags::default()
        .with_vp8(false)
        .with_h264(true);
    config
}
