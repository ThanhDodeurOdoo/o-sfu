use super::support::*;

#[tokio::test]
async fn fake_rtc_peers_rebootstrap_user_replacement_without_stale_media_routes() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-replacement-rtc",
        UserId::Integer(80),
        UserId::Integer(81),
    )
    .await?;

    let mut source = FakeMediaSource::audio();
    publish_source_and_ready_route(
        &server,
        &room,
        &mut initial_publisher,
        &mut subscriber,
        &UserId::Integer(80),
        &source,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_audio_packet_forwarded(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(80), TEST_ROOM_KEY).await;
    let mut replacement = require_some(replacement, "replacement peer should connect")?;

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(&mut subscriber, UserId::Integer(80)).await;
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(80)).await;

    assert_audio_packet_dropped(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    require_some(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await,
        "replacement peer should reach ready state",
    )?;
    publish_source_and_ready_route(
        &server,
        &room,
        &mut replacement,
        &mut subscriber,
        &UserId::Integer(80),
        &source,
    )
    .await;
    assert_audio_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_replacement_unpublish_and_republish_leave_no_stale_consumer_state() -> TestResult
{
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-replacement-unpublish",
        UserId::Integer(82),
        UserId::Integer(83),
    )
    .await?;

    Box::pin(assert_replacement_unpublish_and_republish_flow(
        &server,
        &room,
        &mut initial_publisher,
        &mut subscriber,
        UserId::Integer(82),
    ))
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_subscriber_replacement_preserves_download_mute_after_renegotiation() -> TestResult
{
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-subscriber-replacement-mute",
        UserId::Integer(82),
        UserId::Integer(83),
    )
    .await?;

    Box::pin(
        assert_subscriber_replacement_preserves_download_mute_after_renegotiation(
            &server,
            &room,
            &mut publisher,
            &mut subscriber,
        ),
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_emit_presence_updates_after_rejoin() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let RoomFakePeers {
        server,
        room,
        publisher: mut initial,
        subscriber: mut observer,
    } = room_fake_peers(
        "issuer-replacement-rtc-info",
        UserId::Integer(84),
        UserId::Integer(85),
    )
    .await?;

    assert_peer_joined_message_protocol(&mut initial, UserId::Integer(85)).await;

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(84), TEST_ROOM_KEY).await;
    let replacement = require_some(replacement, "replacement peer should connect")?;

    let _ = initial
        .send_info(UserInfo {
            is_talking: Some(true),
            ..UserInfo::default()
        })
        .await;

    assert_eq!(
        initial.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(&mut observer, UserId::Integer(84)).await;
    assert_peer_joined_message_protocol(&mut observer, UserId::Integer(84)).await;
    assert_no_server_message_protocol(&mut observer).await;
    require_some(replacement.close().await, "replacement should close")?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_finish_a_queued_publish_negotiation() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-replacement-rtc-queued-publish",
        UserId::Integer(86),
        UserId::Integer(87),
    )
    .await?;

    let mut source = FakeMediaSource::audio();
    require_some(
        initial_publisher.publish_track(&source).await,
        "initial publisher should send audio publish intent",
    )?;
    let request = initial_publisher.read_next_server_request().await;
    let (request_id, request) =
        require_some(request, "initial publisher should receive renegotiation")?;
    assert!(
        matches!(request, ServerRequest::Renegotiate(_)),
        "publish should leave a renegotiation answer pending on the original socket"
    );

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(86), TEST_ROOM_KEY).await;
    let mut replacement = require_some(replacement, "replacement peer should connect")?;

    assert_departure_message_protocol(&mut subscriber, UserId::Integer(86)).await;
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(86)).await;

    require_some(
        initial_publisher
            .respond_to_server_request(request_id, request)
            .await,
        "stale publisher should send queued negotiation response",
    )?;
    assert_no_server_message_protocol(&mut subscriber).await;

    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );

    require_some(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await,
        "replacement peer should reach ready state",
    )?;
    publish_source_and_ready_route(
        &server,
        &room,
        &mut replacement,
        &mut subscriber,
        &UserId::Integer(86),
        &source,
    )
    .await;
    assert_audio_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
    Ok(())
}
