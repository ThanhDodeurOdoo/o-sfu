use super::support::*;

#[tokio::test]
async fn fake_rtc_peers_rebootstrap_user_replacement_without_stale_media_routes() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-replacement-rtc").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(80),
        UserId::Integer(81),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut initial_publisher, mut subscriber)) = setup else {
        return;
    };

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
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

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

    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
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
}

#[tokio::test]
async fn fake_rtc_replacement_unpublish_and_republish_leave_no_stale_consumer_state() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-replacement-unpublish").await;
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
    let Some((mut initial_publisher, mut subscriber)) = setup else {
        return;
    };

    Box::pin(assert_replacement_unpublish_and_republish_flow(
        &server,
        &room,
        &mut initial_publisher,
        &mut subscriber,
        UserId::Integer(82),
    ))
    .await;
}

#[tokio::test]
async fn fake_rtc_subscriber_replacement_preserves_download_mute_after_renegotiation() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-subscriber-replacement-mute").await;
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

    Box::pin(
        assert_subscriber_replacement_preserves_download_mute_after_renegotiation(
            &server,
            &room,
            &mut publisher,
            &mut subscriber,
        ),
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_emit_presence_updates_after_rejoin() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-replacement-rtc-info").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let peers =
        connect_two_fake_peers(&server, &room, UserId::Integer(84), UserId::Integer(85)).await;
    assert!(peers.is_some());
    let Some((mut initial, mut observer)) = peers else {
        return;
    };

    assert_peer_joined_message_protocol(&mut initial, UserId::Integer(85)).await;

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(84), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(replacement) = replacement else {
        return;
    };

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
    assert!(replacement.close().await.is_some());
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_finish_a_queued_publish_negotiation() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-replacement-rtc-queued-publish").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(86),
        UserId::Integer(87),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut initial_publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    assert!(initial_publisher.publish_track(&source).await.is_some());
    let request = initial_publisher.read_next_server_request().await;
    assert!(request.is_some());
    let Some((request_id, request)) = request else {
        return;
    };
    assert!(
        matches!(request, ServerRequest::Renegotiate(_)),
        "publish should leave a renegotiation answer pending on the original socket"
    );

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(86), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_departure_message_protocol(&mut subscriber, UserId::Integer(86)).await;
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(86)).await;

    assert!(
        initial_publisher
            .respond_to_server_request(request_id, request)
            .await
            .is_some()
    );
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

    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
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
}
