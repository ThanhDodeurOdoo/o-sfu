use super::support::*;

#[tokio::test]
async fn fake_peers_publish_and_receive_track_snapshot_over_real_server_entries() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let RoomFakePeers {
        server: _server,
        room: _room,
        mut publisher,
        mut subscriber,
    } = room_fake_peers("issuer-a", UserId::Integer(1), UserId::Integer(2)).await?;

    assert!(publisher.welcome().features.rtc);
    assert!(subscriber.welcome().features.rtc);

    let source = FakeMediaSource::audio();
    require_some(
        publisher.publish_track(&source).await,
        "publisher should send audio publish intent",
    )?;
    require_some(
        publisher.complete_next_negotiation().await,
        "publisher should complete audio negotiation",
    )?;
    assert_track_snapshot(&mut subscriber, UserId::Integer(1), StreamType::Audio, true).await;
    Ok(())
}

#[tokio::test]
async fn fake_peers_keep_room_topology_isolation_with_same_user_ids() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let config = test_config(1_000, 10);

    let server = spawn_test_server(config).await?;

    let peers = Box::pin(connect_two_isolated_audio_flows(&server)).await;
    let (mut publisher_a, mut subscriber_a, mut publisher_b, mut subscriber_b) =
        require_some(peers, "isolated audio flows should connect")?;

    let source = FakeMediaSource::audio();
    require_some(
        publisher_a.publish_track(&source).await,
        "room A publisher should send audio publish intent",
    )?;
    require_some(
        publisher_a.complete_next_negotiation().await,
        "room A publisher should complete audio negotiation",
    )?;
    assert_track_snapshot(
        &mut subscriber_a,
        UserId::Integer(90),
        StreamType::Audio,
        true,
    )
    .await;
    assert_no_server_message_protocol(&mut subscriber_b).await;

    require_some(
        publisher_b.publish_track(&source).await,
        "room B publisher should send audio publish intent",
    )?;
    require_some(
        publisher_b.complete_next_negotiation().await,
        "room B publisher should complete audio negotiation",
    )?;
    assert_track_snapshot(
        &mut subscriber_b,
        UserId::Integer(90),
        StreamType::Audio,
        true,
    )
    .await;

    require_some(publisher_a.close().await, "room A publisher should close")?;
    assert_departure_message_protocol(&mut subscriber_a, UserId::Integer(90)).await;
    assert_no_server_message_protocol(&mut subscriber_b).await;
    Ok(())
}

#[tokio::test]
async fn fake_peers_cover_publish_unpublish_late_join_and_disconnect_deterministically()
-> TestResult {
    let _guard = full_stack_test_guard().await;
    let RoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = room_fake_peers("issuer-b", UserId::Integer(10), UserId::Integer(20)).await?;

    require_some(
        publish_camera_track(&mut publisher, &mut subscriber).await,
        "camera track should publish",
    )?;

    assert_consumer_download_toggle_round_trip_protocol(&mut subscriber).await;
    assert_camera_unpublish_updates_snapshot_and_info(&mut publisher, &mut subscriber).await;

    let late_subscriber = connect_late_subscriber(&server, &room).await;
    let mut late_subscriber = require_some(late_subscriber, "late subscriber should connect")?;
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(30)).await;
    assert_late_join_has_no_track_snapshot(&mut late_subscriber).await;

    require_some(publisher.close().await, "publisher should close")?;
    assert_departure_message_protocol(&mut subscriber, UserId::Integer(10)).await;
    assert_departure_message_protocol(&mut late_subscriber, UserId::Integer(10)).await;
    Ok(())
}

#[tokio::test]
async fn fake_peers_cover_user_replacement_and_republish_over_protocol_user_flow() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let RoomFakePeers {
        server,
        room,
        publisher: mut initial_publisher,
        mut subscriber,
    } = room_fake_peers("issuer-c", UserId::Integer(40), UserId::Integer(50)).await?;

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(40), TEST_ROOM_KEY).await;
    let mut replacement = require_some(replacement, "replacement peer should connect")?;

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(&mut subscriber, UserId::Integer(40)).await;
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(40)).await;

    let source = FakeMediaSource::audio();
    require_some(
        replacement.publish_track(&source).await,
        "replacement should send audio publish intent",
    )?;
    require_some(
        replacement.complete_next_negotiation().await,
        "replacement should complete audio negotiation",
    )?;
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(40),
        StreamType::Audio,
        true,
    )
    .await;
    Ok(())
}
