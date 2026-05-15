use super::support::*;

#[tokio::test]
async fn fake_peers_publish_and_receive_track_snapshot_over_real_server_entries() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-a").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let peers =
        connect_two_fake_peers(&server, &room, UserId::Integer(1), UserId::Integer(2)).await;
    assert!(peers.is_some());
    let Some((mut publisher, mut subscriber)) = peers else {
        return;
    };

    assert!(publisher.welcome().features.rtc);
    assert!(subscriber.welcome().features.rtc);

    let source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(&mut subscriber, UserId::Integer(1), StreamType::Audio, true).await;
}

#[tokio::test]
async fn fake_peers_keep_room_topology_isolation_with_same_user_ids() {
    let _guard = full_stack_test_guard().await;
    let config = test_config(1_000, 10);

    let server = spawn_test_server(config).await.ok();
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };

    let peers = Box::pin(connect_two_isolated_audio_flows(&server)).await;
    assert!(peers.is_some());
    let Some((mut publisher_a, mut subscriber_a, mut publisher_b, mut subscriber_b)) = peers else {
        return;
    };

    let source = FakeMediaSource::audio();
    assert!(publisher_a.publish_track(&source).await.is_some());
    assert!(publisher_a.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber_a,
        UserId::Integer(90),
        StreamType::Audio,
        true,
    )
    .await;
    assert_no_server_message_protocol(&mut subscriber_b).await;

    assert!(publisher_b.publish_track(&source).await.is_some());
    assert!(publisher_b.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber_b,
        UserId::Integer(90),
        StreamType::Audio,
        true,
    )
    .await;

    assert!(publisher_a.close().await.is_some());
    assert_departure_message_protocol(&mut subscriber_a, UserId::Integer(90)).await;
    assert_no_server_message_protocol(&mut subscriber_b).await;
}

#[tokio::test]
async fn fake_peers_cover_publish_unpublish_late_join_and_disconnect_deterministically() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-b").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let peers = connect_camera_flow_peers(&server, &room).await;
    assert!(peers.is_some());
    let Some((mut publisher, mut subscriber)) = peers else {
        return;
    };

    assert!(
        publish_camera_track(&mut publisher, &mut subscriber)
            .await
            .is_some()
    );

    assert_consumer_download_toggle_round_trip_protocol(&mut subscriber).await;
    assert_camera_unpublish_updates_snapshot_and_info(&mut publisher, &mut subscriber).await;

    let late_subscriber = connect_late_subscriber(&server, &room).await;
    assert!(late_subscriber.is_some());
    let Some(mut late_subscriber) = late_subscriber else {
        return;
    };
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(30)).await;
    assert_late_join_has_no_track_snapshot(&mut late_subscriber).await;

    assert!(publisher.close().await.is_some());
    assert_departure_message_protocol(&mut subscriber, UserId::Integer(10)).await;
    assert_departure_message_protocol(&mut late_subscriber, UserId::Integer(10)).await;
}

#[tokio::test]
async fn fake_peers_cover_user_replacement_and_republish_over_protocol_user_flow() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-c").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let peers =
        connect_two_fake_peers(&server, &room, UserId::Integer(40), UserId::Integer(50)).await;
    assert!(peers.is_some());
    let Some((mut initial_publisher, mut subscriber)) = peers else {
        return;
    };

    let replacement = connect_fake_peer(&server, &room, UserId::Integer(40), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(&mut subscriber, UserId::Integer(40)).await;
    assert_peer_joined_message_protocol(&mut subscriber, UserId::Integer(40)).await;

    let source = FakeMediaSource::audio();
    assert!(replacement.publish_track(&source).await.is_some());
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(40),
        StreamType::Audio,
        true,
    )
    .await;
}
