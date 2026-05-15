use super::support::*;

#[tokio::test]
async fn fake_rtc_peer_media_updates_room_stats_deterministically() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-d").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let peers = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(60),
        UserId::Integer(61),
        Duration::from_secs(5),
    )
    .await;
    assert!(peers.is_some());
    let Some((mut publisher, mut subscriber)) = peers else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(60),
        StreamType::Audio,
        true,
    )
    .await;

    let mut clock = FakeClock::default();
    let stats = stream_until_audio_bitrate_is_observable(
        &server,
        &room,
        &mut publisher,
        &mut source,
        &mut clock,
    )
    .await;
    assert!(stats.is_some());
    let Some(stats) = stats else {
        return;
    };
    assert!(stats.audio > 0);
    assert!(stats.total >= stats.audio);
}

#[tokio::test]
async fn fake_rtc_peers_export_longer_transport_lifetimes_after_steady_state_run() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-lifetime-metrics").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let peers = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(62),
        UserId::Integer(63),
        Duration::from_secs(5),
    )
    .await;
    assert!(peers.is_some());
    let Some((publisher, subscriber)) = peers else {
        return;
    };

    sleep(Duration::from_millis(1_200)).await;

    assert!(publisher.close().await.is_some());
    assert!(subscriber.close().await.is_some());

    let lifetime_metrics = wait_for_transport_lifetime_metrics(&server, 2).await;
    assert!(lifetime_metrics.is_some());
    let Some(lifetime_metrics) = lifetime_metrics else {
        return;
    };

    assert_eq!(lifetime_metrics.le_1_second, 0);
    assert_eq!(lifetime_metrics.le_10_seconds, 2);
    assert_eq!(lifetime_metrics.le_60_seconds, 2);
    assert_eq!(lifetime_metrics.le_300_seconds, 2);
    assert_eq!(lifetime_metrics.count, 2);
    assert!(lifetime_metrics.sum_seconds >= 2.0);
}

#[tokio::test]
async fn fake_rtc_peers_export_transport_and_rtp_metrics_during_live_media() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-live-metrics").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(64),
        UserId::Integer(65),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(64),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    let initial_forwarded_bytes =
        assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
            .await
            + assert_audio_packet_forwarded(
                &mut publisher,
                &mut subscriber,
                &mut source,
                &mut clock,
            )
            .await;

    let before_live_metrics = wait_for_live_rtc_metrics(&server, 2).await;
    assert!(before_live_metrics.is_some());
    let Some(before_live_metrics) = before_live_metrics else {
        return;
    };
    assert_initial_live_rtc_metrics(&before_live_metrics, initial_forwarded_bytes);

    let mut additional_forwarded_bytes = 0;
    for _ in 0..4 {
        additional_forwarded_bytes +=
            assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
                .await;
    }

    let during_live_metrics = wait_for_live_rtc_metrics(&server, 2).await;
    assert!(during_live_metrics.is_some());
    let Some(during_live_metrics) = during_live_metrics else {
        return;
    };

    assert_steady_state_live_rtc_metrics(
        &before_live_metrics,
        &during_live_metrics,
        additional_forwarded_bytes,
    );

    assert!(publisher.close().await.is_some());
    assert!(subscriber.close().await.is_some());

    let after_live_metrics = wait_for_live_rtc_metrics(&server, 0).await;
    assert!(after_live_metrics.is_some());
    let Some(after_live_metrics) = after_live_metrics else {
        return;
    };

    assert_eq!(after_live_metrics.connected_transport_users, 0);
    assert_eq!(after_live_metrics.disconnected_transport_users, 0);
    assert_eq!(
        after_live_metrics.transport_health_transitions_connected_to_unset
            - during_live_metrics.transport_health_transitions_connected_to_unset,
        2
    );
}
