use super::support::*;

#[tokio::test]
async fn fake_rtc_peer_media_updates_room_stats_deterministically() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers("issuer-d", UserId::Integer(60), UserId::Integer(61)).await?;

    let mut source = FakeMediaSource::audio();
    publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(60),
        &source,
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
    let stats = require_some(stats, "audio bitrate should become observable")?;
    assert!(stats.audio > 0);
    assert!(stats.total >= stats.audio);
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_export_longer_transport_lifetimes_after_steady_state_run() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        publisher,
        subscriber,
        ..
    } = ready_room_fake_peers(
        "issuer-lifetime-metrics",
        UserId::Integer(62),
        UserId::Integer(63),
    )
    .await?;

    sleep(Duration::from_millis(1_200)).await;

    require_some(publisher.close().await, "publisher should close")?;
    require_some(subscriber.close().await, "subscriber should close")?;

    let lifetime_metrics = wait_for_transport_lifetime_metrics(&server, 2).await;
    let lifetime_metrics = require_some(
        lifetime_metrics,
        "transport lifetime metrics should include both peers",
    )?;

    assert_eq!(lifetime_metrics.le_1_second, 0);
    assert_eq!(lifetime_metrics.le_10_seconds, 2);
    assert_eq!(lifetime_metrics.le_60_seconds, 2);
    assert_eq!(lifetime_metrics.le_300_seconds, 2);
    assert_eq!(lifetime_metrics.count, 2);
    assert!(lifetime_metrics.sum_seconds >= 2.0);
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_export_transport_and_rtp_metrics_during_live_media() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-live-metrics",
        UserId::Integer(64),
        UserId::Integer(65),
    )
    .await?;

    let mut source = FakeMediaSource::audio();
    publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(64),
        &source,
    )
    .await;

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
    let before_live_metrics = require_some(
        before_live_metrics,
        "live metrics should include both peers",
    )?;
    assert_initial_live_rtc_metrics(&before_live_metrics, initial_forwarded_bytes);

    let mut additional_forwarded_bytes = 0;
    for _ in 0..4 {
        additional_forwarded_bytes +=
            assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
                .await;
    }

    let during_live_metrics = wait_for_live_rtc_metrics(&server, 2).await;
    let during_live_metrics =
        require_some(during_live_metrics, "live metrics should retain both peers")?;

    assert_steady_state_live_rtc_metrics(
        &before_live_metrics,
        &during_live_metrics,
        additional_forwarded_bytes,
    );

    require_some(publisher.close().await, "publisher should close")?;
    require_some(subscriber.close().await, "subscriber should close")?;

    let after_live_metrics = wait_for_live_rtc_metrics(&server, 0).await;
    let after_live_metrics =
        require_some(after_live_metrics, "live metrics should drain after close")?;

    assert_eq!(after_live_metrics.connected_transport_users, 0);
    assert_eq!(after_live_metrics.disconnected_transport_users, 0);
    assert_eq!(
        after_live_metrics.transport_health_transitions_connected_to_unset
            - during_live_metrics.transport_health_transitions_connected_to_unset,
        2
    );
    Ok(())
}
