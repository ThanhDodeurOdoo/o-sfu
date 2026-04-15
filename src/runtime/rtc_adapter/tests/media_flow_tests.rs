use super::fixtures::*;

#[tokio::test]
async fn rtc_transport_bootstrap_starts_packet_loop() {
    let adapter = RtcTransportAdapter::default();
    assert!(!adapter.packet_loop_started.load(Ordering::Acquire));
    let session_key = transport_key(1, 15, SessionId::Integer(15));
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));
}

#[tokio::test]
async fn rtc_metrics_track_live_transport_sessions_without_double_counting() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 16, SessionId::Integer(16));

    assert_eq!(adapter.metrics.snapshot().active_transport_sessions, 0);
    assert!(
        adapter
            .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert_eq!(adapter.metrics.snapshot().active_transport_sessions, 1);

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert_eq!(adapter.metrics.snapshot().active_transport_sessions, 1);

    assert!(adapter.close_session(&session_key).await.is_ok());
    assert_eq!(adapter.metrics.snapshot().active_transport_sessions, 0);
}

#[tokio::test]
async fn rtc_publish_media_uses_signaled_mid_and_ssrc() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 18, SessionId::Integer(18));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 42_424);
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());

    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await;
    assert!(transport_media_id.is_ok());
    let Some(transport_media_id) = transport_media_id.ok() else {
        return;
    };

    let expected_mid: Mid = "aud-up".into();
    assert_eq!(
        adapter.debug_resolve_mid(transport_media_id).await,
        Some(expected_mid)
    );
    assert_eq!(
        adapter
            .debug_session_stream_rx_ssrc(&session_key, expected_mid)
            .await,
        Some(42_424)
    );
}

#[tokio::test]
async fn rtc_consume_media_uses_negotiated_mid_and_ssrc() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_key = transport_key(1, 19, SessionId::Integer(19));
    let consumer_session_key = transport_key(1, 20, SessionId::Integer(20));
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_000);

    assert!(
        adapter
            .transport_bootstrap_payload(&producer_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert!(
        adapter
            .transport_bootstrap_payload(&consumer_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Audio,
            &producer_rtp_parameters,
        )
        .await;
    assert!(source_media_id.is_ok());
    let Some(source_media_id) = source_media_id.ok() else {
        return;
    };

    let result = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Audio,
            &producer_session_key,
            source_media_id,
            &consumer_rtp_parameters,
        )
        .await;
    assert!(result.is_ok());

    let expected_source_mid: Mid = "aud-up".into();
    let expected_dest_mid: Mid = "aud-down".into();
    let route_entry = adapter
        .debug_route_entry(&producer_session_key, expected_source_mid)
        .await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert!(route_entry.source_active);
    assert!(route_entry.destinations.iter().any(|dest| {
        dest.dest_session == consumer_session_key && dest.dest_mid == expected_dest_mid
    }));
    assert_eq!(
        adapter
            .debug_session_stream_tx_ssrc(&consumer_session_key, expected_dest_mid)
            .await,
        Some(61_000)
    );
}

#[tokio::test]
async fn rtc_route_activity_updates_producer_and_consumer_flags() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_key = transport_key(1, 23, SessionId::Integer(23));
    let consumer_session_key = transport_key(1, 24, SessionId::Integer(24));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 91_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("vid-down", 92_000);

    assert!(
        adapter
            .transport_bootstrap_payload(&producer_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert!(
        adapter
            .transport_bootstrap_payload(&consumer_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await;
    assert!(source_media_id.is_ok());
    let Some(source_media_id) = source_media_id.ok() else {
        return;
    };

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &producer_session_key,
            source_media_id,
            &consumer_rtp_parameters,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };

    assert!(
        adapter
            .set_producer_active(&producer_session_key, source_media_id, false)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .set_consumer_active(
                &consumer_session_key,
                consumer_media_id,
                &producer_session_key,
                source_media_id,
                false,
            )
            .await
            .is_ok()
    );

    let route_entry = adapter
        .debug_route_entry(&producer_session_key, Mid::from("vid-up"))
        .await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert!(!route_entry.source_active);
    assert!(route_entry.destinations.iter().any(|destination| {
        destination.dest_session == consumer_session_key
            && destination.dest_mid == Mid::from("vid-down")
            && !destination.active
    }));
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_counts_recent_media_bytes() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 21, SessionId::Integer(21));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 77_777);

    assert!(
        adapter
            .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Video, &rtp_parameters)
        .await
        .expect("should declare recv media");

    adapter
        .debug_record_incoming_media(&session_key, transport_media_id, 120, Instant::now())
        .await;

    let snapshot = adapter.transport_bitrate_snapshot(slice::from_ref(&session_key));
    assert_eq!(snapshot.per_media.len(), 1);
    assert_eq!(
        snapshot
            .per_media
            .first()
            .expect("should have media bitrate"),
        &(transport_media_id, 960)
    );
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_expires_after_one_second() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 22, SessionId::Integer(22));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 88_888);

    assert!(
        adapter
            .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await
        .expect("should declare recv media");

    let now = Instant::now();
    adapter
        .debug_record_incoming_media(&session_key, transport_media_id, 64, now)
        .await;
    let Some(worker_handle) = adapter.worker_handle().ok().flatten() else {
        return;
    };
    let snapshot = {
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return;
        };
        snapshot_state.transport_bitrate_snapshot_at(
            slice::from_ref(&session_key),
            now + Duration::from_secs(2),
        )
    };
    assert_eq!(snapshot.total, 0);
    assert!(snapshot.per_media.is_empty());
}
