use super::fixtures::*;
use crate::engine::media_transport::SourcePacketGate;

#[tokio::test]
async fn rtc_transport_bootstrap_starts_packet_loop() {
    let adapter = RtcWorker::default();
    assert!(!adapter.packet_loop_started());
    let session_key = transport_key(1, 15, UserId::Integer(15));
    let bootstrap_result = adapter.create_initial_session_offer(&session_key).await;
    assert!(bootstrap_result.is_ok());
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started());
}

#[tokio::test]
async fn rtc_metrics_track_live_transport_users_without_double_counting() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 16, UserId::Integer(16));

    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 0);
    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 1);

    assert!(matches!(
        adapter.create_initial_session_offer(&session_key).await,
        Err(TransportAdapterError::InvalidInput)
    ));
    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 1);

    assert!(adapter.close_session(&session_key).await.is_ok());
    let snapshot = adapter.metrics.snapshot();
    assert_eq!(snapshot.active_transport_users(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 1);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
}

#[tokio::test]
async fn rtc_publish_media_uses_signaled_mid_and_ssrc() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 18, UserId::Integer(18));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 42_424);
    let bootstrap_result = adapter.create_initial_session_offer(&session_key).await;
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
async fn rtc_session_bootstrap_applies_configured_outgoing_bitrate_cap() {
    let adapter = rtc_with_bitrate_limits(Bitrate::from_kbps(1_500), Bitrate::from_kbps(2_500));
    let session_key = transport_key(1, 181, UserId::Integer(181));

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter.debug_session_max_bitrate_out(&session_key).await,
        Some(Bitrate::from_kbps(2_500))
    );
}

#[tokio::test]
async fn rtc_receiver_bwe_target_update_writes_session_state() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 183, UserId::Integer(183));
    expect_initial_offer(&adapter, &session_key).await;
    let update = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::from_kbps(850));

    let results = adapter
        .set_receiver_bwe_targets(slice::from_ref(&update))
        .await
        .expect("receiver BWE target command should reach the worker");

    assert_eq!(results, vec![Ok(())]);
    assert_eq!(
        adapter
            .debug_session_receiver_bwe_target(&session_key)
            .await,
        Some(Bitrate::from_kbps(850))
    );
}

#[tokio::test]
async fn rtc_receiver_bwe_target_update_dedupes_identical_targets() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 184, UserId::Integer(184));
    expect_initial_offer(&adapter, &session_key).await;
    let update = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::from_kbps(640));

    let first_results = adapter
        .set_receiver_bwe_targets(slice::from_ref(&update))
        .await
        .expect("first receiver BWE target command should reach the worker");
    let second_results = adapter
        .set_receiver_bwe_targets(slice::from_ref(&update))
        .await
        .expect("second receiver BWE target command should reach the worker");

    assert_eq!(first_results, vec![Ok(())]);
    assert_eq!(second_results, vec![Ok(())]);
    assert_eq!(
        adapter
            .debug_session_receiver_bwe_str0m_update_count(&session_key)
            .await,
        Some(1)
    );
}

#[tokio::test]
async fn rtc_receiver_bwe_target_update_caps_at_max_bitrate_out() {
    let adapter = rtc_with_bitrate_limits(Bitrate::from_mbps(8), Bitrate::from_kbps(500));
    let session_key = transport_key(1, 185, UserId::Integer(185));
    expect_initial_offer(&adapter, &session_key).await;
    let update = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::from_kbps(900));

    let results = adapter
        .set_receiver_bwe_targets(slice::from_ref(&update))
        .await
        .expect("receiver BWE target command should reach the worker");

    assert_eq!(results, vec![Ok(())]);
    assert_eq!(
        adapter
            .debug_session_receiver_bwe_target(&session_key)
            .await,
        Some(Bitrate::from_kbps(500))
    );
}

#[tokio::test]
async fn rtc_receiver_bwe_target_update_missing_session_returns_error() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 186, UserId::Integer(186));
    let update = ReceiverBweTargetUpdate::new(session_key, Bitrate::from_kbps(400));

    let results = adapter
        .set_receiver_bwe_targets(slice::from_ref(&update))
        .await
        .expect("missing session result should still return through the worker");

    assert!(matches!(
        results.as_slice(),
        [Err(TransportAdapterError::InvalidInput)]
    ));
}

#[tokio::test]
async fn rtc_receiver_bwe_zero_target_clears_previous_target() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 187, UserId::Integer(187));
    expect_initial_offer(&adapter, &session_key).await;
    let non_zero = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::from_kbps(700));
    let zero = ReceiverBweTargetUpdate::new(session_key.clone(), Bitrate::zero());

    assert!(
        adapter
            .set_receiver_bwe_targets(slice::from_ref(&non_zero))
            .await
            .expect("non-zero target command should reach the worker")[0]
            .is_ok()
    );
    assert!(
        adapter
            .set_receiver_bwe_targets(slice::from_ref(&zero))
            .await
            .expect("zero target command should reach the worker")[0]
            .is_ok()
    );

    assert_eq!(
        adapter
            .debug_session_receiver_bwe_target(&session_key)
            .await,
        Some(Bitrate::zero())
    );
}

#[tokio::test]
async fn rtc_recv_media_applies_configured_incoming_bitrate_cap() {
    let adapter =
        rtc_with_bitrate_limits(Bitrate::from_bps(1_234_567), Bitrate::from_bps(7_654_321));
    let session_key = transport_key(1, 182, UserId::Integer(182));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 52_525);

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter.debug_session_max_bitrate_in(&session_key).await,
        Some(Bitrate::from_bps(1_234_567))
    );
}

#[tokio::test]
async fn rtc_consume_media_uses_negotiated_mid_and_ssrc() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 19, UserId::Integer(19));
    let consumer_session_key = transport_key(1, 20, UserId::Integer(20));
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_000);

    assert!(
        adapter
            .create_initial_session_offer(&producer_session_key)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .create_initial_session_offer(&consumer_session_key)
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

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Audio,
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &consumer_rtp_parameters,
            true,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };

    let expected_dest_mid: Mid = "aud-down".into();
    let route_entry = adapter.debug_route_entry_by_media_id(source_media_id).await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert_eq!(route_entry.source_transport_media_id, source_media_id);
    assert!(route_entry.source_active);
    assert_eq!(route_entry.active_destination_count, 1);
    assert!(route_entry.destinations.iter().any(|dest| {
        dest.dest_session == consumer_session_key
            && dest.dest_transport_media_id == consumer_media_id
            && dest.dest_mid == expected_dest_mid
    }));
    assert_eq!(
        adapter
            .debug_session_stream_tx_ssrc(&consumer_session_key, expected_dest_mid)
            .await,
        Some(61_000)
    );
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consume_media_can_start_route_inactive() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 221, UserId::Integer(221));
    let consumer_session_key = transport_key(1, 222, UserId::Integer(222));
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_100);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_100);

    assert!(
        adapter
            .create_initial_session_offer(&producer_session_key)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .create_initial_session_offer(&consumer_session_key)
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Audio,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should be accepted");
    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Audio,
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &consumer_rtp_parameters,
            false,
        )
        .await
        .expect("consumer media should be accepted");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("source route should exist");
    assert_eq!(route_entry.active_destination_count, 0);
    assert!(route_entry.destinations.iter().any(|dest| {
        dest.dest_session == consumer_session_key
            && dest.dest_transport_media_id == consumer_media_id
            && !dest.active
    }));
}

#[tokio::test]
async fn rtc_consumer_rid_policy_waits_for_live_rid_before_strict_aggregate_gate() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 21, UserId::Integer(21));
    let first_consumer_session_key = transport_key(1, 22, UserId::Integer(22));
    let second_consumer_session_key = transport_key(1, 23, UserId::Integer(23));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 71_000);
    let selected_consumer_rtp_parameters =
        sample_router_rtp_parameters_with_rid("vid-down-1", 72_000, "hi");
    let open_consumer_rtp_parameters = sample_router_rtp_parameters("vid-down-2", 73_000);

    for session_key in [
        &producer_session_key,
        &first_consumer_session_key,
        &second_consumer_session_key,
    ] {
        assert!(
            adapter
                .create_initial_session_offer(session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let _first_consumer_media_id = adapter
        .add_send_media(
            &first_consumer_session_key,
            Str0mMediaKind::Video,
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &selected_consumer_rtp_parameters,
            true,
        )
        .await
        .expect("selected-rid consumer should register");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should exist after first consumer registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    let _second_consumer_media_id = adapter
        .add_send_media(
            &second_consumer_session_key,
            Str0mMediaKind::Video,
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &open_consumer_rtp_parameters,
            true,
        )
        .await
        .expect("open consumer should register");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should still exist after mixed policy registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consumer_packet_gate_update_waits_for_live_rid_before_strict_aggregate_gate() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 123, UserId::Integer(123));
    let consumer_session_key = transport_key(1, 124, UserId::Integer(124));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 81_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters_with_rid("vid-down", 82_000, "hi");

    for session_key in [&producer_session_key, &consumer_session_key] {
        assert!(
            adapter
                .create_initial_session_offer(session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &consumer_rtp_parameters,
            true,
        )
        .await
        .expect("consumer media should register");

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should exist after consumer registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    let route = transport_consumer_route(
        &consumer_session_key,
        consumer_media_id,
        &producer_session_key,
        source_media_id,
    );
    assert!(
        adapter
            .set_consumer_packet_gate(&route, SourcePacketGate::Rid("lo".into()))
            .await
            .is_ok()
    );

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should still exist after consumer gate update");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    assert!(
        adapter
            .set_consumer_packet_gate(&route, SourcePacketGate::Open)
            .await
            .is_ok()
    );

    let route_entry = adapter
        .debug_route_entry_by_media_id(source_media_id)
        .await
        .expect("route entry should still exist after opening the consumer gate");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consumer_packet_gate_rejects_stale_source_owner() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 125, UserId::Integer(125));
    let stale_producer_session_key = transport_key(1, 126, UserId::Integer(125));
    let consumer_session_key = transport_key(1, 127, UserId::Integer(127));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 83_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("vid-down", 84_000);

    for session_key in [&producer_session_key, &consumer_session_key] {
        assert!(
            adapter
                .create_initial_session_offer(session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &consumer_rtp_parameters,
            true,
        )
        .await
        .expect("consumer media should register");

    assert!(
        adapter
            .set_consumer_packet_gate(
                &transport_consumer_route(
                    &consumer_session_key,
                    consumer_media_id,
                    &stale_producer_session_key,
                    source_media_id,
                ),
                SourcePacketGate::Rid("lo".into()),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rtc_route_activity_updates_producer_and_consumer_flags() {
    let adapter = RtcWorker::default();
    let producer_session_key = transport_key(1, 23, UserId::Integer(23));
    let consumer_session_key = transport_key(1, 24, UserId::Integer(24));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 91_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("vid-down", 92_000);

    assert!(
        adapter
            .create_initial_session_offer(&producer_session_key)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .create_initial_session_offer(&consumer_session_key)
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
            RtcSendMediaSource::local(&producer_session_key, source_media_id),
            &consumer_rtp_parameters,
            true,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };
    let source = TransportSourceKey::new(producer_session_key.clone(), source_media_id);

    assert!(adapter.set_producer_active(&source, false).await.is_ok());
    assert!(
        adapter
            .set_consumer_active(
                &transport_consumer_route(
                    &consumer_session_key,
                    consumer_media_id,
                    &producer_session_key,
                    source_media_id,
                ),
                false,
            )
            .await
            .is_ok()
    );

    let route_entry = adapter.debug_route_entry_by_media_id(source_media_id).await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert_eq!(route_entry.source_transport_media_id, source_media_id);
    assert!(!route_entry.source_active);
    assert_eq!(route_entry.active_destination_count, 0);
    assert!(route_entry.destinations.iter().any(|destination| {
        destination.dest_session == consumer_session_key
            && destination.dest_transport_media_id == consumer_media_id
            && destination.dest_mid == Mid::from("vid-down")
            && !destination.active
    }));
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_counts_recent_media_bytes() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 21, UserId::Integer(21));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 77_777);

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
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
    assert_eq!(snapshot.total, Bitrate::from_bps(960));
    assert_eq!(snapshot.per_media.len(), 1);
    assert_eq!(
        snapshot
            .per_media
            .first()
            .expect("should have media bitrate"),
        &(transport_media_id, Bitrate::from_bps(960))
    );
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_expires_after_one_second() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 22, UserId::Integer(22));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 88_888);

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
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
        let Ok(bitrate_registry) = worker_handle.bitrate_registry.lock() else {
            return;
        };
        bitrate_registry.transport_bitrate_snapshot_at(
            slice::from_ref(&session_key),
            now + Duration::from_secs(2),
        )
    };
    assert_eq!(snapshot.total, Bitrate::zero());
    assert!(snapshot.per_media.is_empty());
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_ignores_closed_sessions() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 23, UserId::Integer(23));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 99_999);

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
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
    assert_eq!(
        adapter
            .transport_bitrate_snapshot(slice::from_ref(&session_key))
            .total,
        Bitrate::from_bps(960)
    );

    assert!(adapter.close_session(&session_key).await.is_ok());

    let snapshot = adapter.transport_bitrate_snapshot(slice::from_ref(&session_key));
    assert_eq!(snapshot.total, Bitrate::zero());
    assert!(snapshot.per_media.is_empty());
}

#[tokio::test]
async fn rtc_active_speaker_source_snapshot_orders_recent_audio_sources() {
    let adapter = RtcWorker::default();
    let first_session_key = transport_key(9, 31, UserId::Integer(31));
    let second_session_key = transport_key(9, 32, UserId::Integer(32));
    let first_rtp_parameters = sample_router_rtp_parameters("aud-up-1", 93_001);
    let second_rtp_parameters = sample_router_rtp_parameters("aud-up-2", 93_002);

    for session_key in [&first_session_key, &second_session_key] {
        assert!(
            adapter
                .create_initial_session_offer(session_key)
                .await
                .is_ok()
        );
    }

    let first_media_id = adapter
        .add_recv_media(
            &first_session_key,
            Str0mMediaKind::Audio,
            &first_rtp_parameters,
        )
        .await
        .expect("first audio media should register");
    let second_media_id = adapter
        .add_recv_media(
            &second_session_key,
            Str0mMediaKind::Audio,
            &second_rtp_parameters,
        )
        .await
        .expect("second audio media should register");

    let now = Instant::now();
    adapter
        .debug_observe_audio_activity(first_media_id, Some(true), None, now)
        .await;
    adapter
        .debug_observe_audio_activity(
            second_media_id,
            Some(true),
            None,
            now + Duration::from_millis(10),
        )
        .await;

    let snapshot = adapter.active_speaker_source_snapshot().await;
    assert_eq!(
        snapshot
            .into_iter()
            .map(ActiveSpeakerSource::transport_media_id)
            .collect::<Vec<_>>(),
        vec![second_media_id, first_media_id]
    );
}

#[tokio::test]
async fn rtc_active_speaker_deadline_tracks_the_current_hold_window() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(9, 41, UserId::Integer(41));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 94_001);

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );

    let media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await
        .expect("audio media should register");
    let observed_at = Instant::now();
    adapter
        .debug_observe_audio_activity(media_id, Some(true), None, observed_at)
        .await;

    let next_deadline = adapter
        .next_active_speaker_deadline()
        .await
        .expect("speaker activity should schedule a hold-window expiry");
    assert!(next_deadline >= observed_at + Duration::from_millis(200));
    assert!(next_deadline <= observed_at + Duration::from_millis(300));

    sleep(Duration::from_millis(300)).await;

    assert_eq!(adapter.next_active_speaker_deadline().await, None);
}

#[tokio::test]
async fn rtc_relay_route_api_registers_and_removes_target_mailboxes() {
    let source_adapter = RtcWorker::default();
    let target_adapter = RtcWorker::default();
    let source_session = transport_key(91, 91, UserId::Integer(91));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 91_091);

    assert!(
        source_adapter
            .create_initial_session_offer(&source_session)
            .await
            .is_ok()
    );
    let source_transport_media_id = source_adapter
        .add_recv_media(&source_session, Str0mMediaKind::Audio, &rtp_parameters)
        .await;
    assert!(source_transport_media_id.is_ok());
    let Some(source_transport_media_id) = source_transport_media_id.ok() else {
        return;
    };
    let source = TransportSourceKey::new(source_session.clone(), source_transport_media_id);

    assert!(
        source_adapter
            .activate_relay_route(&source, &target_adapter)
            .await
            .is_ok()
    );
    assert_eq!(
        source_adapter
            .debug_relay_target_count(source_transport_media_id)
            .await,
        1
    );
    assert_eq!(
        source_adapter
            .debug_active_relay_target_count(source_transport_media_id)
            .await,
        0
    );

    assert!(
        source_adapter
            .apply_relay_target_activity(&source, &target_adapter, true)
            .await
            .is_ok()
    );
    assert_eq!(
        source_adapter
            .debug_active_relay_target_count(source_transport_media_id)
            .await,
        1
    );

    assert!(
        source_adapter
            .deactivate_relay_route(source_transport_media_id, &target_adapter)
            .await
            .is_ok()
    );
    assert_eq!(
        source_adapter
            .debug_relay_target_count(source_transport_media_id)
            .await,
        0
    );
}
