use super::fixtures::*;
use crate::runtime::media_transport::SourcePacketGate;

#[tokio::test]
async fn rtc_transport_bootstrap_starts_packet_loop() {
    let adapter = RtcTransportShard::default();
    assert!(!adapter.packet_loop_started());
    let session_key = transport_key(1, 15, UserId::Integer(15));
    let bootstrap_result = prepare_transport_session(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started());
}

#[tokio::test]
async fn rtc_metrics_track_live_transport_users_without_double_counting() {
    let adapter = RtcTransportShard::default();
    let session_key = transport_key(1, 16, UserId::Integer(16));

    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 0);
    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 1);

    assert!(matches!(
        adapter
            .negotiation()
            .create_initial_session_offer(&session_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    ));
    assert_eq!(adapter.metrics.snapshot().active_transport_users(), 1);

    assert!(
        adapter
            .users()
            .close_session_with_outcome(&session_key)
            .await
            .is_ok()
    );
    let snapshot = adapter.metrics.snapshot();
    assert_eq!(snapshot.active_transport_users(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 1);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
}

#[tokio::test]
async fn rtc_publish_media_uses_signaled_mid_and_ssrc() {
    let adapter = RtcTransportShard::default();
    let session_key = transport_key(1, 18, UserId::Integer(18));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 42_424);
    let bootstrap_result = prepare_transport_session(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());

    let transport_media_id = adapter
        .media()
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await;
    assert!(transport_media_id.is_ok());
    let Some(transport_media_id) = transport_media_id.ok() else {
        return;
    };

    let expected_mid: Mid = "aud-up".into();
    assert_eq!(
        resolve_mid(&adapter, transport_media_id).await,
        Some(expected_mid)
    );
    assert_eq!(
        session_stream_rx_ssrc(&adapter, &session_key, expected_mid).await,
        Some(42_424)
    );
}

#[tokio::test]
async fn rtc_session_bootstrap_applies_configured_outgoing_bitrate_cap() {
    let adapter =
        rtc_engine_with_bitrate_limits(Bitrate::from_kbps(1_500), Bitrate::from_kbps(2_500));
    let session_key = transport_key(1, 181, UserId::Integer(181));

    assert!(
        adapter
            .negotiation()
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert_eq!(
        session_max_bitrate_out(&adapter, &session_key).await,
        Some(Bitrate::from_kbps(2_500))
    );
}

#[tokio::test]
async fn rtc_recv_media_applies_configured_incoming_bitrate_cap() {
    let adapter =
        rtc_engine_with_bitrate_limits(Bitrate::from_bps(1_234_567), Bitrate::from_bps(7_654_321));
    let session_key = transport_key(1, 182, UserId::Integer(182));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 52_525);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .media()
            .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
            .await
            .is_ok()
    );
    assert_eq!(
        session_max_bitrate_in(&adapter, &session_key).await,
        Some(Bitrate::from_bps(1_234_567))
    );
}

#[tokio::test]
async fn rtc_consume_media_uses_negotiated_mid_and_ssrc() {
    let adapter = RtcTransportShard::default();
    let producer_session_key = transport_key(1, 19, UserId::Integer(19));
    let consumer_session_key = transport_key(1, 20, UserId::Integer(20));
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_000);

    assert!(
        prepare_transport_session(&adapter, &producer_session_key)
            .await
            .is_ok()
    );
    assert!(
        prepare_transport_session(&adapter, &consumer_session_key)
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .media()
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
        .media()
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Audio,
            &producer_session_key,
            source_media_id,
            None,
            &consumer_rtp_parameters,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };

    let expected_dest_mid: Mid = "aud-down".into();
    let route_entry = route_entry_by_media_id(&adapter, source_media_id).await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert_eq!(route_entry.source_transport_media_id, source_media_id);
    assert!(route_entry.source_active);
    assert!(route_entry.destinations.iter().any(|dest| {
        dest.dest_session == consumer_session_key
            && dest.dest_transport_media_id == consumer_media_id
            && dest.dest_mid == expected_dest_mid
    }));
    assert_eq!(
        session_stream_tx_ssrc(&adapter, &consumer_session_key, expected_dest_mid).await,
        Some(61_000)
    );
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consumer_rid_policy_waits_for_live_rid_before_strict_aggregate_gate() {
    let adapter = RtcTransportShard::default();
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
            prepare_transport_session(&adapter, session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .media()
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let _first_consumer_media_id = adapter
        .media()
        .add_send_media(
            &first_consumer_session_key,
            Str0mMediaKind::Video,
            &producer_session_key,
            source_media_id,
            None,
            &selected_consumer_rtp_parameters,
        )
        .await
        .expect("selected-rid consumer should register");

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should exist after first consumer registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    let _second_consumer_media_id = adapter
        .media()
        .add_send_media(
            &second_consumer_session_key,
            Str0mMediaKind::Video,
            &producer_session_key,
            source_media_id,
            None,
            &open_consumer_rtp_parameters,
        )
        .await
        .expect("open consumer should register");

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should still exist after mixed policy registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consumer_packet_gate_update_waits_for_live_rid_before_strict_aggregate_gate() {
    let adapter = RtcTransportShard::default();
    let producer_session_key = transport_key(1, 123, UserId::Integer(123));
    let consumer_session_key = transport_key(1, 124, UserId::Integer(124));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 81_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters_with_rid("vid-down", 82_000, "hi");

    for session_key in [&producer_session_key, &consumer_session_key] {
        assert!(
            prepare_transport_session(&adapter, session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .media()
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let consumer_media_id = adapter
        .media()
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &producer_session_key,
            source_media_id,
            None,
            &consumer_rtp_parameters,
        )
        .await
        .expect("consumer media should register");

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should exist after consumer registration");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    assert!(
        adapter
            .media()
            .set_consumer_packet_gate(
                &consumer_session_key,
                consumer_media_id,
                &producer_session_key,
                source_media_id,
                SourcePacketGate::Rid("lo".into()),
            )
            .await
            .is_ok()
    );

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should still exist after consumer gate update");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);

    assert!(
        adapter
            .media()
            .set_consumer_packet_gate(
                &consumer_session_key,
                consumer_media_id,
                &producer_session_key,
                source_media_id,
                SourcePacketGate::Open,
            )
            .await
            .is_ok()
    );

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should still exist after opening the consumer gate");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Open);
}

#[tokio::test]
async fn rtc_consumer_packet_gate_rejects_stale_source_owner() {
    let adapter = RtcTransportShard::default();
    let producer_session_key = transport_key(1, 125, UserId::Integer(125));
    let stale_producer_session_key = transport_key(1, 126, UserId::Integer(125));
    let consumer_session_key = transport_key(1, 127, UserId::Integer(127));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 83_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("vid-down", 84_000);

    for session_key in [&producer_session_key, &consumer_session_key] {
        assert!(
            prepare_transport_session(&adapter, session_key)
                .await
                .is_ok()
        );
    }

    let source_media_id = adapter
        .media()
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let consumer_media_id = adapter
        .media()
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &producer_session_key,
            source_media_id,
            None,
            &consumer_rtp_parameters,
        )
        .await
        .expect("consumer media should register");

    assert!(
        adapter
            .media()
            .set_consumer_packet_gate(
                &consumer_session_key,
                consumer_media_id,
                &stale_producer_session_key,
                source_media_id,
                SourcePacketGate::Rid("lo".into()),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rtc_route_activity_updates_producer_and_consumer_flags() {
    let adapter = RtcTransportShard::default();
    let producer_session_key = transport_key(1, 23, UserId::Integer(23));
    let consumer_session_key = transport_key(1, 24, UserId::Integer(24));
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 91_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("vid-down", 92_000);

    assert!(
        prepare_transport_session(&adapter, &producer_session_key)
            .await
            .is_ok()
    );
    assert!(
        prepare_transport_session(&adapter, &consumer_session_key)
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .media()
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
        .media()
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &producer_session_key,
            source_media_id,
            None,
            &consumer_rtp_parameters,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };

    assert!(
        adapter
            .media()
            .set_producer_active(&producer_session_key, source_media_id, false)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .media()
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

    let route_entry = route_entry_by_media_id(&adapter, source_media_id).await;
    assert!(route_entry.is_some());
    let Some(route_entry) = route_entry else {
        return;
    };
    assert_eq!(route_entry.source_transport_media_id, source_media_id);
    assert!(!route_entry.source_active);
    assert!(route_entry.destinations.iter().any(|destination| {
        destination.dest_session == consumer_session_key
            && destination.dest_transport_media_id == consumer_media_id
            && destination.dest_mid == Mid::from("vid-down")
            && !destination.active
    }));
}

#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_counts_recent_media_bytes() {
    let adapter = RtcTransportShard::default();
    let session_key = transport_key(1, 21, UserId::Integer(21));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 77_777);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    let transport_media_id = adapter
        .media()
        .add_recv_media(&session_key, Str0mMediaKind::Video, &rtp_parameters)
        .await
        .expect("should declare recv media");

    record_incoming_media(
        &adapter,
        &session_key,
        transport_media_id,
        120,
        Instant::now(),
    )
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
    let adapter = RtcTransportShard::default();
    let session_key = transport_key(1, 22, UserId::Integer(22));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 88_888);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    let transport_media_id = adapter
        .media()
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await
        .expect("should declare recv media");

    let now = Instant::now();
    record_incoming_media(&adapter, &session_key, transport_media_id, 64, now).await;
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
    let adapter = RtcTransportShard::default();
    let session_key = transport_key(1, 23, UserId::Integer(23));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 99_999);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    let transport_media_id = adapter
        .media()
        .add_recv_media(&session_key, Str0mMediaKind::Video, &rtp_parameters)
        .await
        .expect("should declare recv media");

    record_incoming_media(
        &adapter,
        &session_key,
        transport_media_id,
        120,
        Instant::now(),
    )
    .await;
    assert_eq!(
        adapter
            .transport_bitrate_snapshot(slice::from_ref(&session_key))
            .total,
        Bitrate::from_bps(960)
    );

    assert!(
        adapter
            .users()
            .close_session_with_outcome(&session_key)
            .await
            .is_ok()
    );

    let snapshot = adapter.transport_bitrate_snapshot(slice::from_ref(&session_key));
    assert_eq!(snapshot.total, Bitrate::zero());
    assert!(snapshot.per_media.is_empty());
}

#[tokio::test]
async fn rtc_active_speaker_source_snapshot_orders_recent_audio_sources() {
    let adapter = RtcTransportShard::default();
    let first_session_key = transport_key(9, 31, UserId::Integer(31));
    let second_session_key = transport_key(9, 32, UserId::Integer(32));
    let first_rtp_parameters = sample_router_rtp_parameters("aud-up-1", 93_001);
    let second_rtp_parameters = sample_router_rtp_parameters("aud-up-2", 93_002);

    for session_key in [&first_session_key, &second_session_key] {
        assert!(
            prepare_transport_session(&adapter, session_key)
                .await
                .is_ok()
        );
    }

    let first_media_id = adapter
        .media()
        .add_recv_media(
            &first_session_key,
            Str0mMediaKind::Audio,
            &first_rtp_parameters,
        )
        .await
        .expect("first audio media should register");
    let second_media_id = adapter
        .media()
        .add_recv_media(
            &second_session_key,
            Str0mMediaKind::Audio,
            &second_rtp_parameters,
        )
        .await
        .expect("second audio media should register");

    let now = Instant::now();
    observe_audio_activity(&adapter, first_media_id, Some(true), None, now).await;
    observe_audio_activity(
        &adapter,
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
    let adapter = RtcTransportShard::default();
    let session_key = transport_key(9, 41, UserId::Integer(41));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 94_001);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );

    let media_id = adapter
        .media()
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await
        .expect("audio media should register");
    let observed_at = Instant::now();
    observe_audio_activity(&adapter, media_id, Some(true), None, observed_at).await;

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
async fn rtc_relay_route_facade_registers_and_removes_target_mailboxes() {
    let source_adapter = RtcTransportShard::default();
    let target_adapter = RtcTransportShard::default();
    let source_session = transport_key(91, 91, UserId::Integer(91));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 91_091);

    assert!(
        prepare_transport_session(&source_adapter, &source_session)
            .await
            .is_ok()
    );
    let source_transport_media_id = source_adapter
        .media()
        .add_recv_media(&source_session, Str0mMediaKind::Audio, &rtp_parameters)
        .await;
    assert!(source_transport_media_id.is_ok());
    let Some(source_transport_media_id) = source_transport_media_id.ok() else {
        return;
    };

    assert!(
        source_adapter
            .media()
            .activate_relay_route(&source_session, source_transport_media_id, &target_adapter)
            .await
            .is_ok()
    );
    assert_eq!(
        relay_target_count_for_source(&source_adapter, source_transport_media_id).await,
        1
    );
    assert_eq!(
        active_relay_target_count_for_source(&source_adapter, source_transport_media_id).await,
        0
    );

    assert!(
        source_adapter
            .media()
            .apply_relay_target_activity(
                &source_session,
                source_transport_media_id,
                &target_adapter,
                true,
            )
            .await
            .is_ok()
    );
    assert_eq!(
        active_relay_target_count_for_source(&source_adapter, source_transport_media_id).await,
        1
    );

    assert!(
        source_adapter
            .media()
            .deactivate_relay_route(source_transport_media_id, &target_adapter)
            .await
            .is_ok()
    );
    assert_eq!(
        relay_target_count_for_source(&source_adapter, source_transport_media_id).await,
        0
    );
}
