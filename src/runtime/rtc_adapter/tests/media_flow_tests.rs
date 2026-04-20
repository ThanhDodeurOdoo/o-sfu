use super::fixtures::*;
use crate::runtime::transport_adapter::SourcePacketGate;

#[tokio::test]
async fn rtc_transport_bootstrap_starts_packet_loop() {
    let adapter = RtcTransportAdapter::default();
    assert!(!adapter.packet_loop_started.load(Ordering::Acquire));
    let session_key = transport_key(1, 15, SessionId::Integer(15));
    let bootstrap_result = prepare_transport_session(&adapter, &session_key).await;
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
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    assert_eq!(adapter.metrics.snapshot().active_transport_sessions, 1);

    assert!(matches!(
        adapter.create_initial_session_offer(&session_key).await,
        Err(TransportAdapterError::InvalidInput)
    ));
    assert_eq!(adapter.metrics.snapshot().active_transport_sessions, 1);

    assert!(adapter.close_session(&session_key).await.is_ok());
    let snapshot = adapter.metrics.snapshot();
    assert_eq!(snapshot.active_transport_sessions, 0);
    assert_eq!(snapshot.transport_session_lifetime_le_1_second, 1);
    assert_eq!(snapshot.transport_session_lifetime_count, 1);
}

#[tokio::test]
async fn rtc_publish_media_uses_signaled_mid_and_ssrc() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 18, SessionId::Integer(18));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 42_424);
    let bootstrap_result = prepare_transport_session(&adapter, &session_key).await;
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
    let adapter = rtc_adapter_with_bitrate_limits(1_500_000, 2_500_000);
    let session_key = transport_key(1, 181, SessionId::Integer(181));

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert_eq!(
        session_max_bitrate_out(&adapter, &session_key).await,
        Some(2_500_000)
    );
}

#[tokio::test]
async fn rtc_recv_media_applies_configured_incoming_bitrate_cap() {
    let adapter = rtc_adapter_with_bitrate_limits(1_234_567, 7_654_321);
    let session_key = transport_key(1, 182, SessionId::Integer(182));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 52_525);

    assert!(
        prepare_transport_session(&adapter, &session_key)
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
        session_max_bitrate_in(&adapter, &session_key).await,
        Some(1_234_567)
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
async fn rtc_consumer_rid_policy_drives_the_source_packet_gate() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_key = transport_key(1, 21, SessionId::Integer(21));
    let first_consumer_session_key = transport_key(1, 22, SessionId::Integer(22));
    let second_consumer_session_key = transport_key(1, 23, SessionId::Integer(23));
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
    assert_eq!(
        route_entry.effective_packet_gate,
        DebugPacketGate::Rid(String::from("hi"))
    );

    let _second_consumer_media_id = adapter
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
async fn rtc_source_packet_gate_composes_with_consumer_policy() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_key = transport_key(1, 123, SessionId::Integer(123));
    let consumer_session_key = transport_key(1, 124, SessionId::Integer(124));
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
        .add_recv_media(
            &producer_session_key,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await
        .expect("producer media should register");

    let _consumer_media_id = adapter
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
            .set_source_packet_gate(
                &producer_session_key,
                source_media_id,
                Some(SourcePacketGate::Rid("hi".into())),
            )
            .await
            .is_ok()
    );

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should exist after source gate update");
    assert_eq!(
        route_entry.effective_packet_gate,
        DebugPacketGate::Rid(String::from("hi"))
    );

    assert!(
        adapter
            .set_source_packet_gate(
                &producer_session_key,
                source_media_id,
                Some(SourcePacketGate::Rid("lo".into())),
            )
            .await
            .is_ok()
    );

    let route_entry = route_entry_by_media_id(&adapter, source_media_id)
        .await
        .expect("route entry should still exist after conflicting source gate update");
    assert_eq!(route_entry.effective_packet_gate, DebugPacketGate::Block);
}

#[tokio::test]
async fn rtc_route_activity_updates_producer_and_consumer_flags() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_key = transport_key(1, 23, SessionId::Integer(23));
    let consumer_session_key = transport_key(1, 24, SessionId::Integer(24));
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
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 21, SessionId::Integer(21));
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 77_777);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    let transport_media_id = adapter
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
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );
    let transport_media_id = adapter
        .add_recv_media(&session_key, Str0mMediaKind::Audio, &rtp_parameters)
        .await
        .expect("should declare recv media");

    let now = Instant::now();
    record_incoming_media(&adapter, &session_key, transport_media_id, 64, now).await;
    let Some(worker_handle) = adapter.worker_handle().ok().flatten() else {
        return;
    };
    let snapshot = {
        let Ok(bitrate_state) = worker_handle.bitrate_state.lock() else {
            return;
        };
        bitrate_state.transport_bitrate_snapshot_at(
            slice::from_ref(&session_key),
            now + Duration::from_secs(2),
        )
    };
    assert_eq!(snapshot.total, 0);
    assert!(snapshot.per_media.is_empty());
}

#[tokio::test]
async fn rtc_active_speaker_source_snapshot_orders_recent_audio_sources() {
    let adapter = RtcTransportAdapter::default();
    let first_session_key = transport_key(9, 31, SessionId::Integer(31));
    let second_session_key = transport_key(9, 32, SessionId::Integer(32));
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
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(9, 41, SessionId::Integer(41));
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 94_001);

    assert!(
        prepare_transport_session(&adapter, &session_key)
            .await
            .is_ok()
    );

    let media_id = adapter
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
async fn rtc_debug_relay_route_helpers_register_and_remove_target_mailboxes() {
    let source_adapter = RtcTransportAdapter::default();
    let target_adapter = RtcTransportAdapter::default();
    let source_transport_media_id = TransportMediaId::new(91);

    assert!(
        activate_relay_route(&source_adapter, source_transport_media_id, &target_adapter,).is_ok()
    );
    assert_eq!(
        relay_target_count_for_source(&source_adapter, source_transport_media_id),
        1
    );
    assert_eq!(
        active_relay_target_count_for_source(&source_adapter, source_transport_media_id),
        0
    );

    source_adapter.set_relay_route_active(source_transport_media_id, &target_adapter, true);
    assert_eq!(
        active_relay_target_count_for_source(&source_adapter, source_transport_media_id),
        1
    );

    deactivate_relay_route(&source_adapter, source_transport_media_id, &target_adapter);
    assert_eq!(
        relay_target_count_for_source(&source_adapter, source_transport_media_id),
        0
    );
}
