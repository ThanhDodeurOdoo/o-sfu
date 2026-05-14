#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

use std::time::{Duration, Instant};

use o_sfu::{
    config::{Config, MediaCodecFlags, RoomShardingPolicy},
    core::{LocalSpilloverPolicy, LocalSpilloverPolicyParts},
    http::IncomingBitRateStatsResponse,
};
use o_sfu_protocol::{
    shared::{DownloadStates, StreamType, UserId, UserInfo, VideoLayoutIntent},
    signaling::{ServerMessage, ServerRequest, TrackBinding},
};
use o_sfu_telemetry::diagnostics::{DiagnosticsActiveSpeakerReason, DiagnosticsActiveSpeakerState};
use o_sfu_tests::support::{
    TEST_ROOM_KEY, TestServer, create_room,
    fake_media::{
        FakeClock, FakeMediaSource, SYNTHETIC_OPUS_ONE_FRAME_TOC, SyntheticH264Stream,
        SyntheticOpusStream, SyntheticVp8Stream,
    },
    metrics_text,
    protocol_full_stack::{
        ProtocolFakePeer, connect_fake_peer, connect_two_fake_peers,
        connect_two_rtc_ready_fake_peers,
    },
    spawn_room_server, spawn_room_server_with_config, spawn_test_server, stats, test_config,
};
use tokio::{
    sync::{Mutex, MutexGuard},
    task::yield_now,
    time::{sleep, timeout},
};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

static FULL_STACK_TEST_LOCK: Mutex<()> = Mutex::const_new(());

async fn full_stack_test_guard() -> MutexGuard<'static, ()> {
    FULL_STACK_TEST_LOCK.lock().await
}

#[test]
fn fake_media_source_uses_manual_clock_deterministically() {
    let mut clock = FakeClock::default();
    let mut source = FakeMediaSource::audio();

    let first = source.next_frame(&mut clock);
    let second = source.next_frame(&mut clock);

    assert_eq!(first.emitted_at, Duration::from_millis(20));
    assert_eq!(second.emitted_at, Duration::from_millis(40));
    assert_eq!(first.sequence_number, 0);
    assert_eq!(second.sequence_number, 1);
    assert_eq!(first.rtp_timestamp, 0);
    assert_eq!(second.rtp_timestamp, 960);
    assert_eq!(first.payload.len(), 160);
    assert_eq!(second.payload.len(), 160);
    assert_eq!(
        first.payload.first().copied(),
        Some(SYNTHETIC_OPUS_ONE_FRAME_TOC)
    );
    assert_eq!(first.extension_values.audio_level, Some(-32));
    assert_eq!(first.extension_values.voice_activity, Some(true));
}

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

#[tokio::test]
async fn fake_rtc_opus_vad_true_drives_active_speaker_diagnostics() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-opus-active-speaker").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(88),
        UserId::Integer(89),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(-32, true));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(88),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    assert!(
        server
            .wait_for_audio_source_active_speaker(
                &room,
                &UserId::Integer(88),
                DiagnosticsActiveSpeakerState::Active,
                DiagnosticsActiveSpeakerReason::Vad,
                Some(-32),
            )
            .await
    );
}

#[tokio::test]
async fn fake_rtc_cross_worker_opus_vad_true_forwards_and_drives_active_speaker() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-opus-active-speaker",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(188);
    let subscriber_user_id = UserId::Integer(189);

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(-32, true));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    assert!(
        server
            .wait_for_audio_source_active_speaker(
                &room,
                &publisher_user_id,
                DiagnosticsActiveSpeakerState::Active,
                DiagnosticsActiveSpeakerReason::Vad,
                Some(-32),
            )
            .await
    );
}

#[tokio::test]
async fn fake_rtc_opus_vad_false_blocks_audio_forwarding() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-opus-vad-false").await;
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
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(0, false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(86),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    assert!(
        server
            .wait_for_audio_source_active_speaker(
                &room,
                &UserId::Integer(86),
                DiagnosticsActiveSpeakerState::Blocked,
                DiagnosticsActiveSpeakerReason::VadFalse,
                Some(0),
            )
            .await
    );
}

#[tokio::test]
async fn fake_rtc_cross_worker_opus_vad_false_blocks_relay_fanout() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-opus-vad-false",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(186);
    let subscriber_user_id = UserId::Integer(187);

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(0, false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    assert!(
        server
            .wait_for_audio_source_active_speaker(
                &room,
                &publisher_user_id,
                DiagnosticsActiveSpeakerState::Blocked,
                DiagnosticsActiveSpeakerReason::VadFalse,
                Some(0),
            )
            .await
    );
}

#[tokio::test]
async fn fake_rtc_peers_forward_vp8_high_rid_keyframe_without_browsers() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-vp8-synthetic").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(92),
        UserId::Integer(93),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(92),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(92)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(92),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_cross_worker_vp8_selected_rid_survives_relay() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-vp8-selected-rid",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(182);
    let subscriber_user_id = UserId::Integer(183);

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut high_source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    assert!(publisher.publish_track(&high_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, publisher_user_id.clone()).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &publisher_user_id,
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;

    let mut low_source = FakeMediaSource::vp8_camera_with_rid("lo");
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut subscriber,
        &mut low_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_cross_worker_h264_selected_rid_requires_idr_after_relay() {
    let _guard = full_stack_test_guard().await;
    let mut config = cross_worker_test_config();
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let room_server = spawn_room_server_with_config(
        config,
        "issuer-cross-worker-h264-selected-rid",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(184);
    let subscriber_user_id = UserId::Integer(185);

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticH264Stream::with_idr(false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, publisher_user_id.clone()).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &publisher_user_id,
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock)
        .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_relays_vp8_after_threshold() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        load_triggered_spillover_test_config(),
        "issuer-load-spillover-vp8-selected-rid",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(190);
    let local_subscriber_user_id = UserId::Integer(191);
    let spillover_subscriber_user_id = UserId::Integer(192);

    let setup = Box::pin(connect_load_triggered_spillover_rtc_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id,
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, _local_subscriber, mut spillover_subscriber)) = setup else {
        return;
    };

    let mut high_source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    assert!(publisher.publish_track(&high_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut spillover_subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(
        spillover_subscriber
            .complete_next_negotiation()
            .await
            .is_some()
    );
    assert_video_subscription_enabled(&mut spillover_subscriber, publisher_user_id.clone()).await;
    assert_consumer_route_active(
        &server,
        &room,
        &spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                spillover_subscriber.user_id(),
                &publisher_user_id,
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut spillover_subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut spillover_subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;

    let mut low_source = FakeMediaSource::vp8_camera_with_rid("lo");
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut spillover_subscriber,
        &mut low_source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_releases_remote_route_after_subscriber_leaves() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        load_triggered_spillover_test_config(),
        "issuer-load-spillover-release-route",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(193);
    let local_subscriber_user_id = UserId::Integer(194);
    let spillover_subscriber_user_id = UserId::Integer(195);

    let setup = Box::pin(connect_load_triggered_spillover_rtc_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        local_subscriber_user_id.clone(),
        spillover_subscriber_user_id.clone(),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut local_subscriber, spillover_subscriber)) = setup else {
        return;
    };

    Box::pin(assert_load_triggered_spillover_release_route_flow(
        &server,
        &room,
        &mut publisher,
        &mut local_subscriber,
        spillover_subscriber,
        &publisher_user_id,
        &spillover_subscriber_user_id,
    ))
    .await;
}

async fn assert_load_triggered_spillover_release_route_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    mut spillover_subscriber: ProtocolFakePeer,
    publisher_user_id: &UserId,
    spillover_subscriber_user_id: &UserId,
) {
    let mut source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let local_track = assert_track_snapshot(
        local_subscriber,
        publisher_user_id.to_owned(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(local_subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(local_subscriber, publisher_user_id.to_owned()).await;
    let spillover_track = assert_track_snapshot(
        &mut spillover_subscriber,
        publisher_user_id.to_owned(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(
        spillover_subscriber
            .complete_next_negotiation()
            .await
            .is_some()
    );
    assert_video_subscription_enabled(&mut spillover_subscriber, publisher_user_id.to_owned())
        .await;
    assert_consumer_route_active(
        server,
        room,
        &spillover_subscriber,
        publisher_user_id,
        spillover_track.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        publisher,
        &mut spillover_subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    assert!(
        local_subscriber
            .read_rtp_packet(Duration::from_secs(2))
            .await
            .is_some()
    );

    assert!(spillover_subscriber.close().await.is_some());
    assert!(
        server
            .wait_for_consumer_route_absence(
                room,
                spillover_subscriber_user_id,
                publisher_user_id,
                spillover_track.stream_type,
            )
            .await
    );
    assert_consumer_route_active(
        server,
        room,
        local_subscriber,
        publisher_user_id,
        local_track.stream_type,
    )
    .await;
    assert_synthetic_video_packet_forwarded(publisher, local_subscriber, &mut source, &mut clock)
        .await;
}

#[tokio::test]
async fn fake_rtc_load_triggered_spillover_preserves_download_mute_after_subscriber_replacement() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server_with_config(
        load_triggered_spillover_test_config(),
        "issuer-load-spillover-replacement-mute",
        Some(TEST_ROOM_KEY),
    )
    .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();
    let publisher_user_id = UserId::Integer(196);
    let local_subscriber_user_id = UserId::Integer(197);
    let spillover_subscriber_user_id = UserId::Integer(198);

    let setup = Box::pin(connect_load_triggered_spillover_rtc_peers(
        &server,
        &room,
        publisher_user_id.clone(),
        local_subscriber_user_id,
        spillover_subscriber_user_id.clone(),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, _local_subscriber, mut spillover_subscriber)) = setup else {
        return;
    };

    Box::pin(assert_load_triggered_spillover_replacement_mute_flow(
        &server,
        &room,
        &mut publisher,
        &mut spillover_subscriber,
        publisher_user_id,
        spillover_subscriber_user_id,
    ))
    .await;
}

async fn assert_load_triggered_spillover_replacement_mute_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    spillover_subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        spillover_subscriber,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(
        spillover_subscriber
            .complete_next_negotiation()
            .await
            .is_some()
    );
    assert_consumer_route_active(
        server,
        room,
        spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;

    assert!(
        spillover_subscriber
            .update_subscription(
                publisher_user_id.clone(),
                DownloadStates {
                    audio: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    assert_consumer_route_inactive(
        server,
        room,
        spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(publisher, spillover_subscriber, &mut source, &mut clock).await;

    let replacement = connect_load_triggered_spillover_replacement(
        server,
        room,
        spillover_subscriber,
        &spillover_subscriber_user_id,
        0,
    )
    .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    let replacement_track = assert_track_snapshot(
        &mut replacement,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_consumer_route_inactive(
        server,
        room,
        &replacement,
        &publisher_user_id,
        replacement_track.stream_type,
    )
    .await;
    assert_audio_packet_dropped(publisher, &mut replacement, &mut source, &mut clock).await;
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_requires_keyframe_before_forwarding() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-vp8-selected-rid-keyframe").await;
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

    let mut source = FakeMediaSource::new(SyntheticVp8Stream::with_next_keyframe(false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(82),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(82)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(82),
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &UserId::Integer(82),
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock)
        .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_vp8_selected_rid_drops_other_rids_after_activation() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-vp8-selected-rid-filter").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(84),
        UserId::Integer(85),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut high_source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&high_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(84),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(84)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(84),
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &UserId::Integer(84),
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;

    let mut low_source = FakeMediaSource::vp8_camera_with_rid("lo");
    assert_synthetic_video_packet_dropped(
        &mut publisher,
        &mut subscriber,
        &mut low_source,
        &mut clock,
    )
    .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut high_source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_peers_forward_h264_high_rid_idr_without_browsers() {
    let _guard = full_stack_test_guard().await;
    let mut config = test_config(1_000, 10);
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let room_server =
        spawn_room_server_with_config(config, "issuer-h264-synthetic", Some(TEST_ROOM_KEY)).await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(94),
        UserId::Integer(95),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::h264_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(94),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(94)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(94),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_h264_selected_rid_requires_idr_before_forwarding() {
    let _guard = full_stack_test_guard().await;
    let mut config = test_config(1_000, 10);
    config.codecs.flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let room_server =
        spawn_room_server_with_config(config, "issuer-h264-selected-rid-idr", Some(TEST_ROOM_KEY))
            .await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(78),
        UserId::Integer(79),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::new(SyntheticH264Stream::with_idr(false));
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(78),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(78)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(78),
        track_binding.stream_type,
    )
    .await;
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                &room,
                subscriber.user_id(),
                &UserId::Integer(78),
                "hi",
            )
            .await
    );

    let mut clock = FakeClock::default();
    assert_synthetic_video_packet_dropped(&mut publisher, &mut subscriber, &mut source, &mut clock)
        .await;
    assert_synthetic_video_packet_forwarded(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_peer_rejects_invalid_synthetic_send_paths_without_panics() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-invalid-synthetic-send").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_two_rtc_ready_fake_peers(
        &server,
        &room,
        UserId::Integer(96),
        UserId::Integer(97),
        Duration::from_secs(5),
    )
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(96),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(&mut subscriber, UserId::Integer(96)).await;
    assert_consumer_route_active(
        &server,
        &room,
        &subscriber,
        &UserId::Integer(96),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    let mut unsupported_codec = FakeMediaSource::unsupported_camera_codec();
    let mut missing_rid = FakeMediaSource::vp8_camera_with_rid("missing");
    assert!(
        publisher
            .send_rtp_packet(&mut unsupported_codec, &mut clock)
            .await
            .is_none()
    );
    assert!(
        publisher
            .send_rtp_packet(&mut missing_rid, &mut clock)
            .await
            .is_none()
    );
}

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
    assert!(initial_publisher.publish_track(&source).await.is_some());
    assert!(
        initial_publisher
            .complete_next_negotiation()
            .await
            .is_some()
    );
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(80),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

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
    assert!(replacement.publish_track(&source).await.is_some());
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(80),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
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

async fn assert_replacement_unpublish_and_republish_flow(
    server: &TestServer,
    room: &str,
    initial_publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
) {
    let mut source = FakeMediaSource::audio();
    let mut clock = FakeClock::default();
    let harness = AudioRouteHarness::new(server, room, &publisher_user_id);
    assert_published_audio_forwarding(
        &harness,
        initial_publisher,
        subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    let replacement =
        connect_fake_peer(server, room, publisher_user_id.clone(), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_replacement_audio_forwarding(
        &harness,
        initial_publisher,
        &mut replacement,
        subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    assert_replacement_unpublish_and_republish_audio(
        &harness,
        &mut replacement,
        subscriber,
        &mut source,
        &mut clock,
    )
    .await;
}

struct AudioRouteHarness<'a> {
    server: &'a TestServer,
    room: &'a str,
    publisher_user_id: &'a UserId,
}

impl<'a> AudioRouteHarness<'a> {
    const fn new(server: &'a TestServer, room: &'a str, publisher_user_id: &'a UserId) -> Self {
        Self {
            server,
            room,
            publisher_user_id,
        }
    }
}

async fn assert_published_audio_forwarding(
    harness: &AudioRouteHarness<'_>,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        subscriber,
        harness.publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route_active(
        harness.server,
        harness.room,
        subscriber,
        harness.publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    assert_audio_packet_forwarded(publisher, subscriber, source, clock).await;
}

async fn assert_replacement_audio_forwarding(
    harness: &AudioRouteHarness<'_>,
    initial_publisher: &mut ProtocolFakePeer,
    replacement: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(subscriber, harness.publisher_user_id.clone()).await;
    assert_peer_joined_message_protocol(subscriber, harness.publisher_user_id.clone()).await;
    assert_audio_packet_dropped(initial_publisher, subscriber, source, clock).await;
    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert_published_audio_forwarding(harness, replacement, subscriber, source, clock).await;
}

async fn assert_replacement_unpublish_and_republish_audio(
    harness: &AudioRouteHarness<'_>,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert!(
        publisher
            .set_publication_active(StreamType::Audio, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_empty_track_snapshot(subscriber).await;
    assert_consumer_route_absent(
        harness.server,
        harness.room,
        subscriber,
        harness.publisher_user_id,
        StreamType::Audio,
    )
    .await;
    assert_audio_packet_dropped(publisher, subscriber, source, clock).await;
    assert_published_audio_forwarding(harness, publisher, subscriber, source, clock).await;
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

async fn assert_subscriber_replacement_preserves_download_mute_after_renegotiation(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    let muted_stream_type =
        mute_subscriber_audio_download(server, room, publisher, subscriber, &mut source).await;
    Box::pin(assert_replacement_subscriber_inherits_muted_audio_download(
        server,
        room,
        publisher,
        subscriber,
        muted_stream_type,
        &mut source,
    ))
    .await;
}

async fn mute_subscriber_audio_download(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
) -> StreamType {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding =
        assert_track_snapshot(subscriber, UserId::Integer(82), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route_active(
        server,
        room,
        subscriber,
        &UserId::Integer(82),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_audio_packet_forwarded(publisher, subscriber, source, &mut clock).await;

    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(82),
                DownloadStates {
                    audio: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    assert_consumer_route_inactive(
        server,
        room,
        subscriber,
        &UserId::Integer(82),
        track_binding.stream_type,
    )
    .await;
    track_binding.stream_type
}

async fn assert_replacement_subscriber_inherits_muted_audio_download(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    muted_stream_type: StreamType,
    source: &mut FakeMediaSource,
) {
    let replacement = connect_fake_peer(server, room, UserId::Integer(83), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        subscriber.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(publisher, UserId::Integer(83)).await;
    assert_peer_joined_message_protocol(publisher, UserId::Integer(83)).await;
    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    let replacement_track = assert_track_snapshot(
        &mut replacement,
        UserId::Integer(82),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_consumer_route_inactive(
        server,
        room,
        &replacement,
        &UserId::Integer(82),
        replacement_track.stream_type,
    )
    .await;

    assert_eq!(muted_stream_type, replacement_track.stream_type);
    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(publisher, &mut replacement, source, &mut clock).await;
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
    assert!(replacement.publish_track(&source).await.is_some());
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        UserId::Integer(86),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_audio_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
}

#[tokio::test]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-e").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_audio_media_flow_peers(&server, &room).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_download_mute_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_peers_stop_forwarding_after_explicit_upload_unpublish() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-f").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_audio_media_flow_peers(&server, &room).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
}

async fn connect_audio_media_flow_peers(
    server: &TestServer,
    room: &str,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    connect_two_rtc_ready_fake_peers(
        server,
        room,
        UserId::Integer(70),
        UserId::Integer(71),
        Duration::from_secs(5),
    )
    .await
}

fn cross_worker_test_config() -> Config {
    let mut config = test_config(1_000, 10);
    config.transport.rtc_media_worker_count = 2;
    config.transport.room_sharding_policy = RoomShardingPolicy::bounded_local_spillover(2);
    config
}

fn load_triggered_spillover_test_config() -> Config {
    let mut config = test_config(1_000, 10);
    let policy = match LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
        min_receiver_count: 3,
        activation_window: 1,
        ..LocalSpilloverPolicyParts::conservative()
    }) {
        Ok(policy) => policy,
        Err(error) => panic!("load-triggered spillover test policy should be valid: {error}"),
    };
    config.transport.rtc_media_worker_count = 2;
    config.transport.room_sharding_policy =
        RoomShardingPolicy::load_triggered_local_spillover(2, policy);
    config
}

async fn assert_cross_worker_placement(
    server: &TestServer,
    room: &str,
    publisher_user_id: &UserId,
    subscriber_user_id: &UserId,
) {
    assert!(
        server
            .wait_for_user_media_worker(room, publisher_user_id, 0)
            .await
    );
    assert!(
        server
            .wait_for_user_media_worker(room, subscriber_user_id, 1)
            .await
    );
}

async fn connect_load_triggered_spillover_rtc_peers(
    server: &TestServer,
    room: &str,
    publisher_user_id: UserId,
    local_subscriber_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer, ProtocolFakePeer)> {
    let activation_user_id = load_triggered_activation_user_id(&spillover_subscriber_user_id);
    let mut publisher =
        connect_load_triggered_peer_on_worker(server, room, &publisher_user_id, 0).await?;
    let mut local_subscriber = connect_load_triggered_local_subscriber(
        server,
        room,
        &mut publisher,
        &local_subscriber_user_id,
    )
    .await?;
    let activation_peer = connect_load_triggered_activation_peer(
        server,
        room,
        &mut publisher,
        &mut local_subscriber,
        &activation_user_id,
    )
    .await?;
    let mut spillover_subscriber = Box::pin(connect_load_triggered_spillover_subscriber(
        server,
        room,
        &mut publisher,
        &mut local_subscriber,
        &spillover_subscriber_user_id,
    ))
    .await?;
    assert_load_triggered_spillover_placement(
        server,
        room,
        &publisher_user_id,
        &local_subscriber_user_id,
        &spillover_subscriber_user_id,
    )
    .await;

    assert!(activation_peer.close().await.is_some());
    assert_departure_message_protocol(&mut publisher, activation_user_id.clone()).await;
    assert_departure_message_protocol(&mut local_subscriber, activation_user_id.clone()).await;
    assert_departure_message_protocol(&mut spillover_subscriber, activation_user_id).await;

    Some((publisher, local_subscriber, spillover_subscriber))
}

async fn connect_load_triggered_peer_on_worker(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    worker_id: usize,
) -> Option<ProtocolFakePeer> {
    let mut peer = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    peer.wait_until_connected(Duration::from_secs(5)).await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    Some(peer)
}

async fn connect_load_triggered_local_subscriber(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber_user_id: &UserId,
) -> Option<ProtocolFakePeer> {
    let peer =
        connect_load_triggered_peer_on_worker(server, room, local_subscriber_user_id, 0).await?;
    assert_peer_joined_message_protocol(publisher, local_subscriber_user_id.clone()).await;
    Some(peer)
}

async fn connect_load_triggered_activation_peer(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    activation_user_id: &UserId,
) -> Option<ProtocolFakePeer> {
    let mut peer =
        connect_fake_peer(server, room, activation_user_id.clone(), TEST_ROOM_KEY).await?;
    peer.wait_until_connected(Duration::from_secs(5)).await?;
    assert_peer_joined_message_protocol(publisher, activation_user_id.clone()).await;
    assert_peer_joined_message_protocol(local_subscriber, activation_user_id.clone()).await;
    Some(peer)
}

async fn connect_load_triggered_spillover_subscriber(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    spillover_subscriber_user_id: &UserId,
) -> Option<ProtocolFakePeer> {
    let peer = connect_load_triggered_peer_on_worker(server, room, spillover_subscriber_user_id, 1)
        .await?;
    assert_peer_joined_message_protocol(publisher, spillover_subscriber_user_id.clone()).await;
    assert_peer_joined_message_protocol(local_subscriber, spillover_subscriber_user_id.clone())
        .await;
    Some(peer)
}

async fn assert_load_triggered_spillover_placement(
    server: &TestServer,
    room: &str,
    publisher_user_id: &UserId,
    local_subscriber_user_id: &UserId,
    spillover_subscriber_user_id: &UserId,
) {
    assert_user_media_worker(server, room, publisher_user_id, 0).await;
    assert_user_media_worker(server, room, local_subscriber_user_id, 0).await;
    assert_user_media_worker(server, room, spillover_subscriber_user_id, 1).await;
}

async fn assert_user_media_worker(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    worker_id: usize,
) {
    assert!(
        server
            .wait_for_user_media_worker(room, user_id, worker_id)
            .await
    );
}

async fn connect_load_triggered_spillover_replacement(
    server: &TestServer,
    room: &str,
    previous_peer: &mut ProtocolFakePeer,
    user_id: &UserId,
    worker_id: usize,
) -> Option<ProtocolFakePeer> {
    let mut replacement = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    assert_eq!(
        previous_peer.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    replacement
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    Some(replacement)
}

fn load_triggered_activation_user_id(spillover_subscriber_user_id: &UserId) -> UserId {
    match spillover_subscriber_user_id {
        UserId::Integer(value) => UserId::Integer(value.saturating_add(10_000)),
        UserId::String(value) => UserId::String(format!("{value}-activation")),
    }
}

async fn assert_audio_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    assert_packet_forwarded(publisher, subscriber, source, clock).await
}

async fn assert_synthetic_video_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    for _ in 0..3 {
        let expected_payload = publisher.send_rtp_packet(source, clock).await;
        assert!(expected_payload.is_some());
        let Some(expected_payload) = expected_payload else {
            return 0;
        };
        if read_expected_rtp_payload(subscriber, &expected_payload, Duration::from_secs(5)).await {
            return u64::try_from(expected_payload.len()).unwrap_or(u64::MAX);
        }
    }
    panic!("synthetic video packet should be forwarded after route warmup");
}

async fn assert_synthetic_video_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher.send_rtp_packet(source, clock).await;
    assert!(expected_payload.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn assert_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    let expected_payload = publisher.send_rtp_packet(source, clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return 0;
    };

    assert!(read_expected_rtp_payload(subscriber, &expected_payload, Duration::from_secs(5)).await);
    u64::try_from(expected_payload.len()).unwrap_or(u64::MAX)
}

async fn read_expected_rtp_payload(
    subscriber: &mut ProtocolFakePeer,
    expected_payload: &[u8],
    timeout_window: Duration,
) -> bool {
    let deadline = Instant::now() + timeout_window;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let Some(received_packet) = subscriber.read_rtp_packet(deadline - now).await else {
            return false;
        };
        if received_packet.payload.as_ref() == expected_payload {
            return true;
        }
    }
}

async fn assert_video_subscription_enabled(
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
) {
    assert!(
        subscriber
            .update_subscription(
                publisher_user_id,
                DownloadStates {
                    camera: Some(true),
                    camera_layout: Some(VideoLayoutIntent::Featured),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
}

async fn assert_audio_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher.send_rtp_packet(source, clock).await;
    assert!(expected_payload.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn connect_two_isolated_audio_flows(
    server: &TestServer,
) -> Option<(
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
)> {
    let room_a = create_room(server, "issuer-topology-a", Some(TEST_ROOM_KEY)).await?;
    let room_b = create_room(server, "issuer-topology-b", Some(TEST_ROOM_KEY)).await?;

    let publisher_a =
        connect_fake_peer(server, &room_a, UserId::Integer(90), TEST_ROOM_KEY).await?;
    let subscriber_a =
        connect_fake_peer(server, &room_a, UserId::Integer(91), TEST_ROOM_KEY).await?;
    let publisher_b =
        connect_fake_peer(server, &room_b, UserId::Integer(90), TEST_ROOM_KEY).await?;
    let subscriber_b =
        connect_fake_peer(server, &room_b, UserId::Integer(91), TEST_ROOM_KEY).await?;

    let mut publisher_a = publisher_a;
    let mut subscriber_a = subscriber_a;
    let mut publisher_b = publisher_b;
    let mut subscriber_b = subscriber_b;

    publisher_a
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    subscriber_a
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    publisher_b
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    subscriber_b
        .wait_until_connected(Duration::from_secs(5))
        .await?;

    Some((publisher_a, subscriber_a, publisher_b, subscriber_b))
}

async fn assert_audio_media_arrives_and_download_mute_stops_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding =
        assert_track_snapshot(subscriber, UserId::Integer(70), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route_active(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    let _forwarded_bytes =
        assert_audio_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;

    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(70),
                DownloadStates {
                    audio: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    assert_consumer_route_inactive(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
    )
    .await;
    assert_audio_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
}

async fn assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding =
        assert_track_snapshot(subscriber, UserId::Integer(70), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route_active(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    let _forwarded_bytes =
        assert_audio_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;

    assert!(
        publisher
            .set_publication_active(StreamType::Audio, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_empty_track_snapshot(subscriber).await;
    assert_consumer_route_absent(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
    )
    .await;
    assert_audio_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
}

async fn connect_camera_flow_peers(
    server: &TestServer,
    room: &str,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    connect_two_fake_peers(server, room, UserId::Integer(10), UserId::Integer(20)).await
}

async fn publish_camera_track(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) -> Option<()> {
    let source = FakeMediaSource::camera();
    publisher.publish_track(&source).await?;
    publisher.complete_next_negotiation().await?;
    assert_track_snapshot(subscriber, UserId::Integer(10), StreamType::Camera, true).await;
    assert_peer_info_update(
        subscriber,
        UserId::Integer(10),
        UserInfo {
            is_camera_on: Some(true),
            ..UserInfo::snapshot_defaults()
        },
    )
    .await;
    Some(())
}

async fn assert_consumer_download_toggle_round_trip_protocol(subscriber: &mut ProtocolFakePeer) {
    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(10),
                DownloadStates {
                    camera: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(10),
                DownloadStates {
                    camera: Some(true),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
}

async fn assert_camera_unpublish_updates_snapshot_and_info(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    assert!(
        publisher
            .set_publication_active(StreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    let first_message = subscriber.read_next_server_message().await;
    let second_message = subscriber.read_next_server_message().await;
    assert!(first_message.is_some());
    assert!(second_message.is_some());
    let (Some(first_message), Some(second_message)) = (first_message, second_message) else {
        return;
    };
    let messages = [first_message, second_message];
    let track_snapshot = messages.iter().find_map(|message| match message {
        ServerMessage::Tracks(snapshot) => Some(snapshot),
        _ => None,
    });
    let Some(track_snapshot) = track_snapshot else {
        panic!("expected track snapshot after camera unpublish");
    };
    assert!(
        track_snapshot.is_empty(),
        "protocol unpublish should clear the authoritative camera track snapshot"
    );

    let peer_info = messages.iter().find_map(|message| match message {
        ServerMessage::PeerInfo(info) => Some(info),
        _ => None,
    });
    let Some(peer_info) = peer_info else {
        panic!("expected peer info update after camera unpublish");
    };
    assert_eq!(peer_info.user_id, UserId::Integer(10));
    assert_eq!(
        peer_info.info,
        UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::snapshot_defaults()
        }
    );
}

async fn connect_late_subscriber(server: &TestServer, room: &str) -> Option<ProtocolFakePeer> {
    connect_fake_peer(server, room, UserId::Integer(30), TEST_ROOM_KEY).await
}

async fn assert_late_join_has_no_track_snapshot(late_subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            late_subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

async fn assert_departure_message_protocol(subscriber: &mut ProtocolFakePeer, user_id: UserId) {
    let departure = subscriber.read_next_server_message().await;
    assert!(departure.is_some());
    let Some(ServerMessage::PeerLeft(departure)) = departure else {
        panic!("expected protocol peer departure notification");
    };
    assert_eq!(departure.user_id, user_id);
}

async fn assert_peer_joined_message_protocol(subscriber: &mut ProtocolFakePeer, user_id: UserId) {
    let joined = subscriber.read_next_server_message().await;
    assert!(joined.is_some());
    let Some(ServerMessage::PeerJoined(joined)) = joined else {
        panic!("expected protocol peer joined notification");
    };
    assert_eq!(joined.user_id, user_id);
}

async fn assert_track_snapshot(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    stream_type: StreamType,
    active: bool,
) -> TrackBinding {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected protocol track snapshot");
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(track_binding) = track_bindings.first() else {
        panic!("expected one protocol track binding");
    };
    assert_eq!(track_binding.user_id, user_id);
    assert_eq!(track_binding.stream_type, stream_type);
    assert_eq!(track_binding.active, active);
    track_binding.clone()
}

async fn assert_empty_track_snapshot(subscriber: &mut ProtocolFakePeer) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected protocol track snapshot");
    };
    assert!(track_bindings.is_empty());
}

async fn assert_peer_info_update(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    expected_info: UserInfo,
) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::PeerInfo(peer_info)) = message else {
        panic!("expected protocol peer info update");
    };
    assert_eq!(peer_info.user_id, user_id);
    assert_eq!(peer_info.info, expected_info);
}

async fn assert_no_server_message_protocol(subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

async fn assert_consumer_route_active(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
) {
    assert!(
        server
            .wait_for_consumer_route_active(
                room,
                subscriber.user_id(),
                publisher_user_id,
                stream_type,
            )
            .await
    );
}

async fn assert_consumer_route_inactive(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
) {
    assert!(
        server
            .wait_for_consumer_route_inactive(
                room,
                subscriber.user_id(),
                publisher_user_id,
                stream_type,
            )
            .await
    );
}

async fn assert_consumer_route_absent(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
) {
    assert!(
        server
            .wait_for_consumer_route_absence(
                room,
                subscriber.user_id(),
                publisher_user_id,
                stream_type,
            )
            .await
    );
}

async fn stream_until_audio_bitrate_is_observable(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> Option<IncomingBitRateStatsResponse> {
    for _ in 0..20 {
        publisher.send_rtp_packets(source, clock, 2).await?;
        let stats = stats(server).await?;
        let room_stats = stats.into_iter().find(|entry| entry.uuid == room)?;
        if room_stats.users_stats.incoming_bit_rate.audio > 0 {
            return Some(room_stats.users_stats.incoming_bit_rate);
        }
        yield_now().await;
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TransportUserLifetimeMetrics {
    le_1_second: u64,
    le_10_seconds: u64,
    le_60_seconds: u64,
    le_300_seconds: u64,
    count: u64,
    sum_seconds: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveRtcMetrics {
    connected_transport_users: i64,
    disconnected_transport_users: i64,
    transport_health_transitions_unset_to_connected: u64,
    transport_health_transitions_connected_to_disconnected: u64,
    transport_health_transitions_connected_to_unset: u64,
    transport_ice_state_changes_new: u64,
    transport_ice_state_changes_checking: u64,
    transport_ice_state_changes_connected: u64,
    transport_ice_state_changes_completed: u64,
    transport_ice_state_changes_disconnected: u64,
    transport_dtls_connected: u64,
    rtp_packets_ingress: u64,
    rtp_packets_egress: u64,
    rtp_payload_bytes_ingress: u64,
    rtp_payload_bytes_egress: u64,
    rtp_forwarded_packets_local_rtc: u64,
    rtp_forwarded_payload_bytes_local_rtc: u64,
    indexed_routes: u64,
    scan_routes: u64,
    fallback_scans: u64,
    scan_users: u64,
}

async fn wait_for_transport_lifetime_metrics(
    server: &TestServer,
    expected_count: u64,
) -> Option<TransportUserLifetimeMetrics> {
    timeout(Duration::from_secs(3), async {
        loop {
            let metrics = parse_transport_lifetime_metrics(&metrics_text(server).await?)?;
            if metrics.count >= expected_count {
                return Some(metrics);
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

async fn wait_for_live_rtc_metrics(
    server: &TestServer,
    expected_connected_users: i64,
) -> Option<LiveRtcMetrics> {
    timeout(Duration::from_secs(3), async {
        loop {
            let metrics = parse_live_rtc_metrics(&metrics_text(server).await?)?;
            if metrics.connected_transport_users == expected_connected_users {
                return Some(metrics);
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

fn parse_transport_lifetime_metrics(metrics_text: &str) -> Option<TransportUserLifetimeMetrics> {
    Some(TransportUserLifetimeMetrics {
        le_1_second: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"1\"}",
        )?,
        le_10_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"10\"}",
        )?,
        le_60_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"60\"}",
        )?,
        le_300_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"300\"}",
        )?,
        count: parse_prometheus_u64(metrics_text, "osfu_transport_user_lifetime_seconds_count")?,
        sum_seconds: parse_prometheus_f64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_sum",
        )?,
    })
}

fn parse_live_rtc_metrics(metrics_text: &str) -> Option<LiveRtcMetrics> {
    Some(LiveRtcMetrics {
        connected_transport_users: parse_prometheus_i64(
            metrics_text,
            "osfu_transport_health_users{state=\"connected\"}",
        )?,
        disconnected_transport_users: parse_prometheus_i64(
            metrics_text,
            "osfu_transport_health_users{state=\"disconnected\"}",
        )?,
        transport_health_transitions_unset_to_connected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_health_transitions_total{from=\"unset\",to=\"connected\"}",
        )?,
        transport_health_transitions_connected_to_disconnected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_health_transitions_total{from=\"connected\",to=\"disconnected\"}",
        )?,
        transport_health_transitions_connected_to_unset: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_health_transitions_total{from=\"connected\",to=\"unset\"}",
        )?,
        transport_ice_state_changes_new: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"new\"}",
        )?,
        transport_ice_state_changes_checking: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"checking\"}",
        )?,
        transport_ice_state_changes_connected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"connected\"}",
        )?,
        transport_ice_state_changes_completed: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"completed\"}",
        )?,
        transport_ice_state_changes_disconnected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"disconnected\"}",
        )?,
        transport_dtls_connected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_dtls_connected_total",
        )?,
        rtp_packets_ingress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_packets_total{direction=\"ingress\"}",
        )?,
        rtp_packets_egress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_packets_total{direction=\"egress\"}",
        )?,
        rtp_payload_bytes_ingress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_payload_bytes_total{direction=\"ingress\"}",
        )?,
        rtp_payload_bytes_egress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_payload_bytes_total{direction=\"egress\"}",
        )?,
        rtp_forwarded_packets_local_rtc: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"}",
        )?,
        rtp_forwarded_payload_bytes_local_rtc: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_forwarded_payload_bytes_total{destination=\"local_rtc\"}",
        )?,
        indexed_routes: parse_prometheus_u64(
            metrics_text,
            "osfu_rtc_datagram_routes_total{path=\"indexed\"}",
        )?,
        scan_routes: parse_prometheus_u64(
            metrics_text,
            "osfu_rtc_datagram_routes_total{path=\"scan\"}",
        )?,
        fallback_scans: parse_prometheus_u64(
            metrics_text,
            "osfu_rtc_datagram_fallback_scans_total",
        )?,
        scan_users: parse_prometheus_u64(metrics_text, "osfu_rtc_datagram_scan_users_total")?,
    })
}

fn assert_initial_live_rtc_metrics(metrics: &LiveRtcMetrics, initial_forwarded_bytes: u64) {
    assert_eq!(metrics.connected_transport_users, 2);
    assert_eq!(metrics.disconnected_transport_users, 0);
    assert!(
        metrics.transport_health_transitions_unset_to_connected >= 2,
        "expected both RTC users to enter a connected transport health state"
    );
    assert_eq!(
        metrics.transport_health_transitions_connected_to_disconnected,
        0
    );
    assert_eq!(metrics.transport_health_transitions_connected_to_unset, 0);
    assert!(
        metrics.transport_ice_state_changes_new + metrics.transport_ice_state_changes_checking >= 2,
        "expected both RTC users to emit early ICE lifecycle counters"
    );
    assert!(
        metrics.transport_ice_state_changes_connected
            + metrics.transport_ice_state_changes_completed
            >= 2,
        "expected both RTC users to reach a connected ICE lifecycle state"
    );
    assert_eq!(metrics.transport_ice_state_changes_disconnected, 0);
    assert_eq!(metrics.transport_dtls_connected, 2);
    assert_eq!(metrics.rtp_packets_ingress, 2);
    assert_eq!(metrics.rtp_packets_egress, 2);
    assert_eq!(metrics.rtp_payload_bytes_ingress, initial_forwarded_bytes);
    assert_eq!(metrics.rtp_payload_bytes_egress, initial_forwarded_bytes);
    assert_eq!(metrics.rtp_forwarded_packets_local_rtc, 2);
    assert_eq!(
        metrics.rtp_forwarded_payload_bytes_local_rtc,
        initial_forwarded_bytes
    );
}

fn assert_steady_state_live_rtc_metrics(
    before: &LiveRtcMetrics,
    during: &LiveRtcMetrics,
    additional_forwarded_bytes: u64,
) {
    assert_eq!(during.connected_transport_users, 2);
    assert_eq!(during.disconnected_transport_users, 0);
    assert_eq!(
        during.transport_health_transitions_unset_to_connected,
        before.transport_health_transitions_unset_to_connected
    );
    assert_eq!(
        during.transport_health_transitions_connected_to_disconnected,
        before.transport_health_transitions_connected_to_disconnected
    );
    assert_eq!(
        during.transport_health_transitions_connected_to_unset,
        before.transport_health_transitions_connected_to_unset
    );
    assert_eq!(
        during.transport_ice_state_changes_new,
        before.transport_ice_state_changes_new
    );
    assert_eq!(
        during.transport_ice_state_changes_checking,
        before.transport_ice_state_changes_checking
    );
    assert_eq!(
        during.transport_ice_state_changes_connected,
        before.transport_ice_state_changes_connected
    );
    assert_eq!(
        during.transport_ice_state_changes_completed,
        before.transport_ice_state_changes_completed
    );
    assert_eq!(
        during.transport_ice_state_changes_disconnected,
        before.transport_ice_state_changes_disconnected
    );
    assert_eq!(
        during.transport_dtls_connected,
        before.transport_dtls_connected
    );
    assert!(
        during.indexed_routes > before.indexed_routes,
        "expected steady-state media to increase indexed datagram routing"
    );
    assert_eq!(during.scan_routes, before.scan_routes);
    assert_eq!(during.fallback_scans, before.fallback_scans);
    assert_eq!(during.scan_users, before.scan_users);
    assert_eq!(during.rtp_packets_ingress - before.rtp_packets_ingress, 4);
    assert_eq!(during.rtp_packets_egress - before.rtp_packets_egress, 4);
    assert_eq!(
        during.rtp_payload_bytes_ingress - before.rtp_payload_bytes_ingress,
        additional_forwarded_bytes
    );
    assert_eq!(
        during.rtp_payload_bytes_egress - before.rtp_payload_bytes_egress,
        additional_forwarded_bytes
    );
    assert_eq!(
        during.rtp_forwarded_packets_local_rtc - before.rtp_forwarded_packets_local_rtc,
        4
    );
    assert_eq!(
        during.rtp_forwarded_payload_bytes_local_rtc - before.rtp_forwarded_payload_bytes_local_rtc,
        additional_forwarded_bytes
    );
}

fn parse_prometheus_i64(metrics_text: &str, metric_name: &str) -> Option<i64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}

fn parse_prometheus_u64(metrics_text: &str, metric_name: &str) -> Option<u64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}

fn parse_prometheus_f64(metrics_text: &str, metric_name: &str) -> Option<f64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}
