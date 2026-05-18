use super::support::*;

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
        TEST_ROOM_KEY,
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
        TEST_ROOM_KEY,
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
