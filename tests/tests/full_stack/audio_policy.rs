use super::support::*;

#[tokio::test]
async fn fake_rtc_opus_vad_true_drives_active_speaker_diagnostics() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-opus-active-speaker",
        UserId::Integer(88),
        UserId::Integer(89),
    )
    .await?;

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(-32, true));
    publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(88),
        &source,
    )
    .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_cross_worker_opus_vad_true_forwards_and_drives_active_speaker() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let publisher_user_id = UserId::Integer(188);
    let subscriber_user_id = UserId::Integer(189);
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-opus-active-speaker",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    )
    .await?;
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(-32, true));
    publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &source,
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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_opus_vad_false_blocks_audio_forwarding() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers(
        "issuer-opus-vad-false",
        UserId::Integer(86),
        UserId::Integer(87),
    )
    .await?;

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(0, false));
    publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &UserId::Integer(86),
        &source,
    )
    .await;

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
    Ok(())
}

#[tokio::test]
async fn fake_rtc_cross_worker_opus_vad_false_blocks_relay_fanout() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let publisher_user_id = UserId::Integer(186);
    let subscriber_user_id = UserId::Integer(187);
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers_with_config(
        cross_worker_test_config(),
        "issuer-cross-worker-opus-vad-false",
        publisher_user_id.clone(),
        subscriber_user_id.clone(),
    )
    .await?;
    assert_cross_worker_placement(&server, &room, &publisher_user_id, &subscriber_user_id).await;

    let mut source = FakeMediaSource::new(SyntheticOpusStream::with_audio_activity(0, false));
    publish_source_and_ready_route(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
        &publisher_user_id,
        &source,
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
    Ok(())
}
