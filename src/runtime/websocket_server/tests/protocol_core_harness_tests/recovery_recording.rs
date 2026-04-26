use super::support::*;

#[tokio::test]
async fn protocol_core_subscribe_updates_consumer_activity() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let server = spawn_test_server_with_timeouts(
        1_000,
        10_000,
        60_000,
        100,
        RuntimeTransportAdapter::from_fake_adapter(Arc::clone(&adapter)),
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-subscribe",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(61));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(62));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    assert!(
        alice
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
            .await
            .is_some()
    );
    assert!(
        bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
            .await
            .is_some()
    );
    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(62))
            .await
            .is_some()
    );

    let producer_id = room
        .test_api()
        .media()
        .publish_track(
            &UserId::Integer(61),
            StreamType::Camera,
            MediaKind::Video,
            sample_video_rtp_parameters("cam-1"),
            &server.transport_adapter,
        )
        .await;
    assert!(producer_id.is_some(), "protocol publisher should be ready");
    assert!(bob.read_server_frame().await.is_some());
    assert!(bob.read_server_frame().await.is_some());

    assert!(
        bob.subscribe(
            ProtocolSessionId::Integer(61),
            ProtocolDownloadStates {
                camera: Some(false),
                ..ProtocolDownloadStates::default()
            },
        )
        .await
        .is_some()
    );

    let observed = timeout(Duration::from_secs(1), async {
        loop {
            if adapter.snapshot_events().iter().any(|event| {
                matches!(
                    event,
                    FakeWebRtcEvent::ConsumerActivityUpdated {
                        consumer_user_id,
                        source_user_id,
                        active: false,
                    } if *consumer_user_id == UserId::Integer(62)
                        && *source_user_id == UserId::Integer(61)
                )
            }) {
                return true;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
    .unwrap_or(false);
    assert!(observed, "fake adapter should record subscribe activity");
}

#[tokio::test]
async fn protocol_core_subscribe_updates_real_rtc_consumer_activity() {
    let Some((server, room, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-subscribe",
        UserId::Integer(91),
        UserId::Integer(92),
        56_311,
        56_312,
    ))
    .await
    else {
        return;
    };

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "publisher should stage the initial protocol publish"
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should consume the rtc-backed renegotiation request and answer it"
    );

    let Some(track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(published_track) = track_bindings.first() else {
        return;
    };
    assert_eq!(published_track.user_id, ProtocolSessionId::Integer(91));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should consume the rtc-backed follow-up renegotiation request"
    );

    assert!(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &room,
            published_track,
            UserId::Integer(91),
            UserId::Integer(92),
            false,
        )
        .await
        .is_some(),
        "subscriber should disable the existing rtc route without extra websocket signaling"
    );
    assert!(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &room,
            published_track,
            UserId::Integer(91),
            UserId::Integer(92),
            true,
        )
        .await
        .is_some(),
        "real rtc route should mark the subscriber destination active again after subscribe(camera=true)"
    );
}

#[tokio::test]
async fn protocol_core_replays_latest_subscribe_after_real_server_recovery() {
    let adapter = Arc::new(FakeWebRtcAdapter::default());
    let alice_user_id = UserId::Integer(83);
    let bob_user_id = UserId::Integer(84);
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_fake_protocol_peers(
        Arc::clone(&adapter),
        "issuer-protocol-subscribe-recovery",
        alice_user_id.clone(),
        bob_user_id.clone(),
    ))
    .await
    else {
        return;
    };

    assert!(
        publish_camera_and_bootstrap_subscriber(
            &mut alice,
            &mut bob,
            &alice_user_id,
            "publisher should stage the initial protocol publish",
            "publisher should consume the initial publish renegotiation and answer it",
            "subscriber should receive the initial translated track snapshot",
        )
        .await
        .is_some()
    );

    assert!(
        bob.subscribe(
            protocol_user_id(&alice_user_id),
            ProtocolDownloadStates {
                camera: Some(false),
                ..ProtocolDownloadStates::default()
            },
        )
        .await
        .is_some()
    );
    let baseline_event_count = adapter.snapshot_events().len();

    assert!(
        recover_subscriber_and_replay_track(
            &mut alice,
            &mut bob,
            &alice_user_id,
            "recovery timer should reconnect the subscriber",
            "subscriber should consume the recovery welcome frame",
            "subscriber should consume the recovery initial offer",
            "subscriber should receive a replayed track snapshot after recovery",
        )
        .await
        .is_some()
    );

    let replayed_inactive = timeout(Duration::from_secs(1), async {
        loop {
            if adapter
                .snapshot_events()
                .iter()
                .skip(baseline_event_count)
                .any(|event| {
                    matches!(
                        event,
                        FakeWebRtcEvent::ConsumerActivityUpdated {
                            consumer_user_id,
                            source_user_id,
                            active: false,
                        } if *consumer_user_id == bob_user_id
                            && *source_user_id == alice_user_id
                    )
                })
            {
                return Some(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        matches!(replayed_inactive, Ok(Some(()))),
        "subscriber recovery should replay the latest muted camera subscription"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_replays_latest_subscribe_after_real_rtc_server_recovery() {
    let alice_user_id = UserId::Integer(93);
    let bob_user_id = UserId::Integer(94);
    let Some((server, room, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-subscribe-recovery",
        alice_user_id.clone(),
        bob_user_id.clone(),
        56_391,
        56_392,
    ))
    .await
    else {
        return;
    };

    let Some(published_track) = publish_camera_and_bootstrap_subscriber(
        &mut alice,
        &mut bob,
        &alice_user_id,
        "publisher should stage the initial protocol publish on the real rtc path",
        "publisher should consume the initial real-rtc publish renegotiation and answer it",
        "subscriber should receive the initial translated track snapshot on the real rtc path",
    )
    .await
    else {
        return;
    };
    assert!(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &room,
            &published_track,
            alice_user_id.clone(),
            bob_user_id.clone(),
            false,
        )
        .await
        .is_some(),
        "subscriber should mark the initial rtc route inactive before recovery"
    );

    let Some(replayed_track) = recover_subscriber_and_replay_track(
        &mut alice,
        &mut bob,
        &alice_user_id,
        "recovery timer should reconnect the real-rtc subscriber",
        "subscriber should consume the recovery welcome frame on the real rtc path",
        "subscriber should consume the recovery initial offer on the real rtc path",
        "subscriber should receive the replayed track snapshot after recovery on the real rtc path",
    )
    .await
    else {
        return;
    };
    let Some(route_activity) = real_rtc_route_activity(
        &server,
        &room,
        alice_user_id.clone(),
        bob_user_id.clone(),
        &replayed_track.mid,
    )
    .await
    else {
        panic!("recovered subscriber route should exist");
    };
    assert!(
        route_activity
            == RealRtcRouteActivity {
                source_active: true,
                consumer_active: false,
            },
        "subscriber recovery should replay the latest muted camera subscription on the real rtc path"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_recording_requests_resolve_against_real_server_responses() {
    let server = spawn_test_server_with_feature_flags(
        1_000,
        100,
        RuntimeTransportAdapter::fake_for_testing(),
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
    )
    .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-recording",
        None,
        CreateRoomQuery {
            recording_address: Some("https://record.example.com".to_owned()),
            ..CreateRoomQuery::default()
        },
    )
    .await;
    let mut peer = connect_protocol_recording_peer(&server, &room).await;
    assert!(peer.is_some());
    let Some(ref mut peer) = peer else {
        return;
    };

    assert!(
        peer.start_recording(Some(true), Some(false), None)
            .await
            .is_some()
    );
    let start_request_id = assert_recording_request_roundtrip(
        peer,
        HostPendingRequestKind::StartRecording,
        None,
        RecordingState {
            recording: Some(true),
            audio: Some(true),
            video: Some(false),
            transcription: Some(false),
        },
    )
    .await;
    assert!(start_request_id.is_some());
    if start_request_id.is_none() {
        return;
    }

    peer.pending_request_commands.clear();
    peer.updates.clear();

    assert!(peer.stop_recording().await.is_some());
    let stop_request_id = assert_recording_request_roundtrip(
        peer,
        HostPendingRequestKind::StopRecording,
        Some(ProtocolStopCode::UserRequest),
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            video: Some(false),
            transcription: Some(false),
        },
    )
    .await;
    assert!(stop_request_id.is_some());
    if stop_request_id.is_none() {
        return;
    }
}

#[tokio::test]
async fn protocol_core_replays_latest_info_after_real_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_protocol_recovery_peers(
        UserId::Integer(71),
        UserId::Integer(72),
    ))
    .await
    else {
        return;
    };

    assert!(
        bob_update_info_and_deliver(
            &mut bob,
            &mut alice,
            ProtocolSessionInfo {
                is_self_muted: Some(true),
                ..ProtocolSessionInfo::default()
            },
        )
        .await
        .is_some()
    );
    alice.updates.clear();

    assert!(
        close_peer_and_observe_recovery(&mut bob, &mut alice)
            .await
            .is_some()
    );
    alice.updates.clear();

    let latest_info = ProtocolSessionInfo {
        is_self_muted: Some(false),
        is_raising_hand: Some(true),
        ..ProtocolSessionInfo::default()
    };
    assert!(
        recover_peer_with_latest_info(&mut bob, latest_info.clone())
            .await
            .is_some()
    );
    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(72))
            .await
            .is_some()
    );
    alice.updates.clear();

    assert!(
        alice.read_server_frame().await.is_some(),
        "alice should receive bob's replayed latest user info after recovery"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(72)),
            ProtocolSessionInfo {
                is_self_muted: Some(false),
                is_raising_hand: Some(true),
                ..ProtocolSessionInfo::snapshot_defaults()
            },
        )])))
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_propagates_raise_hand_info_over_real_server_user_flow() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_protocol_recovery_peers(
        UserId::Integer(91),
        UserId::Integer(92),
    ))
    .await
    else {
        return;
    };

    let latest_info = ProtocolSessionInfo {
        is_raising_hand: Some(true),
        ..ProtocolSessionInfo::default()
    };
    assert!(
        bob_update_info_and_deliver(&mut bob, &mut alice, latest_info.clone())
            .await
            .is_some()
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(92)),
            ProtocolSessionInfo {
                is_raising_hand: Some(true),
                ..ProtocolSessionInfo::snapshot_defaults()
            },
        )])))
    );
}

#[tokio::test]
async fn protocol_core_replays_latest_publish_after_real_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_protocol_recovery_peers(
        UserId::Integer(81),
        UserId::Integer(82),
    ))
    .await
    else {
        return;
    };

    assert!(
        bob.publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "publisher should stage the initial protocol publish"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the initial publish renegotiation and answer it"
    );
    let initial_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        initial_track_snapshot.is_some(),
        "subscriber should receive the initial translated track snapshot"
    );
    let Some(initial_track_snapshot) = initial_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &initial_track_snapshot,
        &ProtocolSessionId::Integer(82),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should receive the initial remote-track renegotiation request"
    );
    alice.updates.clear();

    assert!(
        close_peer_and_observe_recovery(&mut bob, &mut alice)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the departure-side renegotiation before recovery rejoin"
    );
    alice.updates.clear();

    assert!(
        bob.flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "recovery timer should reconnect the publisher"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery welcome frame"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery initial offer"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the replayed publish renegotiation after recovery"
    );

    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(82))
            .await
            .is_some()
    );
    let replayed_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        replayed_track_snapshot.is_some(),
        "subscriber should receive a replayed track snapshot after publisher recovery"
    );
    let Some(replayed_track_snapshot) = replayed_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &ProtocolSessionId::Integer(82),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should receive the replayed remote-track renegotiation request"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}

#[tokio::test]
async fn protocol_core_replays_latest_publish_after_real_rtc_server_recovery() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-recovery",
        UserId::Integer(91),
        UserId::Integer(92),
        55_091,
        55_092,
    ))
    .await
    else {
        return;
    };

    assert!(
        bob.publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "publisher should stage the initial protocol publish on the real rtc path"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the initial real-rtc publish renegotiation and answer it"
    );
    let initial_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        initial_track_snapshot.is_some(),
        "subscriber should receive the initial translated track snapshot on the real rtc path"
    );
    let Some(initial_track_snapshot) = initial_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &initial_track_snapshot,
        &ProtocolSessionId::Integer(92),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the initial real-rtc remote-track renegotiation request"
    );
    alice.updates.clear();

    assert!(
        close_peer_and_observe_recovery(&mut bob, &mut alice)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the departure-side real-rtc renegotiation before recovery rejoin"
    );
    alice.updates.clear();

    assert!(
        bob.flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "recovery timer should reconnect the real-rtc publisher"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery welcome frame on the real rtc path"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the recovery initial offer on the real rtc path"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "publisher should consume the replayed real-rtc publish renegotiation after recovery"
    );

    assert!(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(92))
            .await
            .is_some()
    );
    let replayed_track_snapshot = read_track_snapshot(&mut alice).await;
    assert!(
        replayed_track_snapshot.is_some(),
        "subscriber should receive a replayed track snapshot after real-rtc publisher recovery"
    );
    let Some(replayed_track_snapshot) = replayed_track_snapshot else {
        return;
    };
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &ProtocolSessionId::Integer(92),
        ProtocolStreamType::Camera,
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "subscriber should consume the replayed real-rtc remote-track renegotiation request"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
}
