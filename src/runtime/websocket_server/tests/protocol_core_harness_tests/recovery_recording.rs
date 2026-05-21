use super::support::*;

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
        consume_peer_info_update(
            &mut bob,
            ProtocolSessionId::Integer(91),
            ProtocolSessionInfo {
                is_camera_on: Some(true),
                ..ProtocolSessionInfo::snapshot_defaults()
            },
        )
        .await
        .is_some(),
        "subscriber should consume the publisher camera-info update before subscribe activity assertions"
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
async fn protocol_core_recording_requests_resolve_as_unsupported_without_backend() {
    let server = TestServerBuilder::new()
        .feature_flags(RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        })
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-recording",
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
    peer.pending_request_commands.clear();
    peer.updates.clear();

    assert!(
        peer.start_recording(Some(true), Some(false), None)
            .await
            .is_some()
    );
    let start_request_id =
        assert_recording_request_rejected(peer, HostPendingRequestKind::StartRecording).await;
    assert!(start_request_id.is_some());
    if start_request_id.is_none() {
        return;
    }

    peer.pending_request_commands.clear();
    peer.updates.clear();

    assert!(peer.stop_recording().await.is_some());
    let stop_request_id =
        assert_recording_request_rejected(peer, HostPendingRequestKind::StopRecording).await;
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
        publish_camera_and_bootstrap_subscriber(
            &mut bob,
            &mut alice,
            &ProtocolSessionId::Integer(82),
            "publisher should stage the initial protocol publish",
            "publisher should consume the initial publish renegotiation and answer it",
            "subscriber should receive the initial translated track snapshot",
        )
        .await
        .is_some()
    );

    assert!(
        recover_publisher_and_replay_camera_publish(
            &mut bob,
            &mut alice,
            ProtocolSessionId::Integer(82),
        )
        .await
        .is_some()
    );
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
        publish_camera_and_bootstrap_subscriber(
            &mut bob,
            &mut alice,
            &ProtocolSessionId::Integer(92),
            "publisher should stage the initial protocol publish",
            "publisher should consume the initial publish renegotiation and answer it",
            "subscriber should receive the initial translated track snapshot",
        )
        .await
        .is_some()
    );

    assert!(
        recover_publisher_and_replay_camera_publish(
            &mut bob,
            &mut alice,
            ProtocolSessionId::Integer(92),
        )
        .await
        .is_some()
    );
}
