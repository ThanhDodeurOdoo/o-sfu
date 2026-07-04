use super::support::*;

type ProtocolSetup = (
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
);

async fn real_rtc_peers(
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
    alice_port: u16,
    bob_port: u16,
) -> TestResult<ProtocolSetup> {
    require_some(
        Box::pin(setup_real_rtc_protocol_peers(
            room_name,
            alice_user_id,
            bob_user_id,
            alice_port,
            bob_port,
        ))
        .await,
        "real RTC protocol peers should start",
    )
}

async fn recovery_peers(alice_user_id: UserId, bob_user_id: UserId) -> TestResult<ProtocolSetup> {
    require_some(
        Box::pin(setup_protocol_recovery_peers(alice_user_id, bob_user_id)).await,
        "protocol recovery peers should start",
    )
}

#[tokio::test]
async fn protocol_core_replays_latest_subscribe_after_real_rtc_server_recovery() -> TestResult {
    let alice_user_id = UserId::Integer(93);
    let bob_user_id = UserId::Integer(94);
    let (server, room, mut alice, mut bob) = Box::pin(real_rtc_peers(
        "issuer-protocol-rtc-subscribe-recovery",
        alice_user_id.clone(),
        bob_user_id.clone(),
        56_391,
        56_392,
    ))
    .await?;

    let published_track = require_some(
        publish_camera_and_setup_subscriber(
            &mut alice,
            &mut bob,
            &alice_user_id,
            "publisher should stage the initial protocol publish on the real rtc path",
            "publisher should consume the initial real-rtc publish renegotiation and answer it",
            "subscriber should receive the initial translated track snapshot on the real rtc path",
        )
        .await,
        "real RTC publish should setup the subscriber",
    )?;
    require_some(
        assert_real_rtc_subscribe_activity(
            &mut bob,
            &server,
            &room,
            &published_track,
            alice_user_id.clone(),
            bob_user_id.clone(),
            false,
        )
        .await,
        "subscriber should mark the initial rtc route inactive before recovery",
    )?;

    let replayed_track = require_some(
        recover_subscriber_and_replay_track(
            &mut alice,
            &mut bob,
            &alice_user_id,
            "recovery timer should reconnect the real-rtc subscriber",
            "subscriber should consume the recovery welcome frame on the real rtc path",
            "subscriber should consume the recovery initial offer on the real rtc path",
            "subscriber should receive the replayed track snapshot after recovery on the real rtc path",
        )
        .await,
        "real RTC subscriber should recover and replay the track",
    )?;
    let route_activity = require_some(
        real_rtc_route_activity(
            &server,
            &room,
            alice_user_id.clone(),
            bob_user_id.clone(),
            &replayed_track.mid,
        )
        .await,
        "recovered subscriber route should exist",
    )?;
    assert_eq!(
        route_activity,
        RealRtcRouteActivity {
            source_active: true,
            consumer_active: false,
        },
        "subscriber recovery should replay the latest muted camera subscription on the real rtc path"
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
    Ok(())
}

#[tokio::test]
async fn protocol_core_recording_requests_resolve_as_unsupported_without_backend() -> TestResult {
    let server = TestServerBuilder::new()
        .feature_flags(RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        })
        .spawn_required()
        .await?;
    let room = create_room(
        &server,
        "issuer-protocol-recording",
        CreateRoomQuery {
            recording_address: Some("https://record.example.com".to_owned()),
            ..CreateRoomQuery::default()
        },
    )
    .await;
    let mut peer = require_some(
        connect_protocol_recording_peer(&server, &room).await,
        "recording protocol peer should connect",
    )?;
    peer.pending_request_starts.clear();
    peer.pending_request_resolutions.clear();
    peer.updates.clear();

    require_some(
        peer.start_recording(Some(true), Some(false), None).await,
        "start recording command should run",
    )?;
    require_some(
        assert_recording_request_rejected(&mut peer, PendingRequestKind::StartRecording).await,
        "start recording request should be rejected",
    )?;

    peer.pending_request_starts.clear();
    peer.pending_request_resolutions.clear();
    peer.updates.clear();

    require_some(
        peer.stop_recording().await,
        "stop recording command should run",
    )?;
    require_some(
        assert_recording_request_rejected(&mut peer, PendingRequestKind::StopRecording).await,
        "stop recording request should be rejected",
    )?;
    Ok(())
}

#[tokio::test]
async fn protocol_core_replays_latest_info_after_real_server_recovery() -> TestResult {
    let (_server, _channel, mut alice, mut bob) =
        Box::pin(recovery_peers(UserId::Integer(71), UserId::Integer(72))).await?;

    require_some(
        update_info_and_deliver_to_peer(
            &mut bob,
            &mut alice,
            ProtocolSessionInfo {
                is_self_muted: Some(true),
                ..ProtocolSessionInfo::default()
            },
        )
        .await,
        "initial info update should deliver",
    )?;
    alice.updates.clear();

    require_some(
        close_peer_and_observe_recovery(&mut bob, &mut alice).await,
        "peer close should trigger recovery",
    )?;
    alice.updates.clear();

    let latest_info = ProtocolSessionInfo {
        is_self_muted: Some(false),
        is_raising_hand: Some(true),
        ..ProtocolSessionInfo::default()
    };
    require_some(
        recover_peer_with_latest_info(&mut bob, latest_info.clone()).await,
        "recovering peer should replay latest info",
    )?;
    require_some(
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(72)).await,
        "subscriber should consume peer rejoin",
    )?;
    alice.updates.clear();

    require_some(
        alice.read_server_frame().await,
        "alice should receive bob's replayed latest user info after recovery",
    )?;
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(72)),
            ProtocolSessionInfo {
                is_self_muted: Some(false),
                is_raising_hand: Some(true),
                ..ProtocolSessionInfo::default().snapshot_complete()
            },
        )])))
    );
    assert!(peer_reached_state(&bob, BundleConnectionState::Recovering));
    assert!(peer_reached_state(&bob, BundleConnectionState::Connected));
    Ok(())
}

#[tokio::test]
async fn protocol_core_replays_latest_publish_after_real_server_recovery() -> TestResult {
    let (_server, _channel, mut alice, mut bob) =
        Box::pin(recovery_peers(UserId::Integer(81), UserId::Integer(82))).await?;

    require_some(
        publish_camera_and_setup_subscriber(
            &mut bob,
            &mut alice,
            &ProtocolSessionId::Integer(82),
            "publisher should stage the initial protocol publish",
            "publisher should consume the initial publish renegotiation and answer it",
            "subscriber should receive the initial translated track snapshot",
        )
        .await,
        "camera publish should setup the subscriber",
    )?;

    require_some(
        recover_publisher_and_replay_camera_publish(
            &mut bob,
            &mut alice,
            ProtocolSessionId::Integer(82),
        )
        .await,
        "publisher recovery should replay camera publish",
    )?;
    Ok(())
}
