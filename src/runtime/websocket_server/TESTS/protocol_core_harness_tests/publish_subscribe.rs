use o_sfu_protocol::host::test_support::track_binding as protocol_track_binding;

use super::support::*;

#[tokio::test]
async fn protocol_core_receives_translated_track_snapshot_and_explicit_unpublish_removal()
-> TestResult {
    let (server, room, mut alice, mut bob) = Box::pin(setup_protocol_peers(
        "issuer-protocol-tracks",
        UserId::Integer(51),
        UserId::Integer(52),
    ))
    .await?;

    require_some(
        room.test_api()
            .media()
            .publish_intent(
                &UserId::Integer(51),
                &source_publish_intent_for_stream_type(StreamType::Camera),
                MediaKind::Video,
                sample_video_rtp_parameters("cam-0"),
                &server.media_transport,
            )
            .await,
        "protocol publisher should be ready",
    )?;

    let track_bindings = require_some(
        read_track_snapshot(&mut bob).await,
        "subscriber should receive the translated track snapshot",
    )?;
    let track_binding = require_some(
        track_bindings.into_iter().find(|binding| {
            binding.user_id == ProtocolSessionId::Integer(51)
                && binding.stream_type == ProtocolStreamType::Camera
        }),
        "subscriber should keep the camera track binding",
    )?;
    assert!(!track_binding.mid.is_empty());
    assert!(track_binding.active);
    let track_mid = track_binding.mid.clone();
    require_some(
        bob.read_server_frame().await,
        "bob should consume the serialized renegotiation request after track setup",
    )?;

    require_some(
        alice.publish(ProtocolStreamType::Camera, false).await,
        "alice should unpublish camera",
    )?;
    require_some(
        alice.read_server_frame().await,
        "publisher should receive the removal renegotiation request",
    )?;
    let removed_tracks = require_some(
        read_track_snapshot(&mut bob).await,
        "bob should consume the translated track-removal snapshot",
    )?;
    assert!(removed_tracks.is_empty());
    assert_eq!(protocol_track_binding(&bob.core, &track_mid), None);
    require_some(
        bob.read_server_frame().await,
        "subscriber should receive the removal renegotiation request",
    )?;
    Ok(())
}

#[tokio::test]
async fn protocol_core_publish_round_trips_through_real_rtc_server_user_protocol() -> TestResult {
    let (_server, _room, mut alice, mut bob) = require_some(
        Box::pin(setup_real_rtc_protocol_peers(
            "issuer-protocol-rtc-publish",
            UserId::Integer(71),
            UserId::Integer(72),
            56_301,
            56_302,
        ))
        .await,
        "real RTC protocol peers should start",
    )?;

    require_some(
        alice.publish(ProtocolStreamType::Camera, true).await,
        "publisher should stage camera publish",
    )?;
    require_some(
        alice.read_server_frame().await,
        "publisher should consume the rtc-backed renegotiation request and answer it",
    )?;

    let track_bindings = require_some(
        read_track_snapshot(&mut bob).await,
        "subscriber should receive the rtc-backed translated track snapshot",
    )?;
    assert_eq!(track_bindings.len(), 1);
    let published_track = require_some(
        track_bindings.first(),
        "subscriber should keep one published track",
    )?;
    assert_eq!(published_track.user_id, ProtocolSessionId::Integer(71));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    assert_eq!(
        protocol_track_binding(&bob.core, &published_track.mid),
        Some(published_track)
    );

    require_some(
        bob.read_server_frame().await,
        "subscriber should receive the rtc-backed follow-up renegotiation request",
    )?;
    Ok(())
}

#[tokio::test]
async fn protocol_handshake_uses_answer_derived_client_capabilities_for_user_state() -> TestResult {
    let server = TestServerBuilder::new()
        .media_transport(build_real_rtc_media_transport())
        .spawn_required()
        .await?;
    let room = create_room(
        &server,
        "issuer-protocol-rtc-capabilities",
        CreateRoomQuery::default(),
    )
    .await;
    let mut alice = require_some(
        ProtocolHarnessPeer::with_custom_rtc_negotiation(56_305, reduced_capability_rtc),
        "custom RTC protocol peer should build",
    )?;
    let alice_token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(75)),
        "alice token should sign",
    )?;

    require_some(
        alice
            .connect_and_finish_handshake(&server.url(), &alice_token, None)
            .await,
        "alice should finish protocol handshake",
    )?;

    let client_rtp_codec_names = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(codec_names) = room
                .test_api()
                .inspect()
                .session_client_rtp_codec_names(&UserId::Integer(75))
                .await
            {
                return codec_names;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        client_rtp_codec_names.is_ok(),
        "protocol handshake should store parsed client RTP capabilities"
    );
    let codec_names = require_some(client_rtp_codec_names.ok(), "codec names should resolve")?;
    assert_eq!(codec_names, vec![String::from("opus"), String::from("VP8")]);
    assert!(
        codec_names.iter().all(|codec| codec != "rtx"),
        "the production VP8 receive surface should not preserve RTX while RID repair demux is disabled"
    );
    assert!(
        codec_names.iter().all(|codec| codec != "H264"),
        "the stored client RTP capabilities must reflect the real RTC answer"
    );
    Ok(())
}

#[tokio::test]
async fn protocol_core_publish_queues_follow_up_renegotiation_until_first_answer_lands() {
    let Some((_server, room, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-publish-queue",
        UserId::Integer(73),
        UserId::Integer(74),
        56_303,
        56_304,
    ))
    .await
    else {
        return;
    };
    alice.auto_answer_negotiation = false;

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the first rtc-backed renegotiation request"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the first publish should leave one pending negotiation answer in the harness"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Screen, true)
            .await
            .is_some()
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the second publish should queue behind the in-flight negotiation instead of producing a second simultaneous offer"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the first queued negotiation"
    );
    assert!(
        consume_camera_publish_setup(
            &mut bob,
            &ProtocolSessionId::Integer(73),
            "subscriber should receive the initial camera track snapshot",
        )
        .await
        .is_some(),
        "subscriber should consume the committed camera publish setup"
    );

    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the queued follow-up renegotiation only after the first answer lands"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the queued publish should surface exactly one follow-up negotiation request"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the queued follow-up negotiation"
    );
    let Some(updated_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(updated_track_bindings.len(), 2);
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(73),
        ProtocolStreamType::Camera,
    );
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(73),
        ProtocolStreamType::Screen,
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the follow-up renegotiation request for the queued publish"
    );
    assert!(
        close_peer_and_wait_for_room_cleanup(&mut alice, &room, &UserId::Integer(73))
            .await
            .is_some(),
        "publisher websocket should close cleanly before the test server is dropped"
    );
    assert!(
        close_peer_and_wait_for_room_cleanup(&mut bob, &room, &UserId::Integer(74))
            .await
            .is_some(),
        "subscriber websocket should close cleanly before the test server is dropped"
    );
}

#[tokio::test]
async fn protocol_core_unpublish_cancels_pending_publish_before_commit() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-publish-cancel",
        UserId::Integer(75),
        UserId::Integer(76),
        56_305,
        56_306,
    ))
    .await
    else {
        return;
    };
    alice.auto_answer_negotiation = false;

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the staged publish renegotiation request"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the first publish should leave one pending negotiation answer in the harness"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "canceling the staged publish should not create an overlapping negotiation"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the staged publish negotiation"
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the follow-up renegotiation that removes the canceled publish"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "canceling the staged publish should queue exactly one follow-up removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber should not observe a track snapshot before the canceled publish is removed"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the follow-up removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber should not observe track or renegotiation updates for a publish canceled before commit"
    );
}

#[tokio::test]
async fn protocol_core_unpublish_round_trips_through_real_rtc_after_publish_commit() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-unpublish",
        UserId::Integer(77),
        UserId::Integer(78),
        56_307,
        56_308,
    ))
    .await
    else {
        return;
    };

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the initial rtc-backed publish renegotiation"
    );

    let Some(initial_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert_eq!(initial_track_bindings.len(), 1);
    let Some(published_track) = initial_track_bindings.first() else {
        return;
    };
    assert_eq!(published_track.user_id, ProtocolSessionId::Integer(77));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should answer the follow-up renegotiation for the committed publish"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the rtc-backed unpublish renegotiation"
    );

    let Some(removed_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
    assert!(
        removed_track_bindings.is_empty(),
        "committed unpublish should clear the authoritative track snapshot"
    );
    assert_eq!(
        protocol_track_binding(&bob.core, &published_track.mid),
        None,
        "committed unpublish should remove the cached track binding"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should answer the rtc-backed renegotiation that removes the remote track"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should also receive the translated peer-info update for the committed unpublish"
    );
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(77)),
            ProtocolSessionInfo {
                is_camera_on: Some(false),
                ..ProtocolSessionInfo::default().snapshot_complete()
            },
        )]))),
        "committed unpublish should clear the publisher camera flag in the observable peer info"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "committed unpublish should not leave further rtc follow-up frames queued"
    );
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the regression keeps the full queued-removal rtc flow explicit in one place for reviewability"
)]
async fn protocol_core_unpublish_queues_subscriber_removal_until_in_flight_rtc_answer_lands() {
    let Some((_server, room, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-unpublish-removal-queue",
        UserId::Integer(79),
        UserId::Integer(80),
        56_309,
        56_310,
    ))
    .await
    else {
        return;
    };

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the initial rtc-backed publish renegotiation"
    );

    let Some(initial_track_bindings) = read_track_snapshot(&mut bob).await else {
        panic!("missing initial track snapshot");
    };
    assert_eq!(initial_track_bindings.len(), 1);
    assert_track_snapshot_contains(
        &initial_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Camera,
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should answer the first rtc renegotiation so the initial consumer is committed"
    );
    let Some(ServerMessage::PeerInfo(first_publish_info)) =
        read_single_protocol_server_message(&mut bob).await
    else {
        panic!("subscriber should receive the camera publish peer-info update");
    };
    assert_eq!(first_publish_info.user_id, ProtocolSessionId::Integer(79));
    assert_eq!(first_publish_info.info.is_camera_on, Some(true));

    assert!(
        alice
            .publish(ProtocolStreamType::Screen, true)
            .await
            .is_some()
    );
    assert!(
        read_until_server_request(&mut alice).await.is_some(),
        "publisher should answer the second rtc-backed publish renegotiation"
    );

    bob.auto_answer_negotiation = false;
    let Some(updated_track_bindings) =
        read_track_snapshot_until_pending_negotiations(&mut bob, 1).await
    else {
        panic!("missing updated track snapshot");
    };
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "subscriber should keep the second renegotiation pending until the harness answers it"
    );
    assert_eq!(updated_track_bindings.len(), 2);
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Camera,
    );
    assert_track_snapshot_contains(
        &updated_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Screen,
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the unpublish renegotiation while the subscriber still has the later addition offer pending"
    );
    let Some(removed_track_bindings) =
        read_track_snapshot_until_pending_negotiations(&mut bob, 1).await
    else {
        panic!("missing removed track snapshot");
    };
    assert_eq!(removed_track_bindings.len(), 1);
    assert_track_snapshot_contains(
        &removed_track_bindings,
        &ProtocolSessionId::Integer(79),
        ProtocolStreamType::Screen,
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "subscriber removal should not create an overlapping renegotiation while the later addition answer is pending"
    );
    let unpublish_info = ProtocolSessionInfo {
        is_camera_on: Some(false),
        is_screen_sharing_on: Some(true),
        ..ProtocolSessionInfo::default().snapshot_complete()
    };
    let unpublish_update = BundleUpdate::SessionInfoChange(BTreeMap::from([(
        bundle_session_info_key(&ProtocolSessionId::Integer(79)),
        unpublish_info.clone(),
    )]));
    if bob.updates.last() != Some(&unpublish_update) {
        assert!(
            consume_peer_info_update(&mut bob, ProtocolSessionId::Integer(79), unpublish_info)
                .await
                .is_some(),
            "subscriber should receive the translated peer-info update for the unpublished track"
        );
    }
    assert_eq!(
        bob.updates.last(),
        Some(&unpublish_update),
        "queued removal should immediately update the publisher's observable stream info"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber removal should stay queued until the in-flight addition answer lands"
    );

    assert!(
        bob.answer_next_negotiation().await.is_some(),
        "subscriber should answer the pending addition negotiation"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the queued removal renegotiation after the addition answer lands"
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "queued removal should surface exactly one follow-up negotiation request after the pending answer lands"
    );
    assert!(
        bob.answer_next_negotiation().await.is_some(),
        "subscriber should answer the queued removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "queued subscriber removal should not leave more websocket frames after the removal answer"
    );
    assert!(
        close_peer_and_wait_for_room_cleanup(&mut alice, &room, &UserId::Integer(79))
            .await
            .is_some(),
        "publisher websocket should close cleanly before the test server is dropped"
    );
    assert!(
        close_peer_and_wait_for_room_cleanup(&mut bob, &room, &UserId::Integer(80))
            .await
            .is_some(),
        "subscriber websocket should close cleanly before the test server is dropped"
    );
}
