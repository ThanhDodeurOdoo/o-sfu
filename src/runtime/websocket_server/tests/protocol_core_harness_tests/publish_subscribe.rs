use super::support::*;

#[tokio::test]
async fn protocol_core_receives_translated_track_snapshot_and_explicit_unpublish_removal() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-tracks",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(51));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(52));
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(52))
            .await
            .is_some()
    );

    let producer_id = room
        .test_api()
        .media()
        .publish_intent(
            &UserId::Integer(51),
            &source_publish_intent_for_stream_type(StreamType::Camera),
            MediaKind::Video,
            sample_video_rtp_parameters("cam-0"),
            &server.media_transport,
        )
        .await;
    assert!(producer_id.is_some(), "protocol publisher should be ready");

    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated tracks snapshot"
    );
    let Some(track_binding) = bob.core.track_binding("cam-0") else {
        panic!("subscriber should keep the camera track binding");
    };
    assert_eq!(track_binding.user_id, ProtocolSessionId::Integer(51));
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    let Some(source) = track_binding.source.as_ref() else {
        panic!("track binding should carry the additive source descriptor");
    };
    assert_eq!(source.source_id, "source-1");
    assert_eq!(source.user_id, ProtocolSessionId::Integer(51));
    assert_eq!(source.stream_type, ProtocolStreamType::Camera);
    assert_eq!(source.mid.as_deref(), Some("cam-0"));
    assert_eq!(source.encodings.len(), 1);
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume the serialized renegotiation request after track bootstrap"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the removal renegotiation request"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume the translated track-removal snapshot"
    );
    assert_eq!(bob.core.track_binding("cam-0"), None);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the removal renegotiation request"
    );
}

#[tokio::test]
async fn protocol_core_publish_round_trips_through_real_server_user_protocol() {
    let adapter = Arc::new(FakeMediaTransport::default());
    let server = TestServerBuilder::new()
        .media_transport(MediaTransport::from_fake_transport(Arc::clone(&adapter)))
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-publish",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(53));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(54));
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(54))
            .await
            .is_some()
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should consume the renegotiation request and answer it"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the translated track snapshot after publish commit"
    );
    let Some(track_binding) = bob.core.track_binding("fake-mid-0") else {
        panic!("subscriber should keep the camera track binding");
    };
    assert_eq!(track_binding.user_id, ProtocolSessionId::Integer(53));
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    let Some(source) = track_binding.source.as_ref() else {
        panic!("track binding should carry the additive source descriptor");
    };
    assert_eq!(source.user_id, ProtocolSessionId::Integer(53));
    assert_eq!(source.stream_type, ProtocolStreamType::Camera);
    assert_eq!(source.mid.as_deref(), Some("fake-mid-0"));
    assert_eq!(source.encodings.len(), 1);
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the follow-up renegotiation request for the new remote track"
    );
    assert!(
        adapter.snapshot_events().iter().any(|event| matches!(
            event,
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id,
                media_kind,
            } if *user_id == UserId::Integer(53)
                && *media_kind == MediaKind::Video
        )),
        "protocol publish should declare producer media through the media transport"
    );
}

#[tokio::test]
async fn protocol_core_publish_round_trips_through_real_rtc_server_user_protocol() {
    let server = TestServerBuilder::new()
        .media_transport(build_real_rtc_media_transport())
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-rtc-publish",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(71));
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(72));
    assert!(alice_token.is_some());
    assert!(bob_token.is_some());
    let (Some(alice_token), Some(bob_token)) = (alice_token, bob_token) else {
        return;
    };

    let Some(mut alice) = ProtocolHarnessPeer::with_real_rtc_negotiation(56_301) else {
        return;
    };
    let Some(mut bob) = ProtocolHarnessPeer::with_real_rtc_negotiation(56_302) else {
        return;
    };
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(72))
            .await
            .is_some()
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should consume the rtc-backed renegotiation request and answer it"
    );

    let Some(bob_websocket) = bob.websocket.as_mut() else {
        return;
    };
    let Some(track_snapshot_payload) =
        timeout(Duration::from_secs(1), read_text_message(bob_websocket))
            .await
            .ok()
            .flatten()
    else {
        return;
    };
    let track_batch = serde_json::from_str::<EnvelopeBatch>(&track_snapshot_payload).ok();
    assert!(track_batch.is_some());
    let Some(track_batch) = track_batch else {
        return;
    };
    let track_messages = protocol_server_messages(&track_batch);
    assert!(track_messages.is_some());
    let Some(track_messages) = track_messages else {
        return;
    };
    let Some(first_track_message) = track_messages.first() else {
        return;
    };
    assert_eq!(track_messages.len(), 1);
    assert!(matches!(first_track_message, ServerMessage::Tracks(_)));
    let Some(ServerMessage::Tracks(track_bindings)) = track_messages.into_iter().next() else {
        return;
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(published_track) = track_bindings.first() else {
        return;
    };
    assert_eq!(published_track.user_id, ProtocolSessionId::Integer(71));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    let track_commands = bob.core.on_ws_message(&track_snapshot_payload);
    assert!(bob.run_commands(track_commands).await.is_some());
    assert_eq!(
        bob.core.track_binding(&published_track.mid),
        Some(published_track)
    );

    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the rtc-backed follow-up renegotiation request"
    );
}

#[tokio::test]
async fn protocol_handshake_uses_answer_derived_client_capabilities_for_user_state() {
    let server = TestServerBuilder::new()
        .media_transport(build_real_rtc_media_transport())
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-rtc-capabilities",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), UserId::Integer(75));
    assert!(alice_token.is_some());
    let Some(alice_token) = alice_token else {
        return;
    };
    let Some(mut alice) =
        ProtocolHarnessPeer::with_custom_rtc_negotiation(56_305, reduced_capability_rtc)
    else {
        return;
    };

    assert!(
        alice
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
            .await
            .is_some()
    );

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
    let Some(codec_names) = client_rtp_codec_names.ok() else {
        return;
    };
    assert_eq!(codec_names, vec![String::from("opus"), String::from("VP8")]);
    assert!(
        codec_names.iter().all(|codec| codec != "rtx"),
        "the production VP8 receive surface should not preserve RTX while RID repair demux is disabled"
    );
    assert!(
        codec_names.iter().all(|codec| codec != "H264"),
        "the stored client RTP capabilities must reflect the real RTC answer"
    );
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
        consume_camera_publish_bootstrap(
            &mut bob,
            &ProtocolSessionId::Integer(73),
            "subscriber should receive the initial camera track snapshot",
        )
        .await
        .is_some(),
        "subscriber should consume the committed camera publish bootstrap"
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
        bob.core.track_binding(&published_track.mid),
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
                ..ProtocolSessionInfo::snapshot_defaults()
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
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
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
        return;
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

    assert!(
        alice
            .publish(ProtocolStreamType::Screen, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should answer the second rtc-backed publish renegotiation"
    );

    let Some(updated_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
    };
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

    bob.auto_answer_negotiation = false;
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the second rtc renegotiation request while the first consumer is already committed"
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "subscriber should keep the second renegotiation pending until the harness answers it"
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
    let Some(removed_track_bindings) = read_track_snapshot(&mut bob).await else {
        return;
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
    let Some(bob_websocket) = bob.websocket.as_mut() else {
        return;
    };
    let Some(peer_info_payload) =
        timeout(Duration::from_millis(150), read_text_message(bob_websocket))
            .await
            .ok()
            .flatten()
    else {
        panic!(
            "subscriber should receive the translated peer-info update for the unpublished track"
        );
    };
    let peer_info_batch = serde_json::from_str::<EnvelopeBatch>(&peer_info_payload).ok();
    assert!(peer_info_batch.is_some());
    let Some(peer_info_batch) = peer_info_batch else {
        return;
    };
    let peer_info_messages = protocol_server_messages(&peer_info_batch);
    assert!(peer_info_messages.is_some());
    let Some(peer_info_messages) = peer_info_messages else {
        return;
    };
    assert!(
        matches!(peer_info_messages.as_slice(), [ServerMessage::PeerInfo(_)]),
        "the frame before the queued removal renegotiation should be the translated peer-info update"
    );
    let peer_info_commands = bob.core.on_ws_message(&peer_info_payload);
    assert!(bob.run_commands(peer_info_commands).await.is_some());
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber removal should stay queued until the in-flight addition answer lands"
    );

    assert!(
        bob.answer_next_negotiation().await.is_some(),
        "subscriber should answer the second renegotiation before the queued removal can flush"
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "subscriber should receive the queued follow-up renegotiation after answering the in-flight addition offer"
    );
    assert_eq!(
        bob.pending_negotiations.len(),
        1,
        "queued consumer removal should surface exactly one follow-up renegotiation request"
    );

    assert!(
        bob.answer_next_negotiation().await.is_some(),
        "subscriber should answer the queued removal renegotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "consumer removal should not leave further rtc follow-up frames queued"
    );
}
