use super::support::*;

#[tokio::test]
async fn protocol_core_replays_real_server_welcome_peer_snapshot() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let existing_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(31));
    let joining_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(32));
    assert!(existing_token.is_some());
    assert!(joining_token.is_some());
    let Some(existing_token) = existing_token else {
        return;
    };
    let Some(joining_token) = joining_token else {
        return;
    };

    let existing_socket = authenticate_with_jwt(&server, &existing_token).await;
    assert!(existing_socket.is_some());
    let Some(mut existing_socket) = existing_socket else {
        return;
    };
    let existing_welcome = read_welcome(&mut existing_socket).await;
    assert!(
        existing_welcome.is_some(),
        "existing peer should complete handshake"
    );

    let mut peer = ProtocolHarnessPeer::default();
    let connected = peer
        .connect(&format!("ws://{}/", server.addr), &joining_token, None)
        .await;
    assert!(
        connected.is_some(),
        "protocol core should connect to test server"
    );
    let read_frame = peer.read_server_frame().await;
    assert!(
        read_frame.is_some(),
        "protocol core should receive the welcome frame"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Authenticated);
    assert_eq!(
        peer.core.features(),
        &AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );
    assert_eq!(
        peer.core.recording_state(),
        &RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert_eq!(
        peer.state_changes,
        vec![
            BundleStateChange {
                state: BundleConnectionState::Connecting,
                cause: None,
            },
            BundleStateChange {
                state: BundleConnectionState::Authenticated,
                cause: None,
            },
        ]
    );
    assert_eq!(
        peer.updates,
        vec![BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(31)),
            ProtocolSessionInfo::snapshot_defaults(),
        )]))]
    );
}

#[tokio::test]
async fn protocol_core_maps_real_server_auth_failure_to_closed_state() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;

    let mut peer = ProtocolHarnessPeer::default();
    let connected = peer
        .connect(
            &format!("ws://{}/", server.addr),
            "invalid.jwt.payload",
            Some(room.uuid().to_owned()),
        )
        .await;
    assert!(connected.is_some(), "protocol core should open websocket");

    let close_code = read_close_code(match peer.websocket.as_mut() {
        Some(websocket) => websocket,
        None => return,
    })
    .await;
    assert_eq!(close_code, Some(CloseCode::Library(4106)));

    let observed = peer.observe_close(4106).await;
    assert!(
        observed.is_some(),
        "protocol core should consume the auth failure close code"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Closed);
    assert_eq!(
        peer.state_changes,
        vec![
            BundleStateChange {
                state: BundleConnectionState::Connecting,
                cause: None,
            },
            BundleStateChange {
                state: BundleConnectionState::Closed,
                cause: Some(String::from("auth_failed")),
            },
        ]
    );
    assert!(peer.timers.is_empty());
}

#[tokio::test]
async fn protocol_core_answers_real_server_offer_when_enabled() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-protocol", CreateRoomQuery::default()).await;
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(33));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let mut peer = ProtocolHarnessPeer::default();
    let connected = peer
        .connect(&format!("ws://{}/", server.addr), &token, None)
        .await;
    assert!(
        connected.is_some(),
        "protocol core should connect to the protocol test server"
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "protocol core should consume the welcome frame"
    );
    assert!(
        peer.read_server_frame().await.is_some(),
        "protocol core should consume and answer the protocol offer"
    );

    assert_eq!(peer.core.state(), BundleConnectionState::Connected);
    assert!(
        peer.state_changes.iter().any(|change| {
            change.state == BundleConnectionState::Connected && change.cause.is_none()
        }),
        "protocol offer handling should drive the protocol core into the connected state"
    );
}

#[tokio::test]
async fn protocol_core_receives_protocol_broadcast_and_peer_updates() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-events",
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(41));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(42));
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(42))
            .await
            .is_some(),
        "existing peers should consume the protocol peer-joined update after a new user joins"
    );
    bob.updates.clear();

    assert!(alice.broadcast(json!({ "text": "hello" })).await.is_some());
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated broadcast"
    );
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::Broadcast(BundleBroadcastUpdate {
            sender_id: ProtocolSessionId::Integer(41),
            message: json!({ "text": "hello" }),
        }))
    );

    assert!(
        alice
            .update_info(ProtocolSessionInfo {
                is_talking: Some(true),
                ..ProtocolSessionInfo::default()
            })
            .await
            .is_some()
    );
    assert!(
        bob.read_server_frame().await.is_some(),
        "bob should consume translated peer info"
    );
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(41)),
            ProtocolSessionInfo {
                is_talking: Some(true),
                ..ProtocolSessionInfo::snapshot_defaults()
            },
        )])))
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "alice should consume its own translated peer info frame before disconnect assertions"
    );

    let close_result = match bob.websocket.as_mut() {
        Some(websocket) => websocket.close(None).await,
        None => return,
    };
    assert!(close_result.is_ok());
    bob.websocket = None;
    sleep(Duration::from_millis(50)).await;

    assert!(
        alice.read_server_frame().await.is_some(),
        "alice should consume translated peer disconnect"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::Disconnect(BundleDisconnectUpdate {
            user_id: ProtocolSessionId::Integer(42),
        }))
    );
}

#[tokio::test]
async fn protocol_user_emits_peerjoined_message_for_existing_peers() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-peerjoined",
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(43));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(44));
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

    let Some(alice_websocket) = alice.websocket.as_mut() else {
        return;
    };
    let Some(peer_joined_payload) =
        timeout(Duration::from_secs(1), read_text_message(alice_websocket))
            .await
            .ok()
            .flatten()
    else {
        panic!("existing peer should receive a peerjoined message");
    };
    let peer_joined_batch = serde_json::from_str::<EnvelopeBatch>(&peer_joined_payload).ok();
    assert!(peer_joined_batch.is_some());
    let Some(peer_joined_batch) = peer_joined_batch else {
        return;
    };
    let peer_joined_messages = protocol_server_messages(&peer_joined_batch);
    assert!(peer_joined_messages.is_some());
    let Some(peer_joined_messages) = peer_joined_messages else {
        return;
    };
    assert!(
        matches!(
            peer_joined_messages.as_slice(),
            [ServerMessage::PeerJoined(_)]
        ),
        "existing peers should receive peerjoined rather than a generic peerinfo frame on join"
    );

    let peer_joined_commands = alice.core.on_ws_message(&peer_joined_payload);
    assert!(alice.run_commands(peer_joined_commands).await.is_some());
}

#[tokio::test]
async fn protocol_user_replacement_emits_peerleft_then_peerjoined_for_existing_peers() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-protocol-peer-replacement",
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(45));
    let bob_token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(46));
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
        consume_peer_joined_update(&mut alice, ProtocolSessionId::Integer(46))
            .await
            .is_some()
    );
    alice.updates.clear();

    let mut replacement = ProtocolHarnessPeer::default();
    assert!(
        replacement
            .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
            .await
            .is_some()
    );

    let close_code = read_close_code(match bob.websocket.as_mut() {
        Some(websocket) => websocket,
        None => return,
    })
    .await;
    assert_eq!(close_code, Some(CloseCode::Library(4108)));

    assert!(
        matches!(
            read_single_protocol_server_message(&mut alice).await,
            Some(ServerMessage::PeerLeft(_))
        ),
        "replacement should emit peerleft before rejoin"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::Disconnect(BundleDisconnectUpdate {
            user_id: ProtocolSessionId::Integer(46),
        }))
    );

    assert!(
        matches!(
            read_single_protocol_server_message(&mut alice).await,
            Some(ServerMessage::PeerJoined(_))
        ),
        "replacement should emit peerjoined after peerleft"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(46)),
            ProtocolSessionInfo::snapshot_defaults(),
        )]))),
    );
}
