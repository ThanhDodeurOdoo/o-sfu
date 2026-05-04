use super::*;

pub(crate) async fn publish_camera_and_bootstrap_subscriber(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
    publish_context: &str,
    renegotiation_context: &str,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    assert!(
        publisher
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some(),
        "{publish_context}"
    );
    assert!(
        publisher.read_server_frame().await.is_some(),
        "{renegotiation_context}"
    );
    let track_snapshot = read_track_snapshot(subscriber).await;
    assert!(track_snapshot.is_some(), "{snapshot_context}");
    let track_snapshot = track_snapshot?;
    let track_binding = track_snapshot.first()?;
    assert_eq!(track_binding.user_id, publisher_user_id.clone());
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the remote-track renegotiation request"
    );
    Some(track_binding.clone())
}

pub(crate) async fn recover_subscriber_and_replay_track(
    publisher: &mut ProtocolHarnessPeer,
    subscriber: &mut ProtocolHarnessPeer,
    publisher_user_id: &UserId,
    reconnect_context: &str,
    welcome_context: &str,
    offer_context: &str,
    snapshot_context: &str,
) -> Option<TrackBinding> {
    assert!(
        close_peer_and_observe_recovery(subscriber, publisher)
            .await
            .is_some()
    );
    assert!(
        subscriber
            .flush_timers_with_delay(RECOVERY_DELAY_MS)
            .await
            .is_some(),
        "{reconnect_context}"
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "{welcome_context}"
    );
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "{offer_context}"
    );
    let replayed_track_snapshot = read_track_snapshot(subscriber).await;
    assert!(replayed_track_snapshot.is_some(), "{snapshot_context}");
    let replayed_track_snapshot = replayed_track_snapshot?;
    assert_track_snapshot_contains(
        &replayed_track_snapshot,
        &publisher_user_id.clone(),
        ProtocolStreamType::Camera,
    );
    let replayed_track = replayed_track_snapshot.first()?;
    assert!(
        subscriber.read_server_frame().await.is_some(),
        "subscriber should consume the replayed remote-track renegotiation request"
    );
    Some(replayed_track.clone())
}

pub(crate) async fn setup_fake_protocol_peers(
    adapter: Arc<FakeMediaTransport>,
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
) -> Option<(
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_test_server_with_timeouts(
        1_000,
        10_000,
        60_000,
        100,
        MediaTransport::from_fake_transport(adapter),
    )
    .await?;
    let room = create_room(&server, room_name, None, CreateRoomQuery::default()).await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), alice_user_id.clone())?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), bob_user_id.clone())?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, bob_user_id.clone()).await?;
    Some((server, room, alice, bob))
}

pub(crate) async fn setup_real_rtc_protocol_peers(
    room_name: &str,
    alice_user_id: UserId,
    bob_user_id: UserId,
    alice_port: u16,
    bob_port: u16,
) -> Option<(
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_protocol_rtc_test_server(1_000, 100).await?;
    let room = create_room(&server, room_name, None, CreateRoomQuery::default()).await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), alice_user_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), bob_user_id.clone())?;

    let mut alice = ProtocolHarnessPeer::with_real_rtc_negotiation(alice_port)?;
    let mut bob = ProtocolHarnessPeer::with_real_rtc_negotiation(bob_port)?;
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, bob_user_id.clone()).await?;

    Some((server, room, alice, bob))
}

pub(crate) async fn setup_protocol_recovery_peers(
    alice_user_id: UserId,
    bob_user_id: UserId,
) -> Option<(
    TestServer,
    Arc<Room>,
    ProtocolHarnessPeer,
    ProtocolHarnessPeer,
)> {
    let server = spawn_protocol_test_server(1_000, 100).await?;
    let room = create_room(
        &server,
        "issuer-protocol-recovery",
        None,
        CreateRoomQuery::default(),
    )
    .await;
    let alice_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), alice_user_id)?;
    let bob_token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), bob_user_id.clone())?;

    let mut alice = ProtocolHarnessPeer::default();
    let mut bob = ProtocolHarnessPeer::default();
    alice
        .connect_and_finish_handshake(&format!("ws://{}/", server.addr), &alice_token, None)
        .await?;
    bob.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &bob_token, None)
        .await?;
    consume_peer_joined_update(&mut alice, bob_user_id.clone()).await?;
    Some((server, room, alice, bob))
}

pub(crate) async fn bob_update_info_and_deliver(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    alice.read_server_frame().await?;
    Some(())
}

pub(crate) async fn close_peer_and_observe_recovery(
    bob: &mut ProtocolHarnessPeer,
    alice: &mut ProtocolHarnessPeer,
) -> Option<()> {
    bob.websocket.as_mut()?.close(None).await.ok()?;
    bob.websocket = None;
    bob.observe_close(1011).await?;
    alice.read_server_frame().await?;
    Some(())
}

pub(crate) async fn recover_peer_with_latest_info(
    bob: &mut ProtocolHarnessPeer,
    info: ProtocolSessionInfo,
) -> Option<()> {
    bob.update_info(info).await?;
    bob.flush_timers_with_delay(RECOVERY_DELAY_MS).await?;
    bob.read_server_frame().await?;
    bob.read_server_frame().await?;
    assert!(bob.websocket.is_some());
    Some(())
}
