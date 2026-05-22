use super::{
    media::{
        RouteState, assert_consumer_route, assert_packet_dropped,
        assert_synthetic_video_packet_forwarded, consume_video_source_and_ready_route,
        publish_source_and_ready_route,
    },
    protocol::{
        assert_departure_message_protocol, assert_peer_joined_message_protocol,
        assert_track_snapshot,
    },
    setup::room_parts_with_config,
    *,
};

pub(crate) struct SpilloverRoomFakePeers {
    pub(crate) server: TestServer,
    pub(crate) room: String,
    pub(crate) publisher: ProtocolFakePeer,
    pub(crate) local_subscriber: ProtocolFakePeer,
    pub(crate) spillover_subscriber: ProtocolFakePeer,
}

pub(crate) async fn spillover_room_fake_peers(
    issuer: &str,
    publisher_user_id: UserId,
    local_subscriber_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> TestResult<SpilloverRoomFakePeers> {
    let (server, room) =
        room_parts_with_config(load_triggered_spillover_test_config(), issuer).await?;
    let (publisher, local_subscriber, spillover_subscriber) = require_some(
        Box::pin(connect_load_triggered_spillover_rtc_peers(
            &server,
            &room,
            publisher_user_id,
            local_subscriber_user_id,
            spillover_subscriber_user_id,
        ))
        .await,
        "load-triggered spillover peers should connect",
    )?;
    Ok(SpilloverRoomFakePeers {
        server,
        room,
        publisher,
        local_subscriber,
        spillover_subscriber,
    })
}

fn load_triggered_spillover_test_config() -> Config {
    let mut config = super::test_config(1_000, 10);
    let policy = match LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
        min_receiver_count: 3,
        activation_window: 1,
        ..LocalSpilloverPolicyParts::conservative()
    }) {
        Ok(policy) => policy,
        Err(error) => panic!("load-triggered spillover test policy should be valid: {error}"),
    };
    config.transport.rtc_media_worker_count = 2;
    config.transport.room_worker_policy =
        RoomWorkerPolicy::load_triggered_local_spillover(2, policy);
    config
}

pub(crate) async fn assert_cross_worker_placement(
    server: &TestServer,
    room: &str,
    publisher_user_id: &UserId,
    subscriber_user_id: &UserId,
) {
    assert_user_media_worker(server, room, publisher_user_id, 0).await;
    assert_user_media_worker(server, room, subscriber_user_id, 1).await;
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
        connect_load_triggered_peer_on_worker(server, room, &publisher_user_id, 0, []).await?;
    let mut local_subscriber = connect_load_triggered_peer_on_worker(
        server,
        room,
        &local_subscriber_user_id,
        0,
        [&mut publisher],
    )
    .await?;
    let mut activation_peer =
        connect_fake_peer(server, room, activation_user_id.clone(), TEST_ROOM_KEY).await?;
    activation_peer
        .wait_until_connected(super::Duration::from_secs(5))
        .await?;
    assert_peer_joined_message_protocol(&mut publisher, activation_user_id.clone()).await;
    assert_peer_joined_message_protocol(&mut local_subscriber, activation_user_id.clone()).await;
    let mut spillover_subscriber = connect_load_triggered_peer_on_worker(
        server,
        room,
        &spillover_subscriber_user_id,
        1,
        [&mut publisher, &mut local_subscriber],
    )
    .await?;
    for (user_id, worker_id) in [
        (&publisher_user_id, 0),
        (&local_subscriber_user_id, 0),
        (&spillover_subscriber_user_id, 1),
    ] {
        assert_user_media_worker(server, room, user_id, worker_id).await;
    }

    activation_peer.close().await?;
    assert_departure_message_protocol(&mut publisher, activation_user_id.clone()).await;
    assert_departure_message_protocol(&mut local_subscriber, activation_user_id.clone()).await;
    assert_departure_message_protocol(&mut spillover_subscriber, activation_user_id).await;

    Some((publisher, local_subscriber, spillover_subscriber))
}

async fn connect_load_triggered_peer_on_worker<const N: usize>(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    worker_id: usize,
    mut joined_observers: [&mut ProtocolFakePeer; N],
) -> Option<ProtocolFakePeer> {
    let mut peer = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    peer.wait_until_connected(super::Duration::from_secs(5))
        .await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    for observer in &mut joined_observers {
        assert_peer_joined_message_protocol(observer, user_id.clone()).await;
    }
    Some(peer)
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
        .wait_until_connected(super::Duration::from_secs(5))
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

pub(crate) async fn assert_load_triggered_spillover_release_route_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    mut spillover_subscriber: ProtocolFakePeer,
    publisher_user_id: &UserId,
    spillover_subscriber_user_id: &UserId,
) -> TestResult {
    let mut source = FakeMediaSource::vp8_camera_high();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let local_track =
        consume_video_source_and_ready_route(server, room, local_subscriber, publisher_user_id)
            .await;
    let spillover_track = consume_video_source_and_ready_route(
        server,
        room,
        &mut spillover_subscriber,
        publisher_user_id,
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
            .read_rtp_packet(super::Duration::from_secs(2))
            .await
            .is_some()
    );

    require_some(
        spillover_subscriber.close().await,
        "spillover subscriber should close",
    )?;
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
    assert_consumer_route(
        server,
        room,
        local_subscriber,
        publisher_user_id,
        local_track.stream_type,
        RouteState::Active,
    )
    .await;
    assert_synthetic_video_packet_forwarded(publisher, local_subscriber, &mut source, &mut clock)
        .await;
    Ok(())
}

pub(crate) async fn assert_load_triggered_spillover_replacement_mute_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    spillover_subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> TestResult {
    let mut source = FakeMediaSource::audio();
    let track_binding = publish_source_and_ready_route(
        server,
        room,
        publisher,
        spillover_subscriber,
        &publisher_user_id,
        &source,
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
    assert_consumer_route(
        server,
        room,
        spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
        RouteState::Inactive,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_packet_dropped(publisher, spillover_subscriber, &mut source, &mut clock).await;

    let mut replacement = require_some(
        connect_load_triggered_spillover_replacement(
            server,
            room,
            spillover_subscriber,
            &spillover_subscriber_user_id,
            1,
        )
        .await,
        "spillover replacement should connect",
    )?;

    let replacement_track = assert_track_snapshot(
        &mut replacement,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_consumer_route(
        server,
        room,
        &replacement,
        &publisher_user_id,
        replacement_track.stream_type,
        RouteState::Inactive,
    )
    .await;
    assert_packet_dropped(publisher, &mut replacement, &mut source, &mut clock).await;
    Ok(())
}
