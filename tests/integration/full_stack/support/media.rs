use super::{protocol::assert_track_snapshot, setup::assert_video_subscription_enabled, *};

pub(crate) async fn publish_source_and_ready_route(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track = assert_track_snapshot(
        subscriber,
        publisher_user_id.clone(),
        source.stream_type(),
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_consumer_route(
        server,
        room,
        subscriber,
        publisher_user_id,
        track.stream_type,
        RouteState::Active,
    )
    .await;
    track
}

pub(crate) async fn consume_video_source_and_ready_route(
    server: &TestServer,
    room: &str,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
) -> TrackBinding {
    let track_binding = assert_track_snapshot(
        subscriber,
        publisher_user_id.clone(),
        StreamType::Camera,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_video_subscription_enabled(subscriber, publisher_user_id.clone()).await;
    assert_consumer_route(
        server,
        room,
        subscriber,
        publisher_user_id,
        track_binding.stream_type,
        RouteState::Active,
    )
    .await;
    track_binding
}

pub(crate) async fn publish_video_source_and_ready_route(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    consume_video_source_and_ready_route(server, room, subscriber, publisher_user_id).await
}

pub(crate) async fn assert_video_subscription_selected_rid(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    rid: &str,
) {
    assert!(
        server
            .wait_for_video_subscription_selected_rid(
                room,
                subscriber.user_id(),
                publisher_user_id,
                rid,
            )
            .await
    );
}

pub(crate) async fn assert_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    let Some(expected_payload) = publisher.send_rtp_packet(source, clock).await else {
        panic!("synthetic packet should be accepted by fake publisher");
    };
    assert!(read_expected_rtp_payload(subscriber, &expected_payload, Duration::from_secs(5)).await);
    u64::try_from(expected_payload.len()).unwrap_or(u64::MAX)
}

pub(crate) async fn assert_synthetic_video_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    for _ in 0..3 {
        let Some(expected_payload) = publisher.send_rtp_packet(source, clock).await else {
            panic!("synthetic video packet should be accepted by fake publisher");
        };
        if read_expected_rtp_payload(subscriber, &expected_payload, Duration::from_secs(5)).await {
            return u64::try_from(expected_payload.len()).unwrap_or(u64::MAX);
        }
    }
    panic!("synthetic video packet should be forwarded after route warmup");
}

pub(crate) async fn assert_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert!(publisher.send_rtp_packet(source, clock).await.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn read_expected_rtp_payload(
    subscriber: &mut ProtocolFakePeer,
    expected_payload: &[u8],
    timeout_window: Duration,
) -> bool {
    let deadline = Instant::now() + timeout_window;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let Some(received_packet) = subscriber.read_rtp_packet(deadline - now).await else {
            return false;
        };
        if received_packet.payload.as_ref() == expected_payload {
            return true;
        }
    }
}

pub(crate) enum RouteState {
    Active,
    Inactive,
}

pub(crate) async fn assert_consumer_route(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
    state: RouteState,
) {
    let routed = match state {
        RouteState::Active => {
            server
                .wait_for_consumer_route_active(
                    room,
                    subscriber.user_id(),
                    publisher_user_id,
                    stream_type,
                )
                .await
        }
        RouteState::Inactive => {
            server
                .wait_for_consumer_route_inactive(
                    room,
                    subscriber.user_id(),
                    publisher_user_id,
                    stream_type,
                )
                .await
        }
    };
    assert!(routed);
}
