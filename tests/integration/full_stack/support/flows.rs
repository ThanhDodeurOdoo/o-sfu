use super::{
    media::{
        RouteState, assert_consumer_route, assert_packet_dropped, assert_packet_forwarded,
        publish_source_and_ready_route,
    },
    protocol::{
        assert_departure_message_protocol, assert_empty_track_snapshot,
        assert_peer_joined_message_protocol, assert_track_snapshot,
    },
    *,
};

pub(crate) async fn assert_replacement_unpublish_and_republish_flow(
    server: &TestServer,
    room: &str,
    initial_publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
) -> TestResult {
    let mut source = FakeMediaSource::audio();
    let mut clock = FakeClock::default();
    let harness = AudioRouteHarness::new(server, room, &publisher_user_id);
    assert_published_audio_forwarding(
        &harness,
        initial_publisher,
        subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    let mut replacement = require_some(
        connect_fake_peer(server, room, publisher_user_id.clone(), TEST_ROOM_KEY).await,
        "replacement peer should connect",
    )?;
    assert_replacement_audio_forwarding(
        &harness,
        initial_publisher,
        &mut replacement,
        subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    assert_replacement_unpublish_and_republish_audio(
        &harness,
        &mut replacement,
        subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    Ok(())
}

struct AudioRouteHarness<'a> {
    server: &'a TestServer,
    room: &'a str,
    publisher_user_id: &'a UserId,
}

impl<'a> AudioRouteHarness<'a> {
    const fn new(server: &'a TestServer, room: &'a str, publisher_user_id: &'a UserId) -> Self {
        Self {
            server,
            room,
            publisher_user_id,
        }
    }
}

async fn assert_published_audio_forwarding(
    harness: &AudioRouteHarness<'_>,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    publish_source_and_ready_route(
        harness.server,
        harness.room,
        publisher,
        subscriber,
        harness.publisher_user_id,
        source,
    )
    .await;
    assert_packet_forwarded(publisher, subscriber, source, clock).await;
}

async fn assert_replacement_audio_forwarding(
    harness: &AudioRouteHarness<'_>,
    initial_publisher: &mut ProtocolFakePeer,
    replacement: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(subscriber, harness.publisher_user_id.clone()).await;
    assert_peer_joined_message_protocol(subscriber, harness.publisher_user_id.clone()).await;
    assert_packet_dropped(initial_publisher, subscriber, source, clock).await;
    assert!(
        replacement
            .wait_until_connected(super::Duration::from_secs(5))
            .await
            .is_some()
    );
    assert_published_audio_forwarding(harness, replacement, subscriber, source, clock).await;
}

async fn assert_replacement_unpublish_and_republish_audio(
    harness: &AudioRouteHarness<'_>,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert!(
        publisher
            .set_publication_active(StreamType::Audio, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_empty_track_snapshot(subscriber).await;
    assert_consumer_route(
        harness.server,
        harness.room,
        subscriber,
        harness.publisher_user_id,
        StreamType::Audio,
        RouteState::Absent,
    )
    .await;
    assert_packet_dropped(publisher, subscriber, source, clock).await;
    assert_published_audio_forwarding(harness, publisher, subscriber, source, clock).await;
}

pub(crate) async fn assert_subscriber_replacement_preserves_download_mute_after_renegotiation(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) -> TestResult {
    let mut source = FakeMediaSource::audio();
    let muted_stream_type =
        mute_subscriber_audio_download(server, room, publisher, subscriber, &mut source).await;
    Box::pin(assert_replacement_subscriber_inherits_muted_audio_download(
        server,
        room,
        publisher,
        subscriber,
        muted_stream_type,
        &mut source,
    ))
    .await
}

async fn mute_subscriber_audio_download(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
) -> StreamType {
    let track_binding = publish_source_and_ready_route(
        server,
        room,
        publisher,
        subscriber,
        &UserId::Integer(82),
        source,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_packet_forwarded(publisher, subscriber, source, &mut clock).await;
    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(82),
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
        subscriber,
        &UserId::Integer(82),
        track_binding.stream_type,
        RouteState::Inactive,
    )
    .await;
    track_binding.stream_type
}

async fn assert_replacement_subscriber_inherits_muted_audio_download(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    muted_stream_type: StreamType,
    source: &mut FakeMediaSource,
) -> TestResult {
    let mut replacement = require_some(
        connect_fake_peer(server, room, UserId::Integer(83), TEST_ROOM_KEY).await,
        "replacement subscriber should connect",
    )?;
    assert_eq!(
        subscriber.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(publisher, UserId::Integer(83)).await;
    assert_peer_joined_message_protocol(publisher, UserId::Integer(83)).await;
    assert!(
        replacement
            .wait_until_connected(super::Duration::from_secs(5))
            .await
            .is_some()
    );
    let replacement_track = assert_track_snapshot(
        &mut replacement,
        UserId::Integer(82),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_consumer_route(
        server,
        room,
        &replacement,
        &UserId::Integer(82),
        replacement_track.stream_type,
        RouteState::Inactive,
    )
    .await;

    assert_eq!(muted_stream_type, replacement_track.stream_type);
    let mut clock = FakeClock::default();
    assert_packet_dropped(publisher, &mut replacement, source, &mut clock).await;
    Ok(())
}

pub(crate) async fn assert_audio_media_arrives_and_download_mute_stops_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    let track_binding = publish_source_and_ready_route(
        server,
        room,
        publisher,
        subscriber,
        &UserId::Integer(70),
        &source,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;
    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(70),
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
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
        RouteState::Inactive,
    )
    .await;
    assert_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
}

pub(crate) async fn assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    let track_binding = publish_source_and_ready_route(
        server,
        room,
        publisher,
        subscriber,
        &UserId::Integer(70),
        &source,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;
    assert!(
        publisher
            .set_publication_active(StreamType::Audio, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_empty_track_snapshot(subscriber).await;
    assert_consumer_route(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
        RouteState::Absent,
    )
    .await;
    assert_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
}
