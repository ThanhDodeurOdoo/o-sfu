pub(super) use std::time::{Duration, Instant};

pub(super) use o_sfu::{
    config::{Config, MediaCodecFlags, RoomWorkerPolicy},
    core::prelude::{LocalSpilloverPolicy, LocalSpilloverPolicyParts},
    http::IncomingBitRateStatsResponse,
};
pub(super) use o_sfu_protocol::{
    shared::{DownloadStates, StreamType, UserId, UserInfo, VideoLayoutIntent},
    signaling::{ServerMessage, ServerRequest, TrackBinding},
};
pub(super) use o_sfu_telemetry::diagnostics::{
    DiagnosticsActiveSpeakerReason, DiagnosticsActiveSpeakerState,
};
pub(super) use o_sfu_tests::support::{
    TEST_ROOM_KEY, TestServer, create_room,
    fake_media::{
        FakeClock, FakeMediaSource, SYNTHETIC_OPUS_ONE_FRAME_TOC, SyntheticH264Stream,
        SyntheticOpusStream, SyntheticVp8Stream,
    },
    metrics_text,
    protocol_full_stack::{
        ProtocolFakePeer, connect_fake_peer, connect_two_fake_peers,
        connect_two_rtc_ready_fake_peers,
    },
    spawn_room_server, spawn_room_server_with_config, spawn_test_server, stats, test_config,
};
pub(super) use tokio::{
    sync::{Mutex, MutexGuard},
    task::yield_now,
    time::{sleep, timeout},
};
pub(super) use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

static FULL_STACK_TEST_LOCK: Mutex<()> = Mutex::const_new(());

pub(super) async fn full_stack_test_guard() -> MutexGuard<'static, ()> {
    FULL_STACK_TEST_LOCK.lock().await
}

async fn publish_source_and_track_snapshot(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_binding = assert_track_snapshot(
        subscriber,
        publisher_user_id.clone(),
        source.stream_type(),
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    track_binding
}

pub(super) async fn publish_source_and_ready_route(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: &UserId,
    source: &FakeMediaSource,
) -> TrackBinding {
    let track_binding =
        publish_source_and_track_snapshot(publisher, subscriber, publisher_user_id, source).await;
    assert_consumer_route_active(
        server,
        room,
        subscriber,
        publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    track_binding
}

pub(super) async fn consume_video_source_and_ready_route(
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
    assert_consumer_route_active(
        server,
        room,
        subscriber,
        publisher_user_id,
        track_binding.stream_type,
    )
    .await;
    track_binding
}

pub(super) async fn publish_video_source_and_ready_route(
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

pub(super) async fn assert_video_subscription_selected_rid(
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

pub(super) async fn assert_load_triggered_spillover_release_route_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    mut spillover_subscriber: ProtocolFakePeer,
    publisher_user_id: &UserId,
    spillover_subscriber_user_id: &UserId,
) {
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
            .read_rtp_packet(Duration::from_secs(2))
            .await
            .is_some()
    );

    assert!(spillover_subscriber.close().await.is_some());
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
    assert_consumer_route_active(
        server,
        room,
        local_subscriber,
        publisher_user_id,
        local_track.stream_type,
    )
    .await;
    assert_synthetic_video_packet_forwarded(publisher, local_subscriber, &mut source, &mut clock)
        .await;
}
pub(super) async fn assert_load_triggered_spillover_replacement_mute_flow(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    spillover_subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) {
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
    assert_consumer_route_inactive(
        server,
        room,
        spillover_subscriber,
        &publisher_user_id,
        track_binding.stream_type,
    )
    .await;

    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(publisher, spillover_subscriber, &mut source, &mut clock).await;

    let replacement = connect_load_triggered_spillover_replacement(
        server,
        room,
        spillover_subscriber,
        &spillover_subscriber_user_id,
        1,
    )
    .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    let replacement_track = assert_track_snapshot(
        &mut replacement,
        publisher_user_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_consumer_route_inactive(
        server,
        room,
        &replacement,
        &publisher_user_id,
        replacement_track.stream_type,
    )
    .await;
    assert_audio_packet_dropped(publisher, &mut replacement, &mut source, &mut clock).await;
}
pub(super) async fn assert_replacement_unpublish_and_republish_flow(
    server: &TestServer,
    room: &str,
    initial_publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
) {
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

    let replacement =
        connect_fake_peer(server, room, publisher_user_id.clone(), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

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
}

pub(super) struct AudioRouteHarness<'a> {
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

pub(super) async fn assert_published_audio_forwarding(
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
    assert_audio_packet_forwarded(publisher, subscriber, source, clock).await;
}

pub(super) async fn assert_replacement_audio_forwarding(
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
    assert_audio_packet_dropped(initial_publisher, subscriber, source, clock).await;
    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert_published_audio_forwarding(harness, replacement, subscriber, source, clock).await;
}

pub(super) async fn assert_replacement_unpublish_and_republish_audio(
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
    assert_consumer_route_absent(
        harness.server,
        harness.room,
        subscriber,
        harness.publisher_user_id,
        StreamType::Audio,
    )
    .await;
    assert_audio_packet_dropped(publisher, subscriber, source, clock).await;
    assert_published_audio_forwarding(harness, publisher, subscriber, source, clock).await;
}
pub(super) async fn assert_subscriber_replacement_preserves_download_mute_after_renegotiation(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
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
    .await;
}

pub(super) async fn mute_subscriber_audio_download(
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
    assert_audio_packet_forwarded(publisher, subscriber, source, &mut clock).await;

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
    assert_consumer_route_inactive(
        server,
        room,
        subscriber,
        &UserId::Integer(82),
        track_binding.stream_type,
    )
    .await;
    track_binding.stream_type
}

pub(super) async fn assert_replacement_subscriber_inherits_muted_audio_download(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    muted_stream_type: StreamType,
    source: &mut FakeMediaSource,
) {
    let replacement = connect_fake_peer(server, room, UserId::Integer(83), TEST_ROOM_KEY).await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        subscriber.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message_protocol(publisher, UserId::Integer(83)).await;
    assert_peer_joined_message_protocol(publisher, UserId::Integer(83)).await;
    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
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
    assert_consumer_route_inactive(
        server,
        room,
        &replacement,
        &UserId::Integer(82),
        replacement_track.stream_type,
    )
    .await;

    assert_eq!(muted_stream_type, replacement_track.stream_type);
    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(publisher, &mut replacement, source, &mut clock).await;
}
pub(super) async fn connect_audio_media_flow_peers(
    server: &TestServer,
    room: &str,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    connect_two_rtc_ready_fake_peers(
        server,
        room,
        UserId::Integer(70),
        UserId::Integer(71),
        Duration::from_secs(5),
    )
    .await
}

pub(super) fn cross_worker_test_config() -> Config {
    let mut config = test_config(1_000, 10);
    config.transport.rtc_media_worker_count = 2;
    config.transport.room_worker_policy = RoomWorkerPolicy::bounded_local_spillover(2);
    config
}

pub(super) fn load_triggered_spillover_test_config() -> Config {
    let mut config = test_config(1_000, 10);
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

pub(super) async fn assert_cross_worker_placement(
    server: &TestServer,
    room: &str,
    publisher_user_id: &UserId,
    subscriber_user_id: &UserId,
) {
    assert!(
        server
            .wait_for_user_media_worker(room, publisher_user_id, 0)
            .await
    );
    assert!(
        server
            .wait_for_user_media_worker(room, subscriber_user_id, 1)
            .await
    );
}

pub(super) async fn connect_load_triggered_spillover_rtc_peers(
    server: &TestServer,
    room: &str,
    publisher_user_id: UserId,
    local_subscriber_user_id: UserId,
    spillover_subscriber_user_id: UserId,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer, ProtocolFakePeer)> {
    let activation_user_id = load_triggered_activation_user_id(&spillover_subscriber_user_id);
    let mut publisher =
        connect_load_triggered_peer_on_worker(server, room, &publisher_user_id, 0).await?;
    let mut local_subscriber = connect_load_triggered_local_subscriber(
        server,
        room,
        &mut publisher,
        &local_subscriber_user_id,
    )
    .await?;
    let activation_peer = connect_load_triggered_activation_peer(
        server,
        room,
        &mut publisher,
        &mut local_subscriber,
        &activation_user_id,
    )
    .await?;
    let mut spillover_subscriber = Box::pin(connect_load_triggered_spillover_subscriber(
        server,
        room,
        &mut publisher,
        &mut local_subscriber,
        &spillover_subscriber_user_id,
    ))
    .await?;
    assert_load_triggered_spillover_placement(
        server,
        room,
        &publisher_user_id,
        &local_subscriber_user_id,
        &spillover_subscriber_user_id,
    )
    .await;

    assert!(activation_peer.close().await.is_some());
    assert_departure_message_protocol(&mut publisher, activation_user_id.clone()).await;
    assert_departure_message_protocol(&mut local_subscriber, activation_user_id.clone()).await;
    assert_departure_message_protocol(&mut spillover_subscriber, activation_user_id).await;

    Some((publisher, local_subscriber, spillover_subscriber))
}

pub(super) async fn connect_load_triggered_peer_on_worker(
    server: &TestServer,
    room: &str,
    user_id: &UserId,
    worker_id: usize,
) -> Option<ProtocolFakePeer> {
    let mut peer = connect_fake_peer(server, room, user_id.clone(), TEST_ROOM_KEY).await?;
    peer.wait_until_connected(Duration::from_secs(5)).await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    Some(peer)
}

pub(super) async fn connect_load_triggered_local_subscriber(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber_user_id: &UserId,
) -> Option<ProtocolFakePeer> {
    let peer =
        connect_load_triggered_peer_on_worker(server, room, local_subscriber_user_id, 0).await?;
    assert_peer_joined_message_protocol(publisher, local_subscriber_user_id.clone()).await;
    Some(peer)
}

pub(super) async fn connect_load_triggered_activation_peer(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    activation_user_id: &UserId,
) -> Option<ProtocolFakePeer> {
    let mut peer =
        connect_fake_peer(server, room, activation_user_id.clone(), TEST_ROOM_KEY).await?;
    peer.wait_until_connected(Duration::from_secs(5)).await?;
    assert_peer_joined_message_protocol(publisher, activation_user_id.clone()).await;
    assert_peer_joined_message_protocol(local_subscriber, activation_user_id.clone()).await;
    Some(peer)
}

pub(super) async fn connect_load_triggered_spillover_subscriber(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    local_subscriber: &mut ProtocolFakePeer,
    spillover_subscriber_user_id: &UserId,
) -> Option<ProtocolFakePeer> {
    let peer = connect_load_triggered_peer_on_worker(server, room, spillover_subscriber_user_id, 1)
        .await?;
    assert_peer_joined_message_protocol(publisher, spillover_subscriber_user_id.clone()).await;
    assert_peer_joined_message_protocol(local_subscriber, spillover_subscriber_user_id.clone())
        .await;
    Some(peer)
}

pub(super) async fn assert_load_triggered_spillover_placement(
    server: &TestServer,
    room: &str,
    publisher_user_id: &UserId,
    local_subscriber_user_id: &UserId,
    spillover_subscriber_user_id: &UserId,
) {
    assert_user_media_worker(server, room, publisher_user_id, 0).await;
    assert_user_media_worker(server, room, local_subscriber_user_id, 0).await;
    assert_user_media_worker(server, room, spillover_subscriber_user_id, 1).await;
}

pub(super) async fn assert_user_media_worker(
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

pub(super) async fn connect_load_triggered_spillover_replacement(
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
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    assert_user_media_worker(server, room, user_id, worker_id).await;
    Some(replacement)
}

pub(super) fn load_triggered_activation_user_id(spillover_subscriber_user_id: &UserId) -> UserId {
    match spillover_subscriber_user_id {
        UserId::Integer(value) => UserId::Integer(value.saturating_add(10_000)),
        UserId::String(value) => UserId::String(format!("{value}-activation")),
    }
}

pub(super) async fn assert_audio_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    assert_packet_forwarded(publisher, subscriber, source, clock).await
}

pub(super) async fn assert_synthetic_video_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    for _ in 0..3 {
        let expected_payload = publisher.send_rtp_packet(source, clock).await;
        assert!(expected_payload.is_some());
        let Some(expected_payload) = expected_payload else {
            return 0;
        };
        if read_expected_rtp_payload(subscriber, &expected_payload, Duration::from_secs(5)).await {
            return u64::try_from(expected_payload.len()).unwrap_or(u64::MAX);
        }
    }
    panic!("synthetic video packet should be forwarded after route warmup");
}

pub(super) async fn assert_synthetic_video_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher.send_rtp_packet(source, clock).await;
    assert!(expected_payload.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

pub(super) async fn assert_packet_forwarded(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> u64 {
    let expected_payload = publisher.send_rtp_packet(source, clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return 0;
    };

    assert!(read_expected_rtp_payload(subscriber, &expected_payload, Duration::from_secs(5)).await);
    u64::try_from(expected_payload.len()).unwrap_or(u64::MAX)
}

pub(super) async fn read_expected_rtp_payload(
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

pub(super) async fn assert_video_subscription_enabled(
    subscriber: &mut ProtocolFakePeer,
    publisher_user_id: UserId,
) {
    assert!(
        subscriber
            .update_subscription(
                publisher_user_id,
                DownloadStates {
                    camera: Some(true),
                    camera_layout: Some(VideoLayoutIntent::Featured),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
}

pub(super) async fn assert_audio_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher.send_rtp_packet(source, clock).await;
    assert!(expected_payload.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

pub(super) async fn connect_two_isolated_audio_flows(
    server: &TestServer,
) -> Option<(
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
)> {
    let room_a = create_room(server, "issuer-topology-a", TEST_ROOM_KEY).await?;
    let room_b = create_room(server, "issuer-topology-b", TEST_ROOM_KEY).await?;

    let publisher_a =
        connect_fake_peer(server, &room_a, UserId::Integer(90), TEST_ROOM_KEY).await?;
    let subscriber_a =
        connect_fake_peer(server, &room_a, UserId::Integer(91), TEST_ROOM_KEY).await?;
    let publisher_b =
        connect_fake_peer(server, &room_b, UserId::Integer(90), TEST_ROOM_KEY).await?;
    let subscriber_b =
        connect_fake_peer(server, &room_b, UserId::Integer(91), TEST_ROOM_KEY).await?;

    let mut publisher_a = publisher_a;
    let mut subscriber_a = subscriber_a;
    let mut publisher_b = publisher_b;
    let mut subscriber_b = subscriber_b;

    publisher_a
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    subscriber_a
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    publisher_b
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    subscriber_b
        .wait_until_connected(Duration::from_secs(5))
        .await?;

    Some((publisher_a, subscriber_a, publisher_b, subscriber_b))
}

pub(super) async fn assert_audio_media_arrives_and_download_mute_stops_flow(
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
    let _forwarded_bytes =
        assert_audio_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;

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
    assert_consumer_route_inactive(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
    )
    .await;
    assert_audio_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
}

pub(super) async fn assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
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
    let _forwarded_bytes =
        assert_audio_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;

    assert!(
        publisher
            .set_publication_active(StreamType::Audio, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_empty_track_snapshot(subscriber).await;
    assert_consumer_route_absent(
        server,
        room,
        subscriber,
        &UserId::Integer(70),
        track_binding.stream_type,
    )
    .await;
    assert_audio_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
}

pub(super) async fn connect_camera_flow_peers(
    server: &TestServer,
    room: &str,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    connect_two_fake_peers(server, room, UserId::Integer(10), UserId::Integer(20)).await
}

pub(super) async fn publish_camera_track(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) -> Option<()> {
    let source = FakeMediaSource::camera();
    publisher.publish_track(&source).await?;
    publisher.complete_next_negotiation().await?;
    assert_track_snapshot(subscriber, UserId::Integer(10), StreamType::Camera, true).await;
    assert_peer_info_update(
        subscriber,
        UserId::Integer(10),
        UserInfo {
            is_camera_on: Some(true),
            ..UserInfo::snapshot_defaults()
        },
    )
    .await;
    Some(())
}

pub(super) async fn assert_consumer_download_toggle_round_trip_protocol(
    subscriber: &mut ProtocolFakePeer,
) {
    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(10),
                DownloadStates {
                    camera: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    assert!(
        subscriber
            .update_subscription(
                UserId::Integer(10),
                DownloadStates {
                    camera: Some(true),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
}

pub(super) async fn assert_camera_unpublish_updates_snapshot_and_info(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    assert!(
        publisher
            .set_publication_active(StreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    let first_message = subscriber.read_next_server_message().await;
    let second_message = subscriber.read_next_server_message().await;
    assert!(first_message.is_some());
    assert!(second_message.is_some());
    let (Some(first_message), Some(second_message)) = (first_message, second_message) else {
        return;
    };
    let messages = [first_message, second_message];
    let track_snapshot = messages.iter().find_map(|message| match message {
        ServerMessage::Tracks(snapshot) => Some(snapshot),
        _ => None,
    });
    let Some(track_snapshot) = track_snapshot else {
        panic!("expected track snapshot after camera unpublish");
    };
    assert!(
        track_snapshot.is_empty(),
        "protocol unpublish should clear the authoritative camera track snapshot"
    );

    let peer_info = messages.iter().find_map(|message| match message {
        ServerMessage::PeerInfo(info) => Some(info),
        _ => None,
    });
    let Some(peer_info) = peer_info else {
        panic!("expected peer info update after camera unpublish");
    };
    assert_eq!(peer_info.user_id, UserId::Integer(10));
    assert_eq!(
        peer_info.info,
        UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::snapshot_defaults()
        }
    );
}

pub(super) async fn connect_late_subscriber(
    server: &TestServer,
    room: &str,
) -> Option<ProtocolFakePeer> {
    connect_fake_peer(server, room, UserId::Integer(30), TEST_ROOM_KEY).await
}

pub(super) async fn assert_late_join_has_no_track_snapshot(late_subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            late_subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

pub(super) async fn assert_departure_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
) {
    let departure = subscriber.read_next_server_message().await;
    assert!(departure.is_some());
    let Some(ServerMessage::PeerLeft(departure)) = departure else {
        panic!("expected protocol peer departure notification");
    };
    assert_eq!(departure.user_id, user_id);
}

pub(super) async fn assert_peer_joined_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
) {
    let joined = subscriber.read_next_server_message().await;
    assert!(joined.is_some());
    let Some(ServerMessage::PeerJoined(joined)) = joined else {
        panic!("expected protocol peer joined notification");
    };
    assert_eq!(joined.user_id, user_id);
}

pub(super) async fn assert_track_snapshot(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    stream_type: StreamType,
    active: bool,
) -> TrackBinding {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected protocol track snapshot");
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(track_binding) = track_bindings.first() else {
        panic!("expected one protocol track binding");
    };
    assert_eq!(track_binding.user_id, user_id);
    assert_eq!(track_binding.stream_type, stream_type);
    assert_eq!(track_binding.active, active);
    track_binding.clone()
}

pub(super) async fn assert_empty_track_snapshot(subscriber: &mut ProtocolFakePeer) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected protocol track snapshot");
    };
    assert!(track_bindings.is_empty());
}

pub(super) async fn assert_peer_info_update(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    expected_info: UserInfo,
) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::PeerInfo(peer_info)) = message else {
        panic!("expected protocol peer info update");
    };
    assert_eq!(peer_info.user_id, user_id);
    assert_eq!(peer_info.info, expected_info);
}

pub(super) async fn assert_no_server_message_protocol(subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

pub(super) async fn assert_consumer_route_active(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
) {
    assert!(
        server
            .wait_for_consumer_route_active(
                room,
                subscriber.user_id(),
                publisher_user_id,
                stream_type,
            )
            .await
    );
}

pub(super) async fn assert_consumer_route_inactive(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
) {
    assert!(
        server
            .wait_for_consumer_route_inactive(
                room,
                subscriber.user_id(),
                publisher_user_id,
                stream_type,
            )
            .await
    );
}

pub(super) async fn assert_consumer_route_absent(
    server: &TestServer,
    room: &str,
    subscriber: &ProtocolFakePeer,
    publisher_user_id: &UserId,
    stream_type: StreamType,
) {
    assert!(
        server
            .wait_for_consumer_route_absence(
                room,
                subscriber.user_id(),
                publisher_user_id,
                stream_type,
            )
            .await
    );
}

pub(super) async fn stream_until_audio_bitrate_is_observable(
    server: &TestServer,
    room: &str,
    publisher: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> Option<IncomingBitRateStatsResponse> {
    for _ in 0..20 {
        publisher.send_rtp_packets(source, clock, 2).await?;
        let stats = stats(server).await?;
        let room_stats = stats.into_iter().find(|entry| entry.uuid == room)?;
        if room_stats.users_stats.incoming_bit_rate.audio > 0 {
            return Some(room_stats.users_stats.incoming_bit_rate);
        }
        yield_now().await;
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct TransportUserLifetimeMetrics {
    pub(super) le_1_second: u64,
    pub(super) le_10_seconds: u64,
    pub(super) le_60_seconds: u64,
    pub(super) le_300_seconds: u64,
    pub(super) count: u64,
    pub(super) sum_seconds: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LiveRtcMetrics {
    pub(super) connected_transport_users: i64,
    pub(super) disconnected_transport_users: i64,
    pub(super) transport_health_transitions_unset_to_connected: u64,
    pub(super) transport_health_transitions_connected_to_disconnected: u64,
    pub(super) transport_health_transitions_connected_to_unset: u64,
    pub(super) transport_ice_state_changes_new: u64,
    pub(super) transport_ice_state_changes_checking: u64,
    pub(super) transport_ice_state_changes_connected: u64,
    pub(super) transport_ice_state_changes_completed: u64,
    pub(super) transport_ice_state_changes_disconnected: u64,
    pub(super) transport_dtls_connected: u64,
    pub(super) rtp_packets_ingress: u64,
    pub(super) rtp_packets_egress: u64,
    pub(super) rtp_payload_bytes_ingress: u64,
    pub(super) rtp_payload_bytes_egress: u64,
    pub(super) rtp_forwarded_packets_local_rtc: u64,
    pub(super) rtp_forwarded_payload_bytes_local_rtc: u64,
    pub(super) indexed_routes: u64,
    pub(super) scan_routes: u64,
    pub(super) fallback_scans: u64,
    pub(super) scan_users: u64,
}

pub(super) async fn wait_for_transport_lifetime_metrics(
    server: &TestServer,
    expected_count: u64,
) -> Option<TransportUserLifetimeMetrics> {
    timeout(Duration::from_secs(3), async {
        loop {
            let metrics = parse_transport_lifetime_metrics(&metrics_text(server).await?)?;
            if metrics.count >= expected_count {
                return Some(metrics);
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

pub(super) async fn wait_for_live_rtc_metrics(
    server: &TestServer,
    expected_connected_users: i64,
) -> Option<LiveRtcMetrics> {
    timeout(Duration::from_secs(3), async {
        loop {
            let metrics = parse_live_rtc_metrics(&metrics_text(server).await?)?;
            if metrics.connected_transport_users == expected_connected_users {
                return Some(metrics);
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

pub(super) fn parse_transport_lifetime_metrics(
    metrics_text: &str,
) -> Option<TransportUserLifetimeMetrics> {
    Some(TransportUserLifetimeMetrics {
        le_1_second: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"1\"}",
        )?,
        le_10_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"10\"}",
        )?,
        le_60_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"60\"}",
        )?,
        le_300_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_bucket{le=\"300\"}",
        )?,
        count: parse_prometheus_u64(metrics_text, "osfu_transport_user_lifetime_seconds_count")?,
        sum_seconds: parse_prometheus_f64(
            metrics_text,
            "osfu_transport_user_lifetime_seconds_sum",
        )?,
    })
}

pub(super) fn parse_live_rtc_metrics(metrics_text: &str) -> Option<LiveRtcMetrics> {
    Some(LiveRtcMetrics {
        connected_transport_users: parse_prometheus_i64(
            metrics_text,
            "osfu_transport_health_users{state=\"connected\"}",
        )?,
        disconnected_transport_users: parse_prometheus_i64(
            metrics_text,
            "osfu_transport_health_users{state=\"disconnected\"}",
        )?,
        transport_health_transitions_unset_to_connected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_health_transitions_total{from=\"unset\",to=\"connected\"}",
        )?,
        transport_health_transitions_connected_to_disconnected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_health_transitions_total{from=\"connected\",to=\"disconnected\"}",
        )?,
        transport_health_transitions_connected_to_unset: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_health_transitions_total{from=\"connected\",to=\"unset\"}",
        )?,
        transport_ice_state_changes_new: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"new\"}",
        )?,
        transport_ice_state_changes_checking: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"checking\"}",
        )?,
        transport_ice_state_changes_connected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"connected\"}",
        )?,
        transport_ice_state_changes_completed: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"completed\"}",
        )?,
        transport_ice_state_changes_disconnected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_ice_state_changes_total{state=\"disconnected\"}",
        )?,
        transport_dtls_connected: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_dtls_connected_total",
        )?,
        rtp_packets_ingress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_packets_total{direction=\"ingress\"}",
        )?,
        rtp_packets_egress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_packets_total{direction=\"egress\"}",
        )?,
        rtp_payload_bytes_ingress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_payload_bytes_total{direction=\"ingress\"}",
        )?,
        rtp_payload_bytes_egress: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_payload_bytes_total{direction=\"egress\"}",
        )?,
        rtp_forwarded_packets_local_rtc: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"}",
        )?,
        rtp_forwarded_payload_bytes_local_rtc: parse_prometheus_u64(
            metrics_text,
            "osfu_rtp_forwarded_payload_bytes_total{destination=\"local_rtc\"}",
        )?,
        indexed_routes: parse_prometheus_u64(
            metrics_text,
            "osfu_rtc_datagram_routes_total{path=\"indexed\"}",
        )?,
        scan_routes: parse_prometheus_u64(
            metrics_text,
            "osfu_rtc_datagram_routes_total{path=\"scan\"}",
        )?,
        fallback_scans: parse_prometheus_u64(
            metrics_text,
            "osfu_rtc_datagram_fallback_scans_total",
        )?,
        scan_users: parse_prometheus_u64(metrics_text, "osfu_rtc_datagram_scan_users_total")?,
    })
}

pub(super) fn assert_initial_live_rtc_metrics(
    metrics: &LiveRtcMetrics,
    initial_forwarded_bytes: u64,
) {
    assert_eq!(metrics.connected_transport_users, 2);
    assert_eq!(metrics.disconnected_transport_users, 0);
    assert!(
        metrics.transport_health_transitions_unset_to_connected >= 2,
        "expected both RTC users to enter a connected transport health state"
    );
    assert_eq!(
        metrics.transport_health_transitions_connected_to_disconnected,
        0
    );
    assert_eq!(metrics.transport_health_transitions_connected_to_unset, 0);
    assert!(
        metrics.transport_ice_state_changes_new + metrics.transport_ice_state_changes_checking >= 2,
        "expected both RTC users to emit early ICE lifecycle counters"
    );
    assert!(
        metrics.transport_ice_state_changes_connected
            + metrics.transport_ice_state_changes_completed
            >= 2,
        "expected both RTC users to reach a connected ICE lifecycle state"
    );
    assert_eq!(metrics.transport_ice_state_changes_disconnected, 0);
    assert_eq!(metrics.transport_dtls_connected, 2);
    assert_eq!(metrics.rtp_packets_ingress, 2);
    assert_eq!(metrics.rtp_packets_egress, 2);
    assert_eq!(metrics.rtp_payload_bytes_ingress, initial_forwarded_bytes);
    assert_eq!(metrics.rtp_payload_bytes_egress, initial_forwarded_bytes);
    assert_eq!(metrics.rtp_forwarded_packets_local_rtc, 2);
    assert_eq!(
        metrics.rtp_forwarded_payload_bytes_local_rtc,
        initial_forwarded_bytes
    );
}

pub(super) fn assert_steady_state_live_rtc_metrics(
    before: &LiveRtcMetrics,
    during: &LiveRtcMetrics,
    additional_forwarded_bytes: u64,
) {
    assert_eq!(during.connected_transport_users, 2);
    assert_eq!(during.disconnected_transport_users, 0);
    assert_eq!(
        during.transport_health_transitions_unset_to_connected,
        before.transport_health_transitions_unset_to_connected
    );
    assert_eq!(
        during.transport_health_transitions_connected_to_disconnected,
        before.transport_health_transitions_connected_to_disconnected
    );
    assert_eq!(
        during.transport_health_transitions_connected_to_unset,
        before.transport_health_transitions_connected_to_unset
    );
    assert_eq!(
        during.transport_ice_state_changes_new,
        before.transport_ice_state_changes_new
    );
    assert_eq!(
        during.transport_ice_state_changes_checking,
        before.transport_ice_state_changes_checking
    );
    assert_eq!(
        during.transport_ice_state_changes_connected,
        before.transport_ice_state_changes_connected
    );
    assert_eq!(
        during.transport_ice_state_changes_completed,
        before.transport_ice_state_changes_completed
    );
    assert_eq!(
        during.transport_ice_state_changes_disconnected,
        before.transport_ice_state_changes_disconnected
    );
    assert_eq!(
        during.transport_dtls_connected,
        before.transport_dtls_connected
    );
    assert!(
        during.indexed_routes > before.indexed_routes,
        "expected steady-state media to increase indexed datagram routing"
    );
    assert_eq!(during.scan_routes, before.scan_routes);
    assert_eq!(during.fallback_scans, before.fallback_scans);
    assert_eq!(during.scan_users, before.scan_users);
    assert_eq!(during.rtp_packets_ingress - before.rtp_packets_ingress, 4);
    assert_eq!(during.rtp_packets_egress - before.rtp_packets_egress, 4);
    assert_eq!(
        during.rtp_payload_bytes_ingress - before.rtp_payload_bytes_ingress,
        additional_forwarded_bytes
    );
    assert_eq!(
        during.rtp_payload_bytes_egress - before.rtp_payload_bytes_egress,
        additional_forwarded_bytes
    );
    assert_eq!(
        during.rtp_forwarded_packets_local_rtc - before.rtp_forwarded_packets_local_rtc,
        4
    );
    assert_eq!(
        during.rtp_forwarded_payload_bytes_local_rtc - before.rtp_forwarded_payload_bytes_local_rtc,
        additional_forwarded_bytes
    );
}

pub(super) fn parse_prometheus_i64(metrics_text: &str, metric_name: &str) -> Option<i64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}

pub(super) fn parse_prometheus_u64(metrics_text: &str, metric_name: &str) -> Option<u64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}

pub(super) fn parse_prometheus_f64(metrics_text: &str, metric_name: &str) -> Option<f64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}
