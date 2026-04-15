#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use std::time::Duration;

use o_sfu::{
    config::TransportBackend,
    signaling::{
        http::IncomingBitRateStats,
        protocol::ServerMessage,
        shared::{DownloadStates, SessionId, StreamType},
    },
};

use crate::support::{
    TEST_CHANNEL_KEY,
    fake_media::{FakeClock, FakeMediaSource},
    native_full_stack::{NativeFakePeer, NativeLocalNetwork},
    test_config,
};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

#[test]
fn fake_media_source_uses_manual_clock_deterministically() {
    let mut clock = FakeClock::default();
    let mut source = FakeMediaSource::audio();

    let first = source.next_frame(&mut clock);
    let second = source.next_frame(&mut clock);

    assert_eq!(first.emitted_at, Duration::from_millis(20));
    assert_eq!(second.emitted_at, Duration::from_millis(40));
    assert_eq!(first.sequence_number, 0);
    assert_eq!(second.sequence_number, 1);
    assert_eq!(first.rtp_timestamp, 0);
    assert_eq!(second.rtp_timestamp, 960);
    assert_eq!(first.payload.len(), 160);
    assert_eq!(second.payload.len(), 160);
}

#[tokio::test]
async fn fake_peers_publish_and_receive_track_snapshot_over_real_server_entries() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-a", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let publisher = network
        .connect_fake_peer(&channel, SessionId::Integer(1), TEST_CHANNEL_KEY)
        .await;
    let subscriber = network
        .connect_fake_peer(&channel, SessionId::Integer(2), TEST_CHANNEL_KEY)
        .await;
    assert!(publisher.is_some());
    assert!(subscriber.is_some());
    let Some(mut publisher) = publisher else {
        return;
    };
    let Some(mut subscriber) = subscriber else {
        return;
    };

    assert!(publisher.welcome().features.rtc);
    assert!(subscriber.welcome().features.rtc);

    let source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        SessionId::Integer(1),
        StreamType::Audio,
        true,
    )
    .await;
}

#[tokio::test]
async fn fake_peers_keep_channel_topology_isolation_with_same_session_ids() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let peers = Box::pin(connect_two_isolated_audio_flows(&network)).await;
    assert!(peers.is_some());
    let Some((mut publisher_a, mut subscriber_a, mut publisher_b, mut subscriber_b)) = peers else {
        return;
    };

    let source = FakeMediaSource::audio();
    assert!(publisher_a.publish_track(&source).await.is_some());
    assert!(publisher_a.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber_a,
        SessionId::Integer(90),
        StreamType::Audio,
        true,
    )
    .await;
    assert_no_server_message_native(&mut subscriber_b).await;

    assert!(publisher_b.publish_track(&source).await.is_some());
    assert!(publisher_b.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber_b,
        SessionId::Integer(90),
        StreamType::Audio,
        true,
    )
    .await;

    assert!(publisher_a.close().await.is_some());
    assert_departure_message_native(&mut subscriber_a, SessionId::Integer(90)).await;
    assert_no_server_message_native(&mut subscriber_b).await;
}

#[tokio::test]
async fn fake_peers_cover_publish_unpublish_late_join_and_disconnect_deterministically() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-b", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let peers = connect_camera_flow_peers(&network, &channel).await;
    assert!(peers.is_some());
    let Some((mut publisher, mut subscriber)) = peers else {
        return;
    };

    assert!(
        publish_camera_track(&mut publisher, &mut subscriber)
            .await
            .is_some()
    );

    assert_consumer_download_toggle_round_trip_native(&mut subscriber).await;
    assert_camera_unpublish_updates_snapshot_and_info(&mut publisher, &mut subscriber).await;

    let late_subscriber = connect_late_subscriber(&network, &channel).await;
    assert!(late_subscriber.is_some());
    let Some(mut late_subscriber) = late_subscriber else {
        return;
    };
    assert_peer_joined_message_native(&mut subscriber, SessionId::Integer(30)).await;
    assert_late_join_has_no_track_snapshot(&mut late_subscriber).await;

    assert!(publisher.close().await.is_some());
    assert_departure_message_native(&mut subscriber, SessionId::Integer(10)).await;
    assert_departure_message_native(&mut late_subscriber, SessionId::Integer(10)).await;
}

#[tokio::test]
async fn fake_peers_cover_session_replacement_and_republish_over_native_protocol() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-c", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let initial_publisher = network
        .connect_fake_peer(&channel, SessionId::Integer(40), TEST_CHANNEL_KEY)
        .await;
    let subscriber = network
        .connect_fake_peer(&channel, SessionId::Integer(50), TEST_CHANNEL_KEY)
        .await;
    assert!(initial_publisher.is_some());
    assert!(subscriber.is_some());
    let Some(mut initial_publisher) = initial_publisher else {
        return;
    };
    let Some(mut subscriber) = subscriber else {
        return;
    };

    let replacement = network
        .connect_fake_peer(&channel, SessionId::Integer(40), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4003))
    );
    assert_departure_message_native(&mut subscriber, SessionId::Integer(40)).await;
    assert_peer_joined_message_native(&mut subscriber, SessionId::Integer(40)).await;

    let source = FakeMediaSource::audio();
    assert!(replacement.publish_track(&source).await.is_some());
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        SessionId::Integer(40),
        StreamType::Audio,
        true,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_peer_media_updates_channel_stats_deterministically() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-d", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let publisher = network
        .connect_fake_peer(&channel, SessionId::Integer(60), TEST_CHANNEL_KEY)
        .await;
    let subscriber = network
        .connect_fake_peer(&channel, SessionId::Integer(61), TEST_CHANNEL_KEY)
        .await;
    assert!(publisher.is_some());
    assert!(subscriber.is_some());
    let Some(mut publisher) = publisher else {
        return;
    };
    let Some(mut subscriber) = subscriber else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    assert!(
        publisher
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert!(
        subscriber
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        SessionId::Integer(60),
        StreamType::Audio,
        true,
    )
    .await;

    let mut clock = FakeClock::default();
    let stats = stream_until_audio_bitrate_is_observable(
        &network,
        &channel,
        &mut publisher,
        &mut source,
        &mut clock,
    )
    .await;
    assert!(stats.is_some());
    let Some(stats) = stats else {
        return;
    };
    assert!(stats.audio > 0);
    assert!(stats.total >= stats.audio);
}

#[tokio::test]
async fn fake_rtc_peers_export_longer_transport_lifetimes_after_steady_state_run() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-lifetime-metrics", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let peers = Box::pin(connect_audio_media_flow_peers_for_sessions(
        &network,
        &channel,
        SessionId::Integer(62),
        SessionId::Integer(63),
    ))
    .await;
    assert!(peers.is_some());
    let Some((publisher, subscriber)) = peers else {
        return;
    };

    sleep(Duration::from_millis(1_200)).await;

    assert!(publisher.close().await.is_some());
    assert!(subscriber.close().await.is_some());

    let lifetime_metrics = wait_for_transport_lifetime_metrics(&network, 2).await;
    assert!(lifetime_metrics.is_some());
    let Some(lifetime_metrics) = lifetime_metrics else {
        return;
    };

    assert_eq!(lifetime_metrics.le_1_second, 0);
    assert_eq!(lifetime_metrics.le_10_seconds, 2);
    assert_eq!(lifetime_metrics.le_60_seconds, 2);
    assert_eq!(lifetime_metrics.le_300_seconds, 2);
    assert_eq!(lifetime_metrics.count, 2);
    assert!(lifetime_metrics.sum_seconds >= 2.0);
}

#[tokio::test]
async fn fake_rtc_peers_keep_transport_health_and_indexed_datagram_metrics_stable_during_live_media()
 {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-live-metrics", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let setup = Box::pin(connect_audio_media_flow_peers_for_sessions(
        &network,
        &channel,
        SessionId::Integer(64),
        SessionId::Integer(65),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        SessionId::Integer(64),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    assert_audio_frame_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await;
    assert_audio_frame_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await;

    let before_live_metrics = wait_for_live_rtc_metrics(&network, 2).await;
    assert!(before_live_metrics.is_some());
    let Some(before_live_metrics) = before_live_metrics else {
        return;
    };
    assert_eq!(before_live_metrics.connected_transport_sessions, 2);
    assert_eq!(before_live_metrics.disconnected_transport_sessions, 0);

    for _ in 0..4 {
        assert_audio_frame_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
            .await;
    }

    let during_live_metrics = wait_for_live_rtc_metrics(&network, 2).await;
    assert!(during_live_metrics.is_some());
    let Some(during_live_metrics) = during_live_metrics else {
        return;
    };

    assert_eq!(during_live_metrics.connected_transport_sessions, 2);
    assert_eq!(during_live_metrics.disconnected_transport_sessions, 0);
    assert!(
        during_live_metrics.indexed_routes > before_live_metrics.indexed_routes,
        "expected steady-state media to increase indexed datagram routing"
    );
    assert_eq!(
        during_live_metrics.scan_routes,
        before_live_metrics.scan_routes
    );
    assert_eq!(
        during_live_metrics.fallback_scans,
        before_live_metrics.fallback_scans
    );
    assert_eq!(
        during_live_metrics.scan_sessions,
        before_live_metrics.scan_sessions
    );

    assert!(publisher.close().await.is_some());
    assert!(subscriber.close().await.is_some());

    let after_live_metrics = wait_for_live_rtc_metrics(&network, 0).await;
    assert!(after_live_metrics.is_some());
    let Some(after_live_metrics) = after_live_metrics else {
        return;
    };

    assert_eq!(after_live_metrics.connected_transport_sessions, 0);
    assert_eq!(after_live_metrics.disconnected_transport_sessions, 0);
}

#[tokio::test]
async fn fake_rtc_peers_rebootstrap_session_replacement_without_stale_media_routes() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-replacement-rtc", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let setup = Box::pin(connect_audio_media_flow_peers_for_sessions(
        &network,
        &channel,
        SessionId::Integer(80),
        SessionId::Integer(81),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut initial_publisher, mut subscriber)) = setup else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    assert!(initial_publisher.publish_track(&source).await.is_some());
    assert!(
        initial_publisher
            .complete_next_negotiation()
            .await
            .is_some()
    );
    assert_track_snapshot(
        &mut subscriber,
        SessionId::Integer(80),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    assert_audio_frame_forwarded(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    let replacement = network
        .connect_fake_peer(&channel, SessionId::Integer(80), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4003))
    );
    assert_departure_message_native(&mut subscriber, SessionId::Integer(80)).await;
    assert_peer_joined_message_native(&mut subscriber, SessionId::Integer(80)).await;

    assert_audio_frame_dropped(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;

    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert!(replacement.publish_track(&source).await.is_some());
    assert!(replacement.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        &mut subscriber,
        SessionId::Integer(80),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_audio_frame_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
}

#[tokio::test]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-e", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let setup = Box::pin(connect_audio_media_flow_peers(&network, &channel)).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_download_mute_stops_flow(&mut publisher, &mut subscriber).await;
}

#[tokio::test]
async fn fake_rtc_peers_cover_explicit_upload_unpublish_compatibility_semantics() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = NativeLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-f", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let setup = Box::pin(connect_audio_media_flow_peers(&network, &channel)).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_explicit_unpublish_stops_flow(&mut publisher, &mut subscriber)
        .await;
}

async fn connect_audio_media_flow_peers(
    network: &NativeLocalNetwork,
    channel: &str,
) -> Option<(NativeFakePeer, NativeFakePeer)> {
    Box::pin(connect_audio_media_flow_peers_for_sessions(
        network,
        channel,
        SessionId::Integer(70),
        SessionId::Integer(71),
    ))
    .await
}

async fn connect_audio_media_flow_peers_for_sessions(
    network: &NativeLocalNetwork,
    channel: &str,
    publisher_session_id: SessionId,
    subscriber_session_id: SessionId,
) -> Option<(NativeFakePeer, NativeFakePeer)> {
    let publisher = network
        .connect_fake_peer(channel, publisher_session_id, TEST_CHANNEL_KEY)
        .await?;
    let subscriber = network
        .connect_fake_peer(channel, subscriber_session_id, TEST_CHANNEL_KEY)
        .await?;
    let mut publisher = publisher;
    let mut subscriber = subscriber;

    publisher
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    subscriber
        .wait_until_connected(Duration::from_secs(5))
        .await?;

    Some((publisher, subscriber))
}

async fn assert_audio_frame_forwarded(
    publisher: &mut NativeFakePeer,
    subscriber: &mut NativeFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher.send_frame(source, clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return;
    };

    let received_frame = subscriber.read_media_frame(Duration::from_secs(2)).await;
    assert!(received_frame.is_some());
    let Some(received_frame) = received_frame else {
        return;
    };
    assert_eq!(received_frame.payload, expected_payload);
}

async fn assert_audio_frame_dropped(
    publisher: &mut NativeFakePeer,
    subscriber: &mut NativeFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let _ = publisher.send_frame(source, clock).await;
    assert!(
        subscriber
            .read_media_frame(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn connect_two_isolated_audio_flows(
    network: &NativeLocalNetwork,
) -> Option<(
    NativeFakePeer,
    NativeFakePeer,
    NativeFakePeer,
    NativeFakePeer,
)> {
    let channel_a = network
        .create_channel("issuer-topology-a", Some(TEST_CHANNEL_KEY))
        .await?;
    let channel_b = network
        .create_channel("issuer-topology-b", Some(TEST_CHANNEL_KEY))
        .await?;

    let publisher_a = network
        .connect_fake_peer(&channel_a, SessionId::Integer(90), TEST_CHANNEL_KEY)
        .await?;
    let subscriber_a = network
        .connect_fake_peer(&channel_a, SessionId::Integer(91), TEST_CHANNEL_KEY)
        .await?;
    let publisher_b = network
        .connect_fake_peer(&channel_b, SessionId::Integer(90), TEST_CHANNEL_KEY)
        .await?;
    let subscriber_b = network
        .connect_fake_peer(&channel_b, SessionId::Integer(91), TEST_CHANNEL_KEY)
        .await?;

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

async fn assert_audio_media_arrives_and_download_mute_stops_flow(
    publisher: &mut NativeFakePeer,
    subscriber: &mut NativeFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(subscriber, SessionId::Integer(70), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    let expected_payload = publisher.send_frame(&mut source, &mut clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return;
    };

    let received_frame = subscriber.read_media_frame(Duration::from_secs(2)).await;
    assert!(received_frame.is_some());
    let Some(received_frame) = received_frame else {
        return;
    };
    assert!(!received_frame.mid.is_empty());
    assert_eq!(received_frame.payload, expected_payload);

    assert!(
        subscriber
            .update_subscription(
                SessionId::Integer(70),
                DownloadStates {
                    audio: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );

    drain_native_control_plane(subscriber, Duration::from_millis(150)).await;

    let next_payload = publisher.send_frame(&mut source, &mut clock).await;
    assert!(next_payload.is_some());
    assert!(
        subscriber
            .read_media_frame(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
    publisher: &mut NativeFakePeer,
    subscriber: &mut NativeFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(subscriber, SessionId::Integer(70), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    let first_payload = publisher.send_frame(&mut source, &mut clock).await;
    assert!(first_payload.is_some());
    assert!(
        subscriber
            .read_media_frame(Duration::from_secs(2))
            .await
            .is_some()
    );

    assert!(
        publisher
            .set_publication_active(StreamType::Audio, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_empty_track_snapshot(subscriber).await;

    drain_native_control_plane(subscriber, Duration::from_millis(150)).await;

    let _ = publisher.send_frame(&mut source, &mut clock).await;
    assert!(
        subscriber
            .read_media_frame(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn connect_camera_flow_peers(
    network: &NativeLocalNetwork,
    channel: &str,
) -> Option<(NativeFakePeer, NativeFakePeer)> {
    let publisher = network
        .connect_fake_peer(channel, SessionId::Integer(10), TEST_CHANNEL_KEY)
        .await?;
    let subscriber = network
        .connect_fake_peer(channel, SessionId::Integer(20), TEST_CHANNEL_KEY)
        .await?;
    Some((publisher, subscriber))
}

async fn publish_camera_track(
    publisher: &mut NativeFakePeer,
    subscriber: &mut NativeFakePeer,
) -> Option<()> {
    let source = FakeMediaSource::camera();
    publisher.publish_track(&source).await?;
    publisher.complete_next_negotiation().await?;
    assert_track_snapshot(subscriber, SessionId::Integer(10), StreamType::Camera, true).await;
    Some(())
}

async fn assert_consumer_download_toggle_round_trip_native(subscriber: &mut NativeFakePeer) {
    assert!(
        subscriber
            .update_subscription(
                SessionId::Integer(10),
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
                SessionId::Integer(10),
                DownloadStates {
                    camera: Some(true),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
}

async fn assert_camera_unpublish_updates_snapshot_and_info(
    publisher: &mut NativeFakePeer,
    subscriber: &mut NativeFakePeer,
) {
    assert!(
        publisher
            .set_publication_active(StreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    let track_snapshot = subscriber.read_next_server_message().await;
    assert!(track_snapshot.is_some());
    let Some(ServerMessage::Tracks(track_snapshot)) = track_snapshot else {
        panic!("expected track snapshot after camera unpublish");
    };
    assert!(
        track_snapshot.is_empty(),
        "native unpublish should clear the authoritative camera track snapshot"
    );

    let peer_info = subscriber.read_next_server_message().await;
    assert!(peer_info.is_some());
    let Some(ServerMessage::PeerInfo(peer_info)) = peer_info else {
        panic!("expected peer info update after camera unpublish");
    };
    assert_eq!(peer_info.session_id, SessionId::Integer(10));
    assert_eq!(peer_info.info.is_camera_on, None);
}

async fn connect_late_subscriber(
    network: &NativeLocalNetwork,
    channel: &str,
) -> Option<NativeFakePeer> {
    network
        .connect_fake_peer(channel, SessionId::Integer(30), TEST_CHANNEL_KEY)
        .await
}

async fn assert_late_join_has_no_track_snapshot(late_subscriber: &mut NativeFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            late_subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

async fn assert_departure_message_native(subscriber: &mut NativeFakePeer, session_id: SessionId) {
    let departure = subscriber.read_next_server_message().await;
    assert!(departure.is_some());
    let Some(ServerMessage::PeerLeft(departure)) = departure else {
        panic!("expected native peer departure notification");
    };
    assert_eq!(departure.session_id, session_id);
}

async fn assert_peer_joined_message_native(subscriber: &mut NativeFakePeer, session_id: SessionId) {
    let joined = subscriber.read_next_server_message().await;
    assert!(joined.is_some());
    let Some(ServerMessage::PeerJoined(joined)) = joined else {
        panic!("expected native peer joined notification");
    };
    assert_eq!(joined.session_id, session_id);
}

async fn assert_track_snapshot(
    subscriber: &mut NativeFakePeer,
    session_id: SessionId,
    stream_type: StreamType,
    active: bool,
) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected native track snapshot");
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(track_binding) = track_bindings.first() else {
        panic!("expected one native track binding");
    };
    assert_eq!(track_binding.session_id, session_id);
    assert_eq!(track_binding.stream_type, stream_type);
    assert_eq!(track_binding.active, active);
}

async fn assert_empty_track_snapshot(subscriber: &mut NativeFakePeer) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected native track snapshot");
    };
    assert!(track_bindings.is_empty());
}

async fn drain_native_control_plane(subscriber: &mut NativeFakePeer, timeout_window: Duration) {
    while subscriber
        .read_server_message_with_timeout(timeout_window)
        .await
        .is_some()
    {}
}

async fn assert_no_server_message_native(subscriber: &mut NativeFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

async fn stream_until_audio_bitrate_is_observable(
    network: &NativeLocalNetwork,
    channel: &str,
    publisher: &mut NativeFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> Option<IncomingBitRateStats> {
    for _ in 0..20 {
        publisher.send_frames(source, clock, 2).await?;
        let stats = network.stats().await?;
        let channel_stats = stats.into_iter().find(|entry| entry.uuid == channel)?;
        if channel_stats.sessions_stats.incoming_bit_rate.audio > 0 {
            return Some(channel_stats.sessions_stats.incoming_bit_rate);
        }
        sleep(Duration::from_millis(25)).await;
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TransportSessionLifetimeMetrics {
    le_1_second: u64,
    le_10_seconds: u64,
    le_60_seconds: u64,
    le_300_seconds: u64,
    count: u64,
    sum_seconds: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveRtcMetrics {
    connected_transport_sessions: i64,
    disconnected_transport_sessions: i64,
    indexed_routes: u64,
    scan_routes: u64,
    fallback_scans: u64,
    scan_sessions: u64,
}

async fn wait_for_transport_lifetime_metrics(
    network: &NativeLocalNetwork,
    expected_count: u64,
) -> Option<TransportSessionLifetimeMetrics> {
    timeout(Duration::from_secs(3), async {
        loop {
            let metrics = parse_transport_lifetime_metrics(&network.metrics_text().await?)?;
            if metrics.count >= expected_count {
                return Some(metrics);
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .ok()
    .flatten()
}

async fn wait_for_live_rtc_metrics(
    network: &NativeLocalNetwork,
    expected_connected_sessions: i64,
) -> Option<LiveRtcMetrics> {
    timeout(Duration::from_secs(3), async {
        loop {
            let metrics = parse_live_rtc_metrics(&network.metrics_text().await?)?;
            if metrics.connected_transport_sessions == expected_connected_sessions {
                return Some(metrics);
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .ok()
    .flatten()
}

fn parse_transport_lifetime_metrics(metrics_text: &str) -> Option<TransportSessionLifetimeMetrics> {
    Some(TransportSessionLifetimeMetrics {
        le_1_second: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_session_lifetime_seconds_bucket{le=\"1\"}",
        )?,
        le_10_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_session_lifetime_seconds_bucket{le=\"10\"}",
        )?,
        le_60_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_session_lifetime_seconds_bucket{le=\"60\"}",
        )?,
        le_300_seconds: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_session_lifetime_seconds_bucket{le=\"300\"}",
        )?,
        count: parse_prometheus_u64(
            metrics_text,
            "osfu_transport_session_lifetime_seconds_count",
        )?,
        sum_seconds: parse_prometheus_f64(
            metrics_text,
            "osfu_transport_session_lifetime_seconds_sum",
        )?,
    })
}

fn parse_live_rtc_metrics(metrics_text: &str) -> Option<LiveRtcMetrics> {
    Some(LiveRtcMetrics {
        connected_transport_sessions: parse_prometheus_i64(
            metrics_text,
            "osfu_transport_health_sessions{state=\"connected\"}",
        )?,
        disconnected_transport_sessions: parse_prometheus_i64(
            metrics_text,
            "osfu_transport_health_sessions{state=\"disconnected\"}",
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
        scan_sessions: parse_prometheus_u64(metrics_text, "osfu_rtc_datagram_scan_sessions_total")?,
    })
}

fn parse_prometheus_i64(metrics_text: &str, metric_name: &str) -> Option<i64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}

fn parse_prometheus_u64(metrics_text: &str, metric_name: &str) -> Option<u64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}

fn parse_prometheus_f64(metrics_text: &str, metric_name: &str) -> Option<f64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(metric_name)?.trim().parse().ok())
}
