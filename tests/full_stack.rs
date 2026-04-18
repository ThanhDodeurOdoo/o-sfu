#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use std::time::Duration;

use o_sfu::public::http::IncomingBitRateStats;
use o_sfu_protocol::{
    shared::{DownloadStates, SessionId, SessionInfo, StreamType},
    signaling::{ServerMessage, ServerRequest},
};

use crate::support::{
    TEST_CHANNEL_KEY,
    fake_media::{FakeClock, FakeMediaSource},
    protocol_full_stack::{ProtocolFakePeer, ProtocolLocalNetwork},
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
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    assert_no_server_message_protocol(&mut subscriber_b).await;

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
    assert_departure_message_protocol(&mut subscriber_a, SessionId::Integer(90)).await;
    assert_no_server_message_protocol(&mut subscriber_b).await;
}

#[tokio::test]
async fn fake_peers_cover_publish_unpublish_late_join_and_disconnect_deterministically() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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

    assert_consumer_download_toggle_round_trip_protocol(&mut subscriber).await;
    assert_camera_unpublish_updates_snapshot_and_info(&mut publisher, &mut subscriber).await;

    let late_subscriber = connect_late_subscriber(&network, &channel).await;
    assert!(late_subscriber.is_some());
    let Some(mut late_subscriber) = late_subscriber else {
        return;
    };
    assert_peer_joined_message_protocol(&mut subscriber, SessionId::Integer(30)).await;
    assert_late_join_has_no_track_snapshot(&mut late_subscriber).await;

    assert!(publisher.close().await.is_some());
    assert_departure_message_protocol(&mut subscriber, SessionId::Integer(10)).await;
    assert_departure_message_protocol(&mut late_subscriber, SessionId::Integer(10)).await;
}

#[tokio::test]
async fn fake_peers_cover_session_replacement_and_republish_over_protocol_session_flow() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    assert_departure_message_protocol(&mut subscriber, SessionId::Integer(40)).await;
    assert_peer_joined_message_protocol(&mut subscriber, SessionId::Integer(40)).await;

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
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
async fn fake_rtc_peers_export_transport_and_rtp_metrics_during_live_media() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    let initial_forwarded_bytes =
        assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
            .await
            + assert_audio_packet_forwarded(
                &mut publisher,
                &mut subscriber,
                &mut source,
                &mut clock,
            )
            .await;

    let before_live_metrics = wait_for_live_rtc_metrics(&network, 2).await;
    assert!(before_live_metrics.is_some());
    let Some(before_live_metrics) = before_live_metrics else {
        return;
    };
    assert_initial_live_rtc_metrics(&before_live_metrics, initial_forwarded_bytes);

    let mut additional_forwarded_bytes = 0;
    for _ in 0..4 {
        additional_forwarded_bytes +=
            assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock)
                .await;
    }

    let during_live_metrics = wait_for_live_rtc_metrics(&network, 2).await;
    assert!(during_live_metrics.is_some());
    let Some(during_live_metrics) = during_live_metrics else {
        return;
    };

    assert_steady_state_live_rtc_metrics(
        &before_live_metrics,
        &during_live_metrics,
        additional_forwarded_bytes,
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
    assert_eq!(
        after_live_metrics.transport_health_transitions_connected_to_unset
            - during_live_metrics.transport_health_transitions_connected_to_unset,
        2
    );
}

#[tokio::test]
async fn fake_rtc_peers_rebootstrap_session_replacement_without_stale_media_routes() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    assert_audio_packet_forwarded(
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
    assert_departure_message_protocol(&mut subscriber, SessionId::Integer(80)).await;
    assert_peer_joined_message_protocol(&mut subscriber, SessionId::Integer(80)).await;

    assert_audio_packet_dropped(
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
    assert_audio_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
}

#[tokio::test]
async fn fake_rtc_replacement_unpublish_and_republish_leave_no_stale_consumer_state() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-replacement-unpublish", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let setup = Box::pin(connect_audio_media_flow_peers_for_sessions(
        &network,
        &channel,
        SessionId::Integer(82),
        SessionId::Integer(83),
    ))
    .await;
    assert!(setup.is_some());
    let Some((mut initial_publisher, mut subscriber)) = setup else {
        return;
    };

    Box::pin(assert_replacement_unpublish_and_republish_flow(
        &network,
        &channel,
        &mut initial_publisher,
        &mut subscriber,
        SessionId::Integer(82),
    ))
    .await;
}

async fn assert_replacement_unpublish_and_republish_flow(
    network: &ProtocolLocalNetwork,
    channel: &str,
    initial_publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_session_id: SessionId,
) {
    let mut source = FakeMediaSource::audio();
    let mut clock = FakeClock::default();
    assert_published_audio_forwarding(
        initial_publisher,
        subscriber,
        &publisher_session_id,
        &mut source,
        &mut clock,
    )
    .await;

    let replacement = network
        .connect_fake_peer(channel, publisher_session_id.clone(), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_replacement_audio_forwarding(
        initial_publisher,
        &mut replacement,
        subscriber,
        &publisher_session_id,
        &mut source,
        &mut clock,
    )
    .await;

    assert_replacement_unpublish_and_republish_audio(
        &mut replacement,
        subscriber,
        &publisher_session_id,
        &mut source,
        &mut clock,
    )
    .await;
}

async fn assert_published_audio_forwarding(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_session_id: &SessionId,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert!(publisher.publish_track(source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(
        subscriber,
        publisher_session_id.clone(),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_audio_packet_forwarded(publisher, subscriber, source, clock).await;
}

async fn assert_replacement_audio_forwarding(
    initial_publisher: &mut ProtocolFakePeer,
    replacement: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_session_id: &SessionId,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4003))
    );
    assert_departure_message_protocol(subscriber, publisher_session_id.clone()).await;
    assert_peer_joined_message_protocol(subscriber, publisher_session_id.clone()).await;
    assert_audio_packet_dropped(initial_publisher, subscriber, source, clock).await;
    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert_published_audio_forwarding(replacement, subscriber, publisher_session_id, source, clock)
        .await;
}

async fn assert_replacement_unpublish_and_republish_audio(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    publisher_session_id: &SessionId,
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
    drain_protocol_control_plane(subscriber, Duration::from_millis(150)).await;
    assert_audio_packet_dropped(publisher, subscriber, source, clock).await;
    assert_published_audio_forwarding(publisher, subscriber, publisher_session_id, source, clock)
        .await;
}

#[tokio::test]
async fn fake_rtc_subscriber_replacement_preserves_download_mute_after_renegotiation() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-subscriber-replacement-mute", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let setup = Box::pin(connect_audio_media_flow_peers_for_sessions(
        &network,
        &channel,
        SessionId::Integer(82),
        SessionId::Integer(83),
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
        SessionId::Integer(82),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    assert_audio_packet_forwarded(&mut publisher, &mut subscriber, &mut source, &mut clock).await;

    assert!(
        subscriber
            .update_subscription(
                SessionId::Integer(82),
                DownloadStates {
                    audio: Some(false),
                    ..DownloadStates::default()
                },
            )
            .await
            .is_some()
    );
    drain_protocol_control_plane(&mut subscriber, Duration::from_millis(150)).await;

    let replacement = network
        .connect_fake_peer(&channel, SessionId::Integer(83), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        subscriber.read_close_code().await,
        Some(CloseCode::Library(4003))
    );
    assert_departure_message_protocol(&mut publisher, SessionId::Integer(83)).await;
    assert_peer_joined_message_protocol(&mut publisher, SessionId::Integer(83)).await;
    assert!(
        replacement
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );
    assert_track_snapshot(
        &mut replacement,
        SessionId::Integer(82),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(replacement.complete_next_negotiation().await.is_some());
    drain_protocol_control_plane(&mut replacement, Duration::from_millis(150)).await;

    assert_audio_packet_dropped(&mut publisher, &mut replacement, &mut source, &mut clock).await;
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_emit_presence_updates_after_rejoin() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel("issuer-replacement-rtc-info", Some(TEST_CHANNEL_KEY))
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let initial = network
        .connect_fake_peer(&channel, SessionId::Integer(84), TEST_CHANNEL_KEY)
        .await;
    let observer = network
        .connect_fake_peer(&channel, SessionId::Integer(85), TEST_CHANNEL_KEY)
        .await;
    assert!(initial.is_some());
    assert!(observer.is_some());
    let (Some(mut initial), Some(mut observer)) = (initial, observer) else {
        return;
    };

    assert_peer_joined_message_protocol(&mut initial, SessionId::Integer(85)).await;

    let replacement = network
        .connect_fake_peer(&channel, SessionId::Integer(84), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(replacement) = replacement else {
        return;
    };

    let _ = initial
        .send_info(SessionInfo {
            is_talking: Some(true),
            ..SessionInfo::default()
        })
        .await;

    assert_eq!(
        initial.read_close_code().await,
        Some(CloseCode::Library(4003))
    );
    assert_departure_message_protocol(&mut observer, SessionId::Integer(84)).await;
    assert_peer_joined_message_protocol(&mut observer, SessionId::Integer(84)).await;
    assert_no_server_message_protocol(&mut observer).await;
    assert!(replacement.close().await.is_some());
}

#[tokio::test]
async fn fake_rtc_replaced_socket_cannot_finish_a_queued_publish_negotiation() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let channel = network
        .create_channel(
            "issuer-replacement-rtc-queued-publish",
            Some(TEST_CHANNEL_KEY),
        )
        .await;
    assert!(channel.is_some());
    let Some(channel) = channel else {
        return;
    };

    let initial_publisher = network
        .connect_fake_peer(&channel, SessionId::Integer(86), TEST_CHANNEL_KEY)
        .await;
    let subscriber = network
        .connect_fake_peer(&channel, SessionId::Integer(87), TEST_CHANNEL_KEY)
        .await;
    assert!(initial_publisher.is_some());
    assert!(subscriber.is_some());
    let (Some(mut initial_publisher), Some(mut subscriber)) = (initial_publisher, subscriber)
    else {
        return;
    };

    assert!(
        initial_publisher
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

    let mut source = FakeMediaSource::audio();
    assert!(initial_publisher.publish_track(&source).await.is_some());
    let request = initial_publisher.read_next_server_request().await;
    assert!(request.is_some());
    let Some((request_id, request)) = request else {
        return;
    };
    assert!(
        matches!(request, ServerRequest::Renegotiate(_)),
        "publish should leave a renegotiation answer pending on the original socket"
    );

    let replacement = network
        .connect_fake_peer(&channel, SessionId::Integer(86), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_departure_message_protocol(&mut subscriber, SessionId::Integer(86)).await;
    assert_peer_joined_message_protocol(&mut subscriber, SessionId::Integer(86)).await;

    assert!(
        initial_publisher
            .respond_to_server_request(request_id, request)
            .await
            .is_some()
    );
    assert_no_server_message_protocol(&mut subscriber).await;

    let mut clock = FakeClock::default();
    assert_audio_packet_dropped(
        &mut initial_publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
    )
    .await;
    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4003))
    );

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
        SessionId::Integer(86),
        StreamType::Audio,
        true,
    )
    .await;
    assert!(subscriber.complete_next_negotiation().await.is_some());
    assert_audio_packet_forwarded(&mut replacement, &mut subscriber, &mut source, &mut clock).await;
}

#[tokio::test]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
async fn fake_rtc_peers_stop_forwarding_after_explicit_upload_unpublish() {
    let config = test_config(1_000, 10);

    let network = ProtocolLocalNetwork::start(config).await;
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
    network: &ProtocolLocalNetwork,
    channel: &str,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    Box::pin(connect_audio_media_flow_peers_for_sessions(
        network,
        channel,
        SessionId::Integer(70),
        SessionId::Integer(71),
    ))
    .await
}

async fn connect_audio_media_flow_peers_for_sessions(
    network: &ProtocolLocalNetwork,
    channel: &str,
    publisher_session_id: SessionId,
    subscriber_session_id: SessionId,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
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

async fn assert_audio_packet_forwarded(
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

    let received_packet = subscriber.read_rtp_packet(Duration::from_secs(2)).await;
    assert!(received_packet.is_some());
    let Some(received_packet) = received_packet else {
        return 0;
    };
    assert_eq!(
        received_packet.payload.as_ref(),
        expected_payload.as_slice()
    );
    u64::try_from(expected_payload.len()).unwrap_or(u64::MAX)
}

async fn assert_audio_packet_dropped(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let _ = publisher.send_rtp_packet(source, clock).await;
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn connect_two_isolated_audio_flows(
    network: &ProtocolLocalNetwork,
) -> Option<(
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
    ProtocolFakePeer,
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
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(subscriber, SessionId::Integer(70), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    let expected_payload = publisher.send_rtp_packet(&mut source, &mut clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return;
    };

    let received_packet = subscriber.read_rtp_packet(Duration::from_secs(2)).await;
    assert!(received_packet.is_some());
    let Some(received_packet) = received_packet else {
        return;
    };
    assert!(!received_packet.mid.is_empty());
    assert_eq!(
        received_packet.payload.as_ref(),
        expected_payload.as_slice()
    );

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

    drain_protocol_control_plane(subscriber, Duration::from_millis(150)).await;

    let next_payload = publisher.send_rtp_packet(&mut source, &mut clock).await;
    assert!(next_payload.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    let mut source = FakeMediaSource::audio();
    assert!(publisher.publish_track(&source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_track_snapshot(subscriber, SessionId::Integer(70), StreamType::Audio, true).await;
    assert!(subscriber.complete_next_negotiation().await.is_some());

    let mut clock = FakeClock::default();
    let first_payload = publisher.send_rtp_packet(&mut source, &mut clock).await;
    assert!(first_payload.is_some());
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_secs(2))
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

    drain_protocol_control_plane(subscriber, Duration::from_millis(150)).await;

    let _ = publisher.send_rtp_packet(&mut source, &mut clock).await;
    assert!(
        subscriber
            .read_rtp_packet(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn connect_camera_flow_peers(
    network: &ProtocolLocalNetwork,
    channel: &str,
) -> Option<(ProtocolFakePeer, ProtocolFakePeer)> {
    let publisher = network
        .connect_fake_peer(channel, SessionId::Integer(10), TEST_CHANNEL_KEY)
        .await?;
    let subscriber = network
        .connect_fake_peer(channel, SessionId::Integer(20), TEST_CHANNEL_KEY)
        .await?;
    Some((publisher, subscriber))
}

async fn publish_camera_track(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) -> Option<()> {
    let source = FakeMediaSource::camera();
    publisher.publish_track(&source).await?;
    publisher.complete_next_negotiation().await?;
    assert_track_snapshot(subscriber, SessionId::Integer(10), StreamType::Camera, true).await;
    Some(())
}

async fn assert_consumer_download_toggle_round_trip_protocol(subscriber: &mut ProtocolFakePeer) {
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
    let track_snapshot = subscriber.read_next_server_message().await;
    assert!(track_snapshot.is_some());
    let Some(ServerMessage::Tracks(track_snapshot)) = track_snapshot else {
        panic!("expected track snapshot after camera unpublish");
    };
    assert!(
        track_snapshot.is_empty(),
        "protocol unpublish should clear the authoritative camera track snapshot"
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
    network: &ProtocolLocalNetwork,
    channel: &str,
) -> Option<ProtocolFakePeer> {
    network
        .connect_fake_peer(channel, SessionId::Integer(30), TEST_CHANNEL_KEY)
        .await
}

async fn assert_late_join_has_no_track_snapshot(late_subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            late_subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

async fn assert_departure_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    session_id: SessionId,
) {
    let departure = subscriber.read_next_server_message().await;
    assert!(departure.is_some());
    let Some(ServerMessage::PeerLeft(departure)) = departure else {
        panic!("expected protocol peer departure notification");
    };
    assert_eq!(departure.session_id, session_id);
}

async fn assert_peer_joined_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    session_id: SessionId,
) {
    let joined = subscriber.read_next_server_message().await;
    assert!(joined.is_some());
    let Some(ServerMessage::PeerJoined(joined)) = joined else {
        panic!("expected protocol peer joined notification");
    };
    assert_eq!(joined.session_id, session_id);
}

async fn assert_track_snapshot(
    subscriber: &mut ProtocolFakePeer,
    session_id: SessionId,
    stream_type: StreamType,
    active: bool,
) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected protocol track snapshot");
    };
    assert_eq!(track_bindings.len(), 1);
    let Some(track_binding) = track_bindings.first() else {
        panic!("expected one protocol track binding");
    };
    assert_eq!(track_binding.session_id, session_id);
    assert_eq!(track_binding.stream_type, stream_type);
    assert_eq!(track_binding.active, active);
}

async fn assert_empty_track_snapshot(subscriber: &mut ProtocolFakePeer) {
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(ServerMessage::Tracks(track_bindings)) = message else {
        panic!("expected protocol track snapshot");
    };
    assert!(track_bindings.is_empty());
}

async fn drain_protocol_control_plane(subscriber: &mut ProtocolFakePeer, timeout_window: Duration) {
    while subscriber
        .read_server_message_with_timeout(timeout_window)
        .await
        .is_some()
    {}
}

async fn assert_no_server_message_protocol(subscriber: &mut ProtocolFakePeer) {
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
    network: &ProtocolLocalNetwork,
    channel: &str,
    publisher: &mut ProtocolFakePeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> Option<IncomingBitRateStats> {
    for _ in 0..20 {
        publisher.send_rtp_packets(source, clock, 2).await?;
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
    transport_health_transitions_unset_to_connected: u64,
    transport_health_transitions_connected_to_disconnected: u64,
    transport_health_transitions_connected_to_unset: u64,
    transport_ice_state_changes_new: u64,
    transport_ice_state_changes_checking: u64,
    transport_ice_state_changes_connected: u64,
    transport_ice_state_changes_completed: u64,
    transport_ice_state_changes_disconnected: u64,
    transport_dtls_connected: u64,
    rtp_packets_ingress: u64,
    rtp_packets_egress: u64,
    rtp_payload_bytes_ingress: u64,
    rtp_payload_bytes_egress: u64,
    rtp_forwarded_packets_local_rtc: u64,
    rtp_forwarded_payload_bytes_local_rtc: u64,
    indexed_routes: u64,
    scan_routes: u64,
    fallback_scans: u64,
    scan_sessions: u64,
}

async fn wait_for_transport_lifetime_metrics(
    network: &ProtocolLocalNetwork,
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
    network: &ProtocolLocalNetwork,
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
        scan_sessions: parse_prometheus_u64(metrics_text, "osfu_rtc_datagram_scan_sessions_total")?,
    })
}

fn assert_initial_live_rtc_metrics(metrics: &LiveRtcMetrics, initial_forwarded_bytes: u64) {
    assert_eq!(metrics.connected_transport_sessions, 2);
    assert_eq!(metrics.disconnected_transport_sessions, 0);
    assert!(
        metrics.transport_health_transitions_unset_to_connected >= 2,
        "expected both RTC sessions to enter a connected transport health state"
    );
    assert_eq!(
        metrics.transport_health_transitions_connected_to_disconnected,
        0
    );
    assert_eq!(metrics.transport_health_transitions_connected_to_unset, 0);
    assert!(
        metrics.transport_ice_state_changes_new + metrics.transport_ice_state_changes_checking >= 2,
        "expected both RTC sessions to emit early ICE lifecycle counters"
    );
    assert!(
        metrics.transport_ice_state_changes_connected
            + metrics.transport_ice_state_changes_completed
            >= 2,
        "expected both RTC sessions to reach a connected ICE lifecycle state"
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

fn assert_steady_state_live_rtc_metrics(
    before: &LiveRtcMetrics,
    during: &LiveRtcMetrics,
    additional_forwarded_bytes: u64,
) {
    assert_eq!(during.connected_transport_sessions, 2);
    assert_eq!(during.disconnected_transport_sessions, 0);
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
    assert_eq!(during.scan_sessions, before.scan_sessions);
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
