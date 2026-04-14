#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use std::time::Duration;

use o_sfu::{
    config::TransportBackend,
    runtime::testing::legacy_wire::current_protocol::{
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentServerRequest,
    },
    signaling::{
        http::IncomingBitRateStats,
        protocol::ServerMessage,
        shared::{DownloadStates, SessionId, StreamType},
        webrtc::MediaKind,
    },
};

use crate::support::{
    TEST_CHANNEL_KEY,
    fake_media::{FakeClock, FakeMediaSource},
    fake_rtc_peer::FakeRtcPeer,
    full_stack::{FakePeer, LocalNetwork},
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

    assert!(publish_camera_track(&mut publisher, &mut subscriber).await.is_some());

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

    let network = LocalNetwork::start(config).await;
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
    let rtc_peer =
        FakeRtcPeer::connect_publisher(&publisher.transport_bootstrap().upload_transport, &source)
            .await;
    assert!(rtc_peer.is_some());
    let Some(mut rtc_peer) = rtc_peer else {
        return;
    };

    assert!(
        publisher
            .connect_transports_with_ice(
                rtc_peer.local_dtls_parameters(),
                Some(rtc_peer.local_ice_parameters().clone()),
            )
            .await
            .is_some()
    );
    assert!(subscriber.connect_transports().await.is_some());
    assert!(
        rtc_peer
            .wait_until_connected(Duration::from_secs(5))
            .await
            .is_some()
    );

    let producer_id = publisher.publish_track(&source).await;
    assert!(producer_id.is_some());
    let Some(producer_id) = producer_id else {
        return;
    };

    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(payload)) = request else {
        panic!("expected INIT_CONSUMER after real RTC publisher setup");
    };
    assert_eq!(payload.source_id, producer_id);
    assert_eq!(payload.session_id, SessionId::Integer(60));
    assert_eq!(payload.stream_type, StreamType::Audio);
    assert_eq!(payload.media_kind, MediaKind::Audio);
    assert!(payload.active);

    let mut clock = FakeClock::default();
    let stats = stream_until_audio_bitrate_is_observable(
        &network,
        &channel,
        &mut rtc_peer,
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
#[allow(
    clippy::large_futures,
    reason = "the integration test intentionally awaits a setup helper that returns owned fake peers and rtc handles"
)]
async fn fake_rtc_peers_rebootstrap_session_replacement_without_stale_media_routes() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
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

    let setup = connect_audio_media_flow_peers_for_sessions(
        &network,
        &channel,
        SessionId::Integer(80),
        SessionId::Integer(81),
    )
    .await;
    assert!(setup.is_some());
    let Some((
        mut initial_publisher,
        mut subscriber,
        mut initial_publisher_rtc,
        mut subscriber_rtc,
    )) = setup
    else {
        return;
    };

    let mut source = FakeMediaSource::audio();
    let initial_track = publish_audio_track_and_expect_bootstrap(
        &mut initial_publisher,
        &mut subscriber,
        &source,
        80,
    )
    .await;
    assert!(subscriber_rtc.expect_remote_track(&initial_track).is_some());

    let mut clock = FakeClock::default();
    assert_audio_frame_forwarded(
        &mut initial_publisher_rtc,
        &mut subscriber_rtc,
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
    assert_departure_message(&mut subscriber, SessionId::Integer(80)).await;

    assert_audio_frame_dropped(
        &mut initial_publisher_rtc,
        &mut subscriber_rtc,
        &mut source,
        &mut clock,
    )
    .await;

    let replacement_rtc = connect_rtc_publisher(&mut replacement, &source).await;
    assert!(replacement_rtc.is_some());
    let Some(mut replacement_rtc) = replacement_rtc else {
        return;
    };

    let _replacement_track =
        publish_audio_track_and_expect_bootstrap(&mut replacement, &mut subscriber, &source, 80)
            .await;
    assert_audio_frame_forwarded(
        &mut replacement_rtc,
        &mut subscriber_rtc,
        &mut source,
        &mut clock,
    )
    .await;
}

#[tokio::test]
#[allow(
    clippy::large_futures,
    reason = "the integration test intentionally awaits a setup helper that returns owned fake peers and rtc handles"
)]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
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

    let setup = connect_audio_media_flow_peers(&network, &channel).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber, mut publisher_rtc, mut subscriber_rtc)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_download_mute_stops_flow(
        &mut publisher,
        &mut subscriber,
        &mut publisher_rtc,
        &mut subscriber_rtc,
    )
    .await;
}

#[tokio::test]
#[allow(
    clippy::large_futures,
    reason = "the integration test intentionally awaits a setup helper that returns owned fake peers and rtc handles"
)]
async fn fake_rtc_peers_cover_explicit_upload_unpublish_compatibility_semantics() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
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

    let setup = connect_audio_media_flow_peers(&network, &channel).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber, mut publisher_rtc, mut subscriber_rtc)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
        &mut publisher,
        &mut subscriber,
        &mut publisher_rtc,
        &mut subscriber_rtc,
    )
    .await;
}

#[allow(
    clippy::large_futures,
    reason = "the integration-only setup helper returns owned fake peers and rtc handles for one scenario"
)]
async fn connect_audio_media_flow_peers(
    network: &LocalNetwork,
    channel: &str,
) -> Option<(FakePeer, FakePeer, FakeRtcPeer, FakeRtcPeer)> {
    connect_audio_media_flow_peers_for_sessions(
        network,
        channel,
        SessionId::Integer(70),
        SessionId::Integer(71),
    )
    .await
}

#[allow(
    clippy::large_futures,
    reason = "the integration-only setup helper returns owned fake peers and rtc handles for one scenario"
)]
async fn connect_audio_media_flow_peers_for_sessions(
    network: &LocalNetwork,
    channel: &str,
    publisher_session_id: SessionId,
    subscriber_session_id: SessionId,
) -> Option<(FakePeer, FakePeer, FakeRtcPeer, FakeRtcPeer)> {
    let publisher = network
        .connect_fake_peer(channel, publisher_session_id, TEST_CHANNEL_KEY)
        .await?;
    let subscriber = network
        .connect_fake_peer(channel, subscriber_session_id, TEST_CHANNEL_KEY)
        .await?;
    let mut publisher = publisher;
    let mut subscriber = subscriber;

    let source = FakeMediaSource::audio();
    let mut publisher_rtc =
        FakeRtcPeer::connect_publisher(&publisher.transport_bootstrap().upload_transport, &source)
            .await?;
    let mut subscriber_rtc =
        FakeRtcPeer::connect_subscriber(&subscriber.transport_bootstrap().download_transport)
            .await?;

    publisher
        .connect_transports_with_ice(
            publisher_rtc.local_dtls_parameters(),
            Some(publisher_rtc.local_ice_parameters().clone()),
        )
        .await?;
    subscriber
        .connect_transports_with_ice(
            subscriber_rtc.local_dtls_parameters(),
            Some(subscriber_rtc.local_ice_parameters().clone()),
        )
        .await?;
    publisher_rtc
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    subscriber_rtc
        .wait_until_connected(Duration::from_secs(5))
        .await?;

    Some((publisher, subscriber, publisher_rtc, subscriber_rtc))
}

async fn publish_audio_track_and_expect_bootstrap(
    publisher: &mut FakePeer,
    subscriber: &mut FakePeer,
    source: &FakeMediaSource,
    session_id: i64,
) -> CurrentRemoteTrackBootstrapPayload {
    let producer_id = publisher.publish_track(source).await;
    assert!(producer_id.is_some());
    let Some(producer_id) = producer_id else {
        panic!("publisher should return a producer id for audio publish");
    };

    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(track)) = request else {
        panic!("expected INIT_CONSUMER for audio publish");
    };
    assert_eq!(track.source_id, producer_id);
    assert_eq!(track.session_id, SessionId::Integer(session_id));
    assert_eq!(track.stream_type, StreamType::Audio);
    assert_eq!(track.media_kind, MediaKind::Audio);
    assert!(track.active);
    track
}

async fn assert_audio_frame_forwarded(
    publisher_rtc: &mut FakeRtcPeer,
    subscriber_rtc: &mut FakeRtcPeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher_rtc.send_frame(source, clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return;
    };

    let received_frame = subscriber_rtc
        .read_media_frame(Duration::from_secs(2))
        .await;
    assert!(received_frame.is_some());
    let Some(received_frame) = received_frame else {
        return;
    };
    assert_eq!(received_frame.payload, expected_payload);
}

async fn assert_audio_frame_dropped(
    publisher_rtc: &mut FakeRtcPeer,
    subscriber_rtc: &mut FakeRtcPeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) {
    let expected_payload = publisher_rtc.send_frame(source, clock).await;
    assert!(expected_payload.is_some());
    assert!(
        subscriber_rtc
            .read_media_frame(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn connect_rtc_publisher(
    publisher: &mut FakePeer,
    source: &FakeMediaSource,
) -> Option<FakeRtcPeer> {
    let mut publisher_rtc =
        FakeRtcPeer::connect_publisher(&publisher.transport_bootstrap().upload_transport, source)
            .await?;
    publisher
        .connect_transports_with_ice(
            publisher_rtc.local_dtls_parameters(),
            Some(publisher_rtc.local_ice_parameters().clone()),
        )
        .await?;
    publisher_rtc
        .wait_until_connected(Duration::from_secs(5))
        .await?;
    Some(publisher_rtc)
}

async fn connect_two_isolated_audio_flows(
    network: &NativeLocalNetwork,
) -> Option<(NativeFakePeer, NativeFakePeer, NativeFakePeer, NativeFakePeer)> {
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

    Some((publisher_a, subscriber_a, publisher_b, subscriber_b))
}

async fn assert_audio_media_arrives_and_download_mute_stops_flow(
    publisher: &mut FakePeer,
    subscriber: &mut FakePeer,
    publisher_rtc: &mut FakeRtcPeer,
    subscriber_rtc: &mut FakeRtcPeer,
) {
    let mut source = FakeMediaSource::audio();
    let producer_id = publisher.publish_track(&source).await;
    assert!(producer_id.is_some());

    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(track)) = request else {
        panic!("expected INIT_CONSUMER for browserless media-flow assertion");
    };
    assert_eq!(track.session_id, SessionId::Integer(70));
    assert_eq!(track.stream_type, StreamType::Audio);
    assert_eq!(track.media_kind, MediaKind::Audio);
    assert!(subscriber_rtc.expect_remote_track(&track).is_some());

    let mut clock = FakeClock::default();
    let expected_payload = publisher_rtc.send_frame(&mut source, &mut clock).await;
    assert!(expected_payload.is_some());
    let Some(expected_payload) = expected_payload else {
        return;
    };

    let received_frame = subscriber_rtc
        .read_media_frame(Duration::from_secs(2))
        .await;
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

    let next_payload = publisher_rtc.send_frame(&mut source, &mut clock).await;
    assert!(next_payload.is_some());
    assert!(
        subscriber_rtc
            .read_media_frame(Duration::from_millis(300))
            .await
            .is_none()
    );
}

async fn assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
    publisher: &mut FakePeer,
    subscriber: &mut FakePeer,
    publisher_rtc: &mut FakeRtcPeer,
    subscriber_rtc: &mut FakeRtcPeer,
) {
    let mut source = FakeMediaSource::audio();
    let producer_id = publisher.publish_track(&source).await;
    assert!(producer_id.is_some());

    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(track)) = request else {
        panic!("expected INIT_CONSUMER before explicit upload unpublish");
    };
    assert_eq!(track.session_id, SessionId::Integer(70));
    assert_eq!(track.stream_type, StreamType::Audio);
    assert_eq!(track.media_kind, MediaKind::Audio);
    assert!(subscriber_rtc.expect_remote_track(&track).is_some());

    let mut clock = FakeClock::default();
    let first_payload = publisher_rtc.send_frame(&mut source, &mut clock).await;
    assert!(first_payload.is_some());
    assert!(
        subscriber_rtc
            .read_media_frame(Duration::from_secs(2))
            .await
            .is_some()
    );

    assert!(
        publisher
            .unpublish_upload(StreamType::Audio)
            .await
            .is_some()
    );

    let second_payload = publisher_rtc.send_frame(&mut source, &mut clock).await;
    assert!(second_payload.is_some());
    assert!(
        subscriber_rtc
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

async fn assert_departure_message(subscriber: &mut FakePeer, session_id: SessionId) {
    let departure = subscriber.read_next_server_message().await;
    assert!(departure.is_some());
    let Some(CurrentServerMessage::SessionDeparted(departure)) = departure else {
        panic!("expected legacy departure notification");
    };
    assert_eq!(departure.session_id, session_id);
}

async fn stream_until_audio_bitrate_is_observable(
    network: &LocalNetwork,
    channel: &str,
    rtc_peer: &mut FakeRtcPeer,
    source: &mut FakeMediaSource,
    clock: &mut FakeClock,
) -> Option<IncomingBitRateStats> {
    for _ in 0..20 {
        rtc_peer.send_frames(source, clock, 2).await?;
        let stats = network.stats().await?;
        let channel_stats = stats.into_iter().find(|entry| entry.uuid == channel)?;
        if channel_stats.sessions_stats.incoming_bit_rate.audio > 0 {
            return Some(channel_stats.sessions_stats.incoming_bit_rate);
        }
        sleep(Duration::from_millis(25)).await;
    }
    None
}
