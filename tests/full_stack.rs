#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use std::time::Duration;

use o_sfu::{
    config::TransportBackend,
    signaling::{
        current_protocol::{CurrentServerMessage, CurrentServerRequest},
        http::IncomingBitRateStats,
        shared::{DownloadStates, SessionId, StreamType},
        webrtc::MediaKind,
    },
};

use crate::support::{
    TEST_CHANNEL_KEY,
    fake_media::{FakeClock, FakeMediaSource},
    fake_rtc_peer::FakeRtcPeer,
    full_stack::{FakePeer, LocalNetwork},
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
async fn fake_peers_publish_and_receive_consumer_bootstrap_over_real_server_entries() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
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

    assert!(publisher.startup().available_features.rtc);
    assert!(subscriber.startup().available_features.rtc);
    assert!(
        publisher
            .transport_bootstrap()
            .download_transport
            .id
            .starts_with("stc-rtc-")
    );
    assert!(
        subscriber
            .transport_bootstrap()
            .upload_transport
            .id
            .starts_with("cts-rtc-")
    );

    assert!(publisher.connect_transports().await.is_some());
    assert!(subscriber.connect_transports().await.is_some());

    let source = FakeMediaSource::audio();
    let producer_id = publisher.publish_track(&source).await;
    assert!(producer_id.is_some());
    let Some(producer_id) = producer_id else {
        return;
    };

    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(payload)) = request else {
        panic!("expected INIT_CONSUMER server request");
    };

    assert_eq!(payload.source_id, producer_id);
    assert_eq!(payload.session_id, SessionId::Integer(1));
    assert_eq!(payload.stream_type, StreamType::Audio);
    assert_eq!(payload.media_kind, MediaKind::Audio);
    assert!(payload.active);
}

#[tokio::test]
async fn fake_peers_keep_channel_topology_isolation_with_same_session_ids() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let peers = connect_two_isolated_audio_flows(&network).await;
    assert!(peers.is_some());
    let Some((mut publisher_a, mut subscriber_a, mut publisher_b, mut subscriber_b)) = peers else {
        return;
    };

    let source = FakeMediaSource::audio();
    let producer_a = publisher_a.publish_track(&source).await;
    assert!(producer_a.is_some());
    let Some(producer_a) = producer_a else {
        return;
    };

    assert_remote_track_bootstrap(&mut subscriber_a, &producer_a, SessionId::Integer(90)).await;
    assert_no_server_request(&mut subscriber_b).await;

    let producer_b = publisher_b.publish_track(&source).await;
    assert!(producer_b.is_some());
    let Some(producer_b) = producer_b else {
        return;
    };

    assert_remote_track_bootstrap(&mut subscriber_b, &producer_b, SessionId::Integer(90)).await;

    assert!(producer_a.starts_with("producer-"));
    assert!(producer_b.starts_with("producer-"));

    assert!(publisher_a.close().await.is_some());
    assert_departure_message(&mut subscriber_a, SessionId::Integer(90)).await;
    assert_no_server_message(&mut subscriber_b).await;
}

#[tokio::test]
async fn fake_peers_cover_publish_mute_late_join_and_disconnect_deterministically() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
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

    let producer_id = publish_camera_track(&mut publisher, &mut subscriber).await;
    assert!(producer_id.is_some());
    let Some(producer_id) = producer_id else {
        return;
    };

    assert_consumer_download_toggle_round_trip(&mut subscriber).await;
    assert_camera_info_update(&mut publisher, &mut subscriber, true).await;
    assert_camera_info_update(&mut publisher, &mut subscriber, false).await;

    let late_subscriber = connect_late_subscriber(&network, &channel).await;
    assert!(late_subscriber.is_some());
    let Some(mut late_subscriber) = late_subscriber else {
        return;
    };
    assert_late_join_consumer_is_inactive(&mut late_subscriber, &producer_id).await;

    assert!(publisher.close().await.is_some());
    assert_departure_message(&mut subscriber, SessionId::Integer(10)).await;
    assert_departure_message(&mut late_subscriber, SessionId::Integer(10)).await;
}

#[tokio::test]
async fn fake_peers_cover_session_replacement_and_republish_deterministically() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
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

    assert!(initial_publisher.connect_transports().await.is_some());
    assert!(subscriber.connect_transports().await.is_some());

    let replacement = network
        .connect_fake_peer(&channel, SessionId::Integer(40), TEST_CHANNEL_KEY)
        .await;
    assert!(replacement.is_some());
    let Some(mut replacement) = replacement else {
        return;
    };

    assert_eq!(
        initial_publisher.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_departure_message(&mut subscriber, SessionId::Integer(40)).await;

    assert!(replacement.connect_transports().await.is_some());
    let source = FakeMediaSource::audio();
    let producer_id = replacement.publish_track(&source).await;
    assert!(producer_id.is_some());
    let Some(producer_id) = producer_id else {
        return;
    };

    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(payload)) = request else {
        panic!("expected INIT_CONSUMER after session replacement");
    };
    assert_eq!(payload.source_id, producer_id);
    assert_eq!(payload.session_id, SessionId::Integer(40));
    assert_eq!(payload.stream_type, StreamType::Audio);
    assert_eq!(payload.media_kind, MediaKind::Audio);
    assert!(payload.active);
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
    let publisher = network
        .connect_fake_peer(channel, SessionId::Integer(70), TEST_CHANNEL_KEY)
        .await?;
    let subscriber = network
        .connect_fake_peer(channel, SessionId::Integer(71), TEST_CHANNEL_KEY)
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

async fn connect_two_isolated_audio_flows(
    network: &LocalNetwork,
) -> Option<(FakePeer, FakePeer, FakePeer, FakePeer)> {
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
    publisher_a.connect_transports().await?;
    subscriber_a.connect_transports().await?;
    publisher_b.connect_transports().await?;
    subscriber_b.connect_transports().await?;
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
            .set_download_state(
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

async fn assert_remote_track_bootstrap(
    subscriber: &mut FakePeer,
    producer_id: &str,
    session_id: SessionId,
) {
    let request = subscriber.read_next_server_request().await;
    assert!(request.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(request)) = request else {
        panic!("expected INIT_CONSUMER");
    };
    assert_eq!(request.source_id, producer_id);
    assert_eq!(request.session_id, session_id);
    assert_eq!(request.stream_type, StreamType::Audio);
    assert_eq!(request.media_kind, MediaKind::Audio);
    assert!(request.active);
}

async fn assert_no_server_request(subscriber: &mut FakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            subscriber.read_next_server_request()
        )
        .await
        .is_err()
    );
}

async fn assert_no_server_message(subscriber: &mut FakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            subscriber.read_next_server_message()
        )
        .await
        .is_err()
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
    network: &LocalNetwork,
    channel: &str,
) -> Option<(FakePeer, FakePeer)> {
    let publisher = network
        .connect_fake_peer(channel, SessionId::Integer(10), TEST_CHANNEL_KEY)
        .await?;
    let subscriber = network
        .connect_fake_peer(channel, SessionId::Integer(20), TEST_CHANNEL_KEY)
        .await?;
    let mut publisher = publisher;
    let mut subscriber = subscriber;
    publisher.connect_transports().await?;
    subscriber.connect_transports().await?;
    Some((publisher, subscriber))
}

async fn publish_camera_track(
    publisher: &mut FakePeer,
    subscriber: &mut FakePeer,
) -> Option<String> {
    let source = FakeMediaSource::camera();
    let producer_id = publisher.publish_track(&source).await?;
    let first_consumer = subscriber.read_next_server_request().await;
    assert!(first_consumer.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(first_consumer)) = first_consumer else {
        panic!("expected first INIT_CONSUMER");
    };
    assert_eq!(first_consumer.source_id, producer_id);
    assert_eq!(first_consumer.stream_type, StreamType::Camera);
    assert!(first_consumer.active);
    Some(producer_id)
}

async fn assert_consumer_download_toggle_round_trip(subscriber: &mut FakePeer) {
    assert!(
        subscriber
            .set_download_state(
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
            .set_download_state(
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

async fn assert_camera_info_update(
    publisher: &mut FakePeer,
    subscriber: &mut FakePeer,
    active: bool,
) {
    assert!(
        publisher
            .set_upload_active(StreamType::Camera, active)
            .await
            .is_some()
    );
    let message = subscriber.read_next_server_message().await;
    assert!(message.is_some());
    let Some(CurrentServerMessage::SessionInfoChanged(snapshot)) = message else {
        panic!("expected session info change after camera state update");
    };
    assert_eq!(
        snapshot.get("10").and_then(|info| info.is_camera_on),
        Some(active)
    );
}

async fn connect_late_subscriber(network: &LocalNetwork, channel: &str) -> Option<FakePeer> {
    let mut late_subscriber = network
        .connect_fake_peer(channel, SessionId::Integer(30), TEST_CHANNEL_KEY)
        .await?;
    late_subscriber.connect_transports().await?;
    Some(late_subscriber)
}

async fn assert_late_join_consumer_is_inactive(late_subscriber: &mut FakePeer, producer_id: &str) {
    let late_consumer = late_subscriber.read_next_server_request().await;
    assert!(late_consumer.is_some());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(late_consumer)) = late_consumer else {
        panic!("expected late-join INIT_CONSUMER");
    };
    assert_eq!(late_consumer.source_id, producer_id);
    assert_eq!(late_consumer.stream_type, StreamType::Camera);
    assert_eq!(late_consumer.media_kind, MediaKind::Video);
    assert!(!late_consumer.active);
}

async fn assert_departure_message(subscriber: &mut FakePeer, session_id: SessionId) {
    let departure = subscriber.read_next_server_message().await;
    assert!(departure.is_some());
    let Some(CurrentServerMessage::SessionDeparted(departure)) = departure else {
        panic!("expected departure notification");
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
