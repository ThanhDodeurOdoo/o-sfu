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
        shared::{DownloadStates, SessionId, StreamType},
        webrtc::MediaKind,
    },
};

use crate::support::{
    TEST_CHANNEL_KEY,
    fake_media::{FakeClock, FakeMediaSource},
    full_stack::{FakePeer, LocalNetwork},
    test_config,
};
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
