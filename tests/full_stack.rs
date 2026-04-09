#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use std::time::Duration;

use o_sfu::{
    config::TransportBackend,
    signaling::{
        current_protocol::CurrentServerRequest,
        shared::{SessionId, StreamType},
        webrtc::MediaKind,
    },
};

use crate::support::{
    TEST_CHANNEL_KEY,
    fake_media::{FakeClock, FakeMediaSource},
    full_stack::LocalNetwork,
    test_config,
};

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
