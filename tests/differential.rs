#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

mod support;

use o_sfu::{
    config::TransportBackend,
    signaling::{
        shared::{SessionId, StreamType},
        webrtc::MediaKind,
    },
};

use crate::support::full_stack::LocalNetwork;
use crate::support::{
    differential::{
        CompatibilityEvent, CompatibilityTranscript, run_camera_publish_oracle_scenario,
    },
    test_config,
};

#[tokio::test]
async fn camera_publish_scenario_produces_a_normalized_compatibility_transcript() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let network = LocalNetwork::start(config).await;
    assert!(network.is_some());
    let Some(network) = network else {
        return;
    };

    let transcript = run_camera_publish_oracle_scenario(&network).await;
    assert!(transcript.is_some());
    let Some(transcript) = transcript else {
        return;
    };

    assert_eq!(
        transcript,
        CompatibilityTranscript {
            backend_name: "o-sfu",
            scenario_name: "camera_publish_toggle_late_join_departure",
            events: vec![
                CompatibilityEvent::RemoteTrackBootstrap {
                    observer_session_id: SessionId::Integer(20),
                    owner_session_id: SessionId::Integer(10),
                    source_token: String::from("track-0"),
                    stream_type: StreamType::Camera,
                    media_kind: MediaKind::Video,
                    active: true,
                },
                CompatibilityEvent::SessionCameraState {
                    observer_session_id: SessionId::Integer(20),
                    owner_session_id: SessionId::Integer(10),
                    active: true,
                },
                CompatibilityEvent::SessionCameraState {
                    observer_session_id: SessionId::Integer(20),
                    owner_session_id: SessionId::Integer(10),
                    active: false,
                },
                CompatibilityEvent::RemoteTrackBootstrap {
                    observer_session_id: SessionId::Integer(30),
                    owner_session_id: SessionId::Integer(10),
                    source_token: String::from("track-0"),
                    stream_type: StreamType::Camera,
                    media_kind: MediaKind::Video,
                    active: false,
                },
                CompatibilityEvent::SessionDeparted {
                    observer_session_id: SessionId::Integer(20),
                    departed_session_id: SessionId::Integer(10),
                },
                CompatibilityEvent::SessionDeparted {
                    observer_session_id: SessionId::Integer(30),
                    departed_session_id: SessionId::Integer(10),
                },
            ],
        }
    );
}
