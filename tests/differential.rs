#![cfg(feature = "legacy-differential-tests")]
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
        CompatibilityEvent, CompatibilityTranscript, LegacySfuBackend,
        run_camera_publish_oracle_scenario_result, run_session_replacement_oracle_scenario_result,
    },
    test_config,
};

#[tokio::test]
async fn camera_publish_scenario_matches_legacy_sfu_and_expected_transcript() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let o_sfu_network = LocalNetwork::start(config).await;
    assert!(o_sfu_network.is_some());
    let Some(o_sfu_network) = o_sfu_network else {
        return;
    };
    let legacy_backend = LegacySfuBackend::start().await;
    assert!(legacy_backend.is_ok(), "{legacy_backend:?}");
    let Ok(legacy_backend) = legacy_backend else {
        return;
    };

    let o_sfu_transcript = run_camera_publish_oracle_scenario_result(&o_sfu_network).await;
    let legacy_transcript = run_camera_publish_oracle_scenario_result(&legacy_backend).await;
    assert!(o_sfu_transcript.is_ok(), "{o_sfu_transcript:?}");
    assert!(legacy_transcript.is_ok(), "{legacy_transcript:?}");
    let Ok(o_sfu_transcript) = o_sfu_transcript else {
        return;
    };
    let Ok(legacy_transcript) = legacy_transcript else {
        return;
    };

    assert_eq!(o_sfu_transcript, expected_o_sfu_transcript());
    assert_eq!(
        o_sfu_transcript.scenario_name,
        legacy_transcript.scenario_name
    );
    assert_eq!(o_sfu_transcript.events, legacy_transcript.events);
}

#[tokio::test]
async fn session_replacement_scenario_matches_legacy_sfu_and_expected_transcript() {
    let mut config = test_config(1_000, 10);
    config.transport_backend = TransportBackend::Rtc;

    let o_sfu_network = LocalNetwork::start(config).await;
    assert!(o_sfu_network.is_some());
    let Some(o_sfu_network) = o_sfu_network else {
        return;
    };
    let legacy_backend = LegacySfuBackend::start().await;
    assert!(legacy_backend.is_ok(), "{legacy_backend:?}");
    let Ok(legacy_backend) = legacy_backend else {
        return;
    };

    let o_sfu_transcript = run_session_replacement_oracle_scenario_result(&o_sfu_network).await;
    let legacy_transcript = run_session_replacement_oracle_scenario_result(&legacy_backend).await;
    assert!(o_sfu_transcript.is_ok(), "{o_sfu_transcript:?}");
    assert!(legacy_transcript.is_ok(), "{legacy_transcript:?}");
    let Ok(o_sfu_transcript) = o_sfu_transcript else {
        return;
    };
    let Ok(legacy_transcript) = legacy_transcript else {
        return;
    };

    assert_eq!(o_sfu_transcript, expected_session_replacement_transcript());
    assert_eq!(
        o_sfu_transcript.scenario_name,
        legacy_transcript.scenario_name
    );
    assert_eq!(o_sfu_transcript.events, legacy_transcript.events);
}

fn expected_o_sfu_transcript() -> CompatibilityTranscript {
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
}

fn expected_session_replacement_transcript() -> CompatibilityTranscript {
    CompatibilityTranscript {
        backend_name: "o-sfu",
        scenario_name: "session_replacement_republish",
        events: vec![
            CompatibilityEvent::SessionClosed {
                session_id: SessionId::Integer(40),
                close_code: 4003,
            },
            CompatibilityEvent::SessionDeparted {
                observer_session_id: SessionId::Integer(50),
                departed_session_id: SessionId::Integer(40),
            },
            CompatibilityEvent::RemoteTrackBootstrap {
                observer_session_id: SessionId::Integer(50),
                owner_session_id: SessionId::Integer(40),
                source_token: String::from("track-0"),
                stream_type: StreamType::Audio,
                media_kind: MediaKind::Audio,
                active: true,
            },
        ],
    }
}
