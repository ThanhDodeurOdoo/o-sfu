use super::fixtures::*;
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    engine::{
        AvailableFeatures, RecordingOptions, RecordingState,
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        room::{RoomManagerConfig, RoomManagerDeps, RoomRuntimePolicy, rtp_capabilities},
    },
};

async fn build_recording_room() -> (
    Arc<super::super::Room>,
    Arc<RuntimeMetrics>,
    UserOutboundReceiver,
    UserOutboundReceiver,
) {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = RoomManager::new(
        RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(100),
                RuntimeFeatureFlags {
                    transcription: true,
                    audio_recording: true,
                    video_recording: true,
                },
                rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        RoomManagerDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let room = manager
        .serve_room(
            "issuer-recording",
            TEST_ROOM_KEY,
            &RoomConfig {
                recording_address: Some("https://record.example.com".to_owned()),
                ..RoomConfig::default()
            },
            None,
        )
        .await;
    let (tx1, rx1) = test_sender();
    let (tx2, rx2) = test_sender();
    room.test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(1),
            None,
            UserPermissions {
                transcription: Some(true),
                audio_recording: Some(true),
                video_recording: Some(true),
            },
            tx1,
        )
        .await
        .expect("recording publisher should join");
    room.test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await
        .expect("recording observer should join");
    (room, metrics, rx1, rx2)
}

fn audio_recording_options() -> RecordingOptions {
    RecordingOptions {
        audio: Some(true),
        video: Some(false),
        transcription: Some(false),
    }
}

fn inactive_recording_state() -> RecordingState {
    RecordingState {
        recording: Some(false),
        audio: Some(false),
        transcription: Some(false),
        video: Some(false),
    }
}

async fn assert_no_recording_message(receiver: &mut UserOutboundReceiver, message: &str) {
    assert!(
        timeout(Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "{message}"
    );
}

#[tokio::test]
async fn recording_features_stay_hidden_until_persistent_backend_exists() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room().await;

    assert_eq!(
        room.available_features(),
        AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );
    assert_eq!(room.recording_state().await, inactive_recording_state());
    assert_no_recording_message(
        &mut publisher_rx,
        "hidden recording capability must not notify the requester",
    )
    .await;
    assert_no_recording_message(
        &mut observer_rx,
        "hidden recording capability must not notify observers",
    )
    .await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted(), 0);
    assert_eq!(metrics_snapshot.recording_start_rejected(), 0);
    assert_eq!(metrics_snapshot.active_recording_rooms(), 0);
}

#[tokio::test]
async fn recording_start_rejects_until_persistent_backend_exists() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room().await;

    let publisher_id = UserId::Integer(1);
    assert!(!room.apply_recording_start(
        &publisher_id,
        user_connection_id(&room, &publisher_id).await,
        audio_recording_options(),
    ));
    assert_eq!(room.recording_state().await, inactive_recording_state());
    assert_no_recording_message(
        &mut publisher_rx,
        "backend-gated recording start must not notify the requester",
    )
    .await;
    assert_no_recording_message(
        &mut observer_rx,
        "backend-gated recording start must not notify observers",
    )
    .await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted(), 0);
    assert_eq!(metrics_snapshot.recording_start_rejected(), 1);
    assert_eq!(metrics_snapshot.active_recording_rooms(), 0);
}

#[tokio::test]
async fn recording_stop_rejects_when_recording_is_unavailable() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room().await;

    let publisher_id = UserId::Integer(1);
    assert!(!room.apply_recording_stop(
        &publisher_id,
        user_connection_id(&room, &publisher_id).await
    ));
    assert_eq!(room.recording_state().await, inactive_recording_state());
    assert_no_recording_message(
        &mut publisher_rx,
        "backend-gated recording stop must not notify the requester",
    )
    .await;
    assert_no_recording_message(
        &mut observer_rx,
        "backend-gated recording stop must not notify observers",
    )
    .await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_stop_accepted(), 0);
    assert_eq!(metrics_snapshot.recording_stop_rejected(), 1);
    assert_eq!(metrics_snapshot.active_recording_rooms(), 0);
}
