use super::fixtures::*;
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        AvailableFeatures, RecordingOptions, RecordingState,
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        room::{RoomManagerConfig, RoomManagerDeps, RoomRuntimePolicy, rtp_capabilities},
    },
};

async fn build_recording_room() -> (
    Arc<super::super::Room>,
    Arc<RuntimeMetrics>,
    mpsc::UnboundedReceiver<UserOutbound>,
    mpsc::UnboundedReceiver<UserOutbound>,
) {
    build_recording_room_with(
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
        Some("https://record.example.com"),
        UserPermissions {
            transcription: Some(true),
            audio_recording: Some(true),
            video_recording: Some(true),
        },
        UserPermissions::default(),
    )
    .await
}

async fn build_recording_room_with(
    feature_flags: RuntimeFeatureFlags,
    recording_address: Option<&str>,
    publisher_permissions: UserPermissions,
    observer_permissions: UserPermissions,
) -> (
    Arc<super::super::Room>,
    Arc<RuntimeMetrics>,
    mpsc::UnboundedReceiver<UserOutbound>,
    mpsc::UnboundedReceiver<UserOutbound>,
) {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = RoomManager::new(
        RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(100),
                feature_flags,
                rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        RoomManagerDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::clone(&metrics),
        },
    );
    let room = manager
        .serve_room(
            "issuer-recording",
            None,
            &RoomConfig {
                recording_address: recording_address.map(str::to_owned),
                ..RoomConfig::default()
            },
            None,
        )
        .await;
    let (tx1, rx1) = test_sender();
    let (tx2, rx2) = test_sender();
    room.test_api()
        .lifecycle()
        .join_user(UserId::Integer(1), None, publisher_permissions, tx1)
        .await
        .expect("recording publisher should join");
    room.test_api()
        .lifecycle()
        .join_user(UserId::Integer(2), None, observer_permissions, tx2)
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

async fn assert_no_recording_message(
    receiver: &mut mpsc::UnboundedReceiver<UserOutbound>,
    message: &str,
) {
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

    assert!(
        !room
            .test_api()
            .lifecycle()
            .start_recording(&UserId::Integer(1), audio_recording_options())
            .await
    );
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
async fn stale_and_current_connections_cannot_bypass_recording_backend_gate() {
    let (room, metrics, _publisher_rx, mut observer_rx) = build_recording_room().await;
    let stale_connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&UserId::Integer(1))
        .await
        .expect("recording publisher should have a connection id");
    let (replacement_tx, mut replacement_rx) = test_sender();
    let replacement_connection_id = room
        .test_api()
        .lifecycle()
        .join_user(
            UserId::Integer(1),
            Some(String::from("replacement")),
            UserPermissions {
                transcription: Some(true),
                audio_recording: Some(true),
                video_recording: Some(true),
            },
            replacement_tx,
        )
        .await
        .expect("replacement publisher should join");
    drain_outbound(&mut replacement_rx);
    drain_outbound(&mut observer_rx);

    assert!(
        !room
            .start_recording_runtime(
                &UserId::Integer(1),
                stale_connection_id,
                audio_recording_options(),
            )
            .await
    );
    assert!(
        !room
            .start_recording_runtime(
                &UserId::Integer(1),
                replacement_connection_id,
                audio_recording_options(),
            )
            .await
    );
    assert_eq!(room.recording_state().await, inactive_recording_state());
    assert!(
        replacement_rx.try_recv().is_err(),
        "recording rejection must not fan out room updates"
    );
    assert!(
        observer_rx.try_recv().is_err(),
        "recording rejection must not notify observers"
    );

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted(), 0);
    assert_eq!(metrics_snapshot.recording_start_rejected(), 2);
}

#[tokio::test]
async fn recording_start_rejects_rooms_without_recording_address() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room_with(
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
        None,
        UserPermissions {
            transcription: Some(true),
            audio_recording: Some(true),
            video_recording: Some(true),
        },
        UserPermissions::default(),
    )
    .await;

    assert!(
        !room
            .test_api()
            .lifecycle()
            .start_recording(&UserId::Integer(1), audio_recording_options())
            .await
    );
    assert_eq!(room.recording_state().await, inactive_recording_state());
    assert_no_recording_message(
        &mut publisher_rx,
        "recording start without a recording address must not notify the requester",
    )
    .await;
    assert_no_recording_message(
        &mut observer_rx,
        "recording start without a recording address must not notify observers",
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

    assert!(
        !room
            .test_api()
            .lifecycle()
            .stop_recording(&UserId::Integer(1))
            .await
    );
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
