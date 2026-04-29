use super::fixtures::*;
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        RecordingOptions, RecordingState, RecordingStateUpdate, StopCode,
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        recording::MediaTap,
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
            recording_media_tap: Arc::new(MediaTap::default()),
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

async fn expect_recording_message(
    receiver: &mut mpsc::UnboundedReceiver<UserOutbound>,
) -> RecordingStateUpdate {
    let outbound = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("recording update should arrive before timeout")
        .expect("recording update room should stay open");
    let UserOutbound::Message(RoomEventMessage::RecordingStateChanged(update)) = outbound else {
        panic!("expected room recording update, got {outbound:?}");
    };
    update
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
async fn recording_start_and_stop_update_room_state_for_all_users() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room().await;

    assert!(
        room.test_api()
            .lifecycle()
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(true),
                },
            )
            .await
    );
    assert_eq!(
        room.recording_state().await,
        RecordingState {
            recording: Some(true),
            audio: Some(true),
            transcription: Some(true),
            video: Some(false),
        }
    );

    let publisher_start = expect_recording_message(&mut publisher_rx).await;
    let observer_start = expect_recording_message(&mut observer_rx).await;
    assert_eq!(publisher_start.stop_code, None);
    assert_eq!(observer_start, publisher_start);
    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted, 1);
    assert_eq!(metrics_snapshot.active_recording_rooms, 1);

    assert!(
        room.test_api()
            .lifecycle()
            .stop_recording(&UserId::Integer(1))
            .await
    );
    assert_eq!(
        room.recording_state().await,
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );

    let publisher_stop = expect_recording_message(&mut publisher_rx).await;
    let observer_stop = expect_recording_message(&mut observer_rx).await;
    assert_eq!(publisher_stop.stop_code, Some(StopCode::UserRequest));
    assert_eq!(observer_stop, publisher_stop);
    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_stop_accepted, 1);
    assert_eq!(metrics_snapshot.active_recording_rooms, 0);
}

#[tokio::test]
async fn recording_allows_transcription_toggle_but_rejects_new_media_while_active() {
    let (room, metrics, mut publisher_rx, _observer_rx) = build_recording_room().await;

    assert!(
        room.test_api()
            .lifecycle()
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    let _start_update = expect_recording_message(&mut publisher_rx).await;

    assert!(
        room.test_api()
            .lifecycle()
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: None,
                    video: None,
                    transcription: Some(true),
                },
            )
            .await
    );
    let transcription_update = expect_recording_message(&mut publisher_rx).await;
    assert_eq!(
        transcription_update.state,
        RecordingState {
            recording: Some(true),
            audio: Some(true),
            transcription: Some(true),
            video: Some(false),
        }
    );

    assert!(
        !room
            .test_api()
            .lifecycle()
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: None,
                    transcription: None,
                },
            )
            .await
    );
    assert!(
        timeout(Duration::from_millis(50), publisher_rx.recv())
            .await
            .is_err(),
        "no extra recording update should be emitted for rejected reconfiguration"
    );
    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted, 2);
    assert_eq!(metrics_snapshot.recording_start_rejected, 1);
    assert_eq!(metrics_snapshot.active_recording_rooms, 1);
}

#[tokio::test]
async fn stale_replaced_connection_cannot_start_or_stop_recording() {
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
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    assert_eq!(
        room.recording_state().await,
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert!(
        replacement_rx.try_recv().is_err(),
        "stale recording start must not fan out room updates"
    );
    assert!(
        observer_rx.try_recv().is_err(),
        "stale recording start must not notify observers"
    );

    assert!(
        room.start_recording_runtime(
            &UserId::Integer(1),
            replacement_connection_id,
            RecordingOptions {
                audio: Some(true),
                video: Some(false),
                transcription: Some(false),
            },
        )
        .await
    );
    let _start_update = expect_recording_message(&mut replacement_rx).await;
    let _observer_start = expect_recording_message(&mut observer_rx).await;

    assert!(
        !room
            .stop_recording_runtime(&UserId::Integer(1), stale_connection_id)
            .await
    );
    assert_eq!(room.recording_state().await.recording, Some(true));
    assert!(
        replacement_rx.try_recv().is_err(),
        "stale recording stop must not fan out room updates"
    );
    assert!(
        observer_rx.try_recv().is_err(),
        "stale recording stop must not notify observers"
    );

    assert!(
        room.stop_recording_runtime(&UserId::Integer(1), replacement_connection_id)
            .await
    );
    let _stop_update = expect_recording_message(&mut replacement_rx).await;
    let _observer_stop = expect_recording_message(&mut observer_rx).await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_rejected, 1);
    assert_eq!(metrics_snapshot.recording_stop_rejected, 1);
}

#[tokio::test]
async fn recording_start_rejects_users_without_recording_permissions() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room_with(
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
        Some("https://record.example.com"),
        UserPermissions::default(),
        UserPermissions::default(),
    )
    .await;

    assert!(
        !room
            .test_api()
            .lifecycle()
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    assert_eq!(
        room.recording_state().await,
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert_no_recording_message(
        &mut publisher_rx,
        "permission-denied recording start must not notify the requester",
    )
    .await;
    assert_no_recording_message(
        &mut observer_rx,
        "permission-denied recording start must not notify observers",
    )
    .await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted, 0);
    assert_eq!(metrics_snapshot.recording_start_rejected, 1);
    assert_eq!(metrics_snapshot.active_recording_rooms, 0);
}

#[tokio::test]
async fn recording_start_rejects_requests_for_disabled_features() {
    for (feature_name, feature_flags, options) in [
        (
            "audio",
            RuntimeFeatureFlags {
                transcription: true,
                audio_recording: false,
                video_recording: true,
            },
            RecordingOptions {
                audio: Some(true),
                video: Some(false),
                transcription: Some(false),
            },
        ),
        (
            "video",
            RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: false,
            },
            RecordingOptions {
                audio: Some(false),
                video: Some(true),
                transcription: Some(false),
            },
        ),
        (
            "transcription",
            RuntimeFeatureFlags {
                transcription: false,
                audio_recording: true,
                video_recording: true,
            },
            RecordingOptions {
                audio: Some(false),
                video: Some(false),
                transcription: Some(true),
            },
        ),
    ] {
        let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room_with(
            feature_flags,
            Some("https://record.example.com"),
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
                .start_recording(&UserId::Integer(1), options)
                .await,
            "{feature_name} recording should stay disabled at runtime"
        );
        assert_no_recording_message(
            &mut publisher_rx,
            "disabled-feature recording start must not notify the requester",
        )
        .await;
        assert_no_recording_message(
            &mut observer_rx,
            "disabled-feature recording start must not notify observers",
        )
        .await;

        let metrics_snapshot = metrics.snapshot();
        assert_eq!(metrics_snapshot.recording_start_accepted, 0);
        assert_eq!(metrics_snapshot.recording_start_rejected, 1);
        assert_eq!(metrics_snapshot.active_recording_rooms, 0);
    }
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
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    assert_eq!(
        room.recording_state().await,
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
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
    assert_eq!(metrics_snapshot.recording_start_accepted, 0);
    assert_eq!(metrics_snapshot.recording_start_rejected, 1);
    assert_eq!(metrics_snapshot.active_recording_rooms, 0);
}

#[tokio::test]
async fn recording_stop_rejects_users_without_stop_authority() {
    let (room, metrics, mut publisher_rx, mut observer_rx) = build_recording_room().await;

    assert!(
        room.test_api()
            .lifecycle()
            .start_recording(
                &UserId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    let _publisher_start = expect_recording_message(&mut publisher_rx).await;
    let _observer_start = expect_recording_message(&mut observer_rx).await;

    assert!(
        !room
            .test_api()
            .lifecycle()
            .stop_recording(&UserId::Integer(2))
            .await
    );
    assert_eq!(
        room.recording_state().await,
        RecordingState {
            recording: Some(true),
            audio: Some(true),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert_no_recording_message(
        &mut publisher_rx,
        "unauthorized recording stop must not notify the active recorder",
    )
    .await;
    assert_no_recording_message(
        &mut observer_rx,
        "unauthorized recording stop must not notify observers",
    )
    .await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_accepted, 1);
    assert_eq!(metrics_snapshot.recording_stop_accepted, 0);
    assert_eq!(metrics_snapshot.recording_stop_rejected, 1);
    assert_eq!(metrics_snapshot.active_recording_rooms, 1);
}
