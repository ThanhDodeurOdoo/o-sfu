use super::fixtures::*;
use crate::config::{MediaCodecFlags, RuntimeFeatureFlags};
use crate::runtime::{
    channel::{ChannelManagerConfig, ChannelRuntimePolicy, rtp_capabilities},
    metrics::RuntimeMetrics,
    recording::MediaTap,
};
use o_sfu_protocol::{
    shared::{RecordingState, RecordingStateUpdate, StopCode},
    signaling::RecordingOptions,
};

async fn build_recording_channel() -> (
    Arc<super::super::Channel>,
    Arc<RuntimeMetrics>,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    build_recording_channel_with(
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
        Some("https://record.example.com"),
        SessionPermissions {
            transcription: Some(true),
            audio_recording: Some(true),
            video_recording: Some(true),
        },
        SessionPermissions::default(),
    )
    .await
}

async fn build_recording_channel_with(
    feature_flags: RuntimeFeatureFlags,
    recording_address: Option<&str>,
    publisher_permissions: SessionPermissions,
    observer_permissions: SessionPermissions,
) -> (
    Arc<super::super::Channel>,
    Arc<RuntimeMetrics>,
    mpsc::UnboundedReceiver<SessionOutbound>,
    mpsc::UnboundedReceiver<SessionOutbound>,
) {
    let metrics = Arc::new(RuntimeMetrics::default());
    let manager = ChannelManager::new(
        ChannelManagerConfig::new(
            1,
            ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(100),
                feature_flags,
                rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::clone(&metrics),
    );
    let channel = manager
        .create_or_get(
            "issuer-recording",
            None,
            &ChannelConfig {
                recording_address: recording_address.map(str::to_owned),
                ..ChannelConfig::default()
            },
            None,
        )
        .await;
    let (tx1, rx1) = test_sender();
    let (tx2, rx2) = test_sender();
    channel
        .test_api()
        .lifecycle()
        .join_session(SessionId::Integer(1), None, publisher_permissions, tx1)
        .await
        .expect("recording publisher should join");
    channel
        .test_api()
        .lifecycle()
        .join_session(SessionId::Integer(2), None, observer_permissions, tx2)
        .await
        .expect("recording observer should join");
    (channel, metrics, rx1, rx2)
}

async fn expect_recording_message(
    receiver: &mut mpsc::UnboundedReceiver<SessionOutbound>,
) -> RecordingStateUpdate {
    let outbound = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("recording update should arrive before timeout")
        .expect("recording update channel should stay open");
    let SessionOutbound::Message(ChannelEventMessage::RecordingStateChanged(update)) = outbound
    else {
        panic!("expected channel recording update, got {outbound:?}");
    };
    update
}

async fn assert_no_recording_message(
    receiver: &mut mpsc::UnboundedReceiver<SessionOutbound>,
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
async fn recording_start_and_stop_update_channel_state_for_all_sessions() {
    let (channel, metrics, mut publisher_rx, mut observer_rx) = build_recording_channel().await;

    assert!(
        channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(true),
                },
            )
            .await
    );
    assert_eq!(
        channel.recording_state().await,
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
    assert_eq!(metrics_snapshot.active_recording_channels, 1);

    assert!(
        channel
            .test_api()
            .lifecycle()
            .stop_recording(&SessionId::Integer(1))
            .await
    );
    assert_eq!(
        channel.recording_state().await,
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
    assert_eq!(metrics_snapshot.active_recording_channels, 0);
}

#[tokio::test]
async fn recording_allows_transcription_toggle_but_rejects_new_media_while_active() {
    let (channel, metrics, mut publisher_rx, _observer_rx) = build_recording_channel().await;

    assert!(
        channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
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
        channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
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
        !channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
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
    assert_eq!(metrics_snapshot.active_recording_channels, 1);
}

#[tokio::test]
async fn stale_replaced_connection_cannot_start_or_stop_recording() {
    let (channel, metrics, _publisher_rx, mut observer_rx) = build_recording_channel().await;
    let stale_connection_id = channel
        .test_api()
        .inspect()
        .session_connection_id(&SessionId::Integer(1))
        .await
        .expect("recording publisher should have a connection id");
    let (replacement_tx, mut replacement_rx) = test_sender();
    let replacement_connection_id = channel
        .test_api()
        .lifecycle()
        .join_session(
            SessionId::Integer(1),
            Some(String::from("replacement")),
            SessionPermissions {
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
        !channel
            .start_recording_runtime(
                &SessionId::Integer(1),
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
        channel.recording_state().await,
        RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert!(
        replacement_rx.try_recv().is_err(),
        "stale recording start must not fan out channel updates"
    );
    assert!(
        observer_rx.try_recv().is_err(),
        "stale recording start must not notify observers"
    );

    assert!(
        channel
            .start_recording_runtime(
                &SessionId::Integer(1),
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
        !channel
            .stop_recording_runtime(&SessionId::Integer(1), stale_connection_id)
            .await
    );
    assert_eq!(channel.recording_state().await.recording, Some(true));
    assert!(
        replacement_rx.try_recv().is_err(),
        "stale recording stop must not fan out channel updates"
    );
    assert!(
        observer_rx.try_recv().is_err(),
        "stale recording stop must not notify observers"
    );

    assert!(
        channel
            .stop_recording_runtime(&SessionId::Integer(1), replacement_connection_id)
            .await
    );
    let _stop_update = expect_recording_message(&mut replacement_rx).await;
    let _observer_stop = expect_recording_message(&mut observer_rx).await;

    let metrics_snapshot = metrics.snapshot();
    assert_eq!(metrics_snapshot.recording_start_rejected, 1);
    assert_eq!(metrics_snapshot.recording_stop_rejected, 1);
}

#[tokio::test]
async fn recording_start_rejects_sessions_without_recording_permissions() {
    let (channel, metrics, mut publisher_rx, mut observer_rx) = build_recording_channel_with(
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
        Some("https://record.example.com"),
        SessionPermissions::default(),
        SessionPermissions::default(),
    )
    .await;

    assert!(
        !channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    assert_eq!(
        channel.recording_state().await,
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
    assert_eq!(metrics_snapshot.active_recording_channels, 0);
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
        let (channel, metrics, mut publisher_rx, mut observer_rx) = build_recording_channel_with(
            feature_flags,
            Some("https://record.example.com"),
            SessionPermissions {
                transcription: Some(true),
                audio_recording: Some(true),
                video_recording: Some(true),
            },
            SessionPermissions::default(),
        )
        .await;

        assert!(
            !channel
                .test_api()
                .lifecycle()
                .start_recording(&SessionId::Integer(1), options)
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
        assert_eq!(metrics_snapshot.active_recording_channels, 0);
    }
}

#[tokio::test]
async fn recording_start_rejects_channels_without_recording_address() {
    let (channel, metrics, mut publisher_rx, mut observer_rx) = build_recording_channel_with(
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        },
        None,
        SessionPermissions {
            transcription: Some(true),
            audio_recording: Some(true),
            video_recording: Some(true),
        },
        SessionPermissions::default(),
    )
    .await;

    assert!(
        !channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
                RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: Some(false),
                },
            )
            .await
    );
    assert_eq!(
        channel.recording_state().await,
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
    assert_eq!(metrics_snapshot.active_recording_channels, 0);
}

#[tokio::test]
async fn recording_stop_rejects_sessions_without_stop_authority() {
    let (channel, metrics, mut publisher_rx, mut observer_rx) = build_recording_channel().await;

    assert!(
        channel
            .test_api()
            .lifecycle()
            .start_recording(
                &SessionId::Integer(1),
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
        !channel
            .test_api()
            .lifecycle()
            .stop_recording(&SessionId::Integer(2))
            .await
    );
    assert_eq!(
        channel.recording_state().await,
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
    assert_eq!(metrics_snapshot.active_recording_channels, 1);
}
