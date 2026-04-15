use super::fixtures::*;
use crate::config::{MediaCodecFlags, RuntimeFeatureFlags};
use crate::runtime::{
    channel::{ChannelManagerConfig, ChannelRuntimePolicy, rtp_capabilities},
    metrics::RuntimeMetrics,
    recording::MediaTap,
};
use crate::signaling::{
    protocol::RecordingOptions,
    shared::{RecordingState, RecordingStateUpdate, StopCode},
};

async fn build_recording_channel() -> (
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
                RuntimeFeatureFlags {
                    transcription: true,
                    audio_recording: true,
                    video_recording: true,
                },
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
                recording_address: Some("https://record.example.com".to_owned()),
                ..ChannelConfig::default()
            },
            None,
        )
        .await;
    let (tx1, rx1) = test_sender();
    let (tx2, rx2) = test_sender();
    channel
        .join_session(
            SessionId::Integer(1),
            None,
            SessionPermissions {
                transcription: Some(true),
                audio_recording: Some(true),
                video_recording: Some(true),
            },
            tx1,
        )
        .await
        .expect("recording publisher should join");
    channel
        .join_session(
            SessionId::Integer(2),
            None,
            SessionPermissions::default(),
            tx2,
        )
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

#[tokio::test]
async fn recording_start_and_stop_update_channel_state_for_all_sessions() {
    let (channel, metrics, mut publisher_rx, mut observer_rx) = build_recording_channel().await;

    assert!(
        channel
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

    assert!(channel.stop_recording(&SessionId::Integer(1)).await);
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
