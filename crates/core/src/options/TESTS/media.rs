#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test assertions use unwrap and expect for clear failure messages"
)]

use super::{Bitrate, VideoAdaptationTuning, VideoAdaptationTuningError};

#[test]
fn video_adaptation_tuning_accepts_valid_knobs() {
    let tuning = VideoAdaptationTuning::try_new(4, 3, 1, 2, 10, Bitrate::from_kbps(32))
        .expect("valid tuning should build");
    assert_eq!(tuning.multiparty_scalable_video_threshold, 4);
    assert_eq!(tuning.thumbnail_budget_divisor, 3);
    assert_eq!(tuning.downswitch_pressure_observations, 1);
    assert_eq!(tuning.upswitch_stable_observations, 2);
    assert_eq!(tuning.receiver_budget_headroom_percent, 10);
    assert_eq!(tuning.audio_reserve_per_speaker, Bitrate::from_kbps(32));
}

#[test]
fn video_adaptation_tuning_rejects_invalid_knobs() {
    let cases = [
        (
            VideoAdaptationTuning::try_new(0, 2, 2, 3, 0, Bitrate::zero()),
            VideoAdaptationTuningError::MultipartyScalableVideoThresholdZero,
        ),
        (
            VideoAdaptationTuning::try_new(3, 0, 2, 3, 0, Bitrate::zero()),
            VideoAdaptationTuningError::ThumbnailBudgetDivisorZero,
        ),
        (
            VideoAdaptationTuning::try_new(3, 2, 0, 3, 0, Bitrate::zero()),
            VideoAdaptationTuningError::DownswitchPressureObservationsZero,
        ),
        (
            VideoAdaptationTuning::try_new(3, 2, 2, 0, 0, Bitrate::zero()),
            VideoAdaptationTuningError::UpswitchStableObservationsZero,
        ),
        (
            VideoAdaptationTuning::try_new(3, 2, 2, 3, 101, Bitrate::zero()),
            VideoAdaptationTuningError::ReceiverBudgetHeadroomPercentTooHigh,
        ),
    ];
    for (result, expected) in cases {
        assert_eq!(result.err(), Some(expected));
    }
}
