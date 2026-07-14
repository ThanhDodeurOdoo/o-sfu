use super::effective_feature_flags;
use crate::config::RuntimeFeatureFlags;

#[test]
fn effective_feature_flags_disable_transcription_without_recording() {
    assert_eq!(
        effective_feature_flags(RuntimeFeatureFlags {
            transcription: true,
            audio_recording: false,
            video_recording: false,
        }),
        RuntimeFeatureFlags {
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );
    assert_eq!(
        effective_feature_flags(RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: false,
        }),
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: false,
        }
    );
}
