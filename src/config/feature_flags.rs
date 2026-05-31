use anyhow::Result;
pub use o_sfu_core::prelude::RuntimeFeatureFlags;

use super::env::env_block;

env_block! {
    struct FeatureEnv {
        transcription: bool = default("FEATURE_TRANSCRIPTION", false);
        audio_recording: bool = default("FEATURE_AUDIO_RECORDING", false);
        video_recording: bool = default("FEATURE_VIDEO_RECORDING", false);
    }
}

pub(super) fn load_runtime_feature_flags(
    get_var: impl FnMut(&str) -> Option<String>,
) -> Result<RuntimeFeatureFlags> {
    let env = FeatureEnv::load(get_var)?;
    Ok(RuntimeFeatureFlags {
        transcription: env.transcription,
        audio_recording: env.audio_recording,
        video_recording: env.video_recording,
    })
}

#[cfg(test)]
mod tests {
    use super::{RuntimeFeatureFlags, load_runtime_feature_flags};

    #[test]
    fn load_runtime_feature_flags_defaults_to_all_disabled() {
        let config = load_runtime_feature_flags(|_| None);
        assert_eq!(config.ok(), Some(RuntimeFeatureFlags::default()));
    }

    #[test]
    fn load_runtime_feature_flags_accepts_explicit_flags() {
        let config = load_runtime_feature_flags(|key| match key {
            "FEATURE_TRANSCRIPTION" | "FEATURE_AUDIO_RECORDING" | "FEATURE_VIDEO_RECORDING" => {
                Some("true".to_owned())
            }
            _ => None,
        });
        assert_eq!(
            config.ok(),
            Some(RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: true,
            })
        );
    }

    #[test]
    fn load_runtime_feature_flags_rejects_invalid_bool() {
        let error = load_runtime_feature_flags(|key| match key {
            "FEATURE_TRANSCRIPTION" => Some("enabled".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("FEATURE_TRANSCRIPTION must be either `true` or `false`")
        );
    }
}
