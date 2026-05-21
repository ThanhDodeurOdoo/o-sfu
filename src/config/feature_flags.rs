use anyhow::Result;
pub use o_sfu_core::prelude::RuntimeFeatureFlags;

use super::{log_view::ConfigLogField, parsing::parse_optional_env};

pub(super) fn load_runtime_feature_flags(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<RuntimeFeatureFlags> {
    Ok(RuntimeFeatureFlags {
        transcription: parse_optional_env(
            &mut get_var,
            "FEATURE_TRANSCRIPTION",
            "FEATURE_TRANSCRIPTION must be either `true` or `false`",
        )?
        .unwrap_or(false),
        audio_recording: parse_optional_env(
            &mut get_var,
            "FEATURE_AUDIO_RECORDING",
            "FEATURE_AUDIO_RECORDING must be either `true` or `false`",
        )?
        .unwrap_or(false),
        video_recording: parse_optional_env(
            &mut get_var,
            "FEATURE_VIDEO_RECORDING",
            "FEATURE_VIDEO_RECORDING must be either `true` or `false`",
        )?
        .unwrap_or(false),
    })
}

#[must_use]
pub(super) fn runtime_feature_flag_log_fields(flags: RuntimeFeatureFlags) -> [ConfigLogField; 3] {
    [
        ConfigLogField::new("transcription", flags.transcription),
        ConfigLogField::new("audio_recording", flags.audio_recording),
        ConfigLogField::new("video_recording", flags.video_recording),
    ]
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
        let config = load_runtime_feature_flags(|key| match key {
            "FEATURE_TRANSCRIPTION" => Some("enabled".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }
}
