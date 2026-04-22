use anyhow::Result;

use super::parsing::parse_optional_env;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeFeatureFlags {
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}

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
