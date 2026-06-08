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
#[path = "TESTS/feature_flags.rs"]
mod tests;
