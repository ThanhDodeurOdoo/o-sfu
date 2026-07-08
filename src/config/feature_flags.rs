use anyhow::Result;
pub use o_sfu_core::prelude::RuntimeFeatureFlags;

use super::env::Env;

pub(super) fn load_runtime_feature_flags(env: &Env<'_>) -> Result<RuntimeFeatureFlags> {
    Ok(RuntimeFeatureFlags {
        transcription: env.var("FEATURE_TRANSCRIPTION").default(false)?,
        audio_recording: env.var("FEATURE_AUDIO_RECORDING").default(false)?,
        video_recording: env.var("FEATURE_VIDEO_RECORDING").default(false)?,
    })
}

#[cfg(test)]
#[path = "TESTS/feature_flags.rs"]
mod tests;
