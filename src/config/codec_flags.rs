use anyhow::Result;
use o_sfu_core::prelude::MediaCodecFlags;

use super::env::Env;

pub(super) fn load_media_codec_flags(env: &Env<'_>) -> Result<MediaCodecFlags> {
    let defaults = MediaCodecFlags::default();
    Ok(defaults
        .with_opus(env.var("CODEC_OPUS").default(defaults.opus_enabled())?)
        .with_pcmu(env.var("CODEC_PCMU").default(defaults.pcmu_enabled())?)
        .with_pcma(env.var("CODEC_PCMA").default(defaults.pcma_enabled())?)
        .with_vp8(env.var("CODEC_VP8").default(defaults.vp8_enabled())?)
        .with_h264(env.var("CODEC_H264").default(defaults.h264_enabled())?)
        .with_h265(env.var("CODEC_H265").default(defaults.h265_enabled())?)
        .with_vp9(env.var("CODEC_VP9").default(defaults.vp9_enabled())?)
        .with_av1(env.var("CODEC_AV1").default(defaults.av1_enabled())?))
}

#[cfg(test)]
#[path = "TESTS/codec_flags.rs"]
mod tests;
