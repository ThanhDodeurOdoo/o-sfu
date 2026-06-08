use anyhow::Result;
use o_sfu_core::prelude::MediaCodecFlags;

use super::env::env_block;

env_block! {
    struct CodecFlagsEnv {
        opus: bool = default("CODEC_OPUS", MediaCodecFlags::default().opus_enabled());
        pcmu: bool = default("CODEC_PCMU", MediaCodecFlags::default().pcmu_enabled());
        pcma: bool = default("CODEC_PCMA", MediaCodecFlags::default().pcma_enabled());
        vp8: bool = default("CODEC_VP8", MediaCodecFlags::default().vp8_enabled());
        h264: bool = default("CODEC_H264", MediaCodecFlags::default().h264_enabled());
        h265: bool = default("CODEC_H265", MediaCodecFlags::default().h265_enabled());
        vp9: bool = default("CODEC_VP9", MediaCodecFlags::default().vp9_enabled());
        av1: bool = default("CODEC_AV1", MediaCodecFlags::default().av1_enabled());
    }
}

pub(super) fn load_media_codec_flags(
    get_var: impl FnMut(&str) -> Option<String>,
) -> Result<MediaCodecFlags> {
    let env = CodecFlagsEnv::load(get_var)?;
    Ok(MediaCodecFlags::default()
        .with_opus(env.opus)
        .with_pcmu(env.pcmu)
        .with_pcma(env.pcma)
        .with_vp8(env.vp8)
        .with_h264(env.h264)
        .with_h265(env.h265)
        .with_vp9(env.vp9)
        .with_av1(env.av1))
}

#[cfg(test)]
#[path = "TESTS/codec_flags.rs"]
mod tests;
