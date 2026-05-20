use anyhow::Result;
use o_sfu_core::prelude::MediaCodecFlags;

use super::parsing::parse_optional_env;

#[derive(Debug, Clone, Copy)]
struct CodecEnvSpec {
    key: &'static str,
    error_message: &'static str,
    apply: fn(MediaCodecFlags, bool) -> MediaCodecFlags,
}

const CODEC_ENV_SPECS: [CodecEnvSpec; 8] = [
    CodecEnvSpec {
        key: "CODEC_OPUS",
        error_message: "CODEC_OPUS must be either `true` or `false`",
        apply: MediaCodecFlags::with_opus,
    },
    CodecEnvSpec {
        key: "CODEC_PCMU",
        error_message: "CODEC_PCMU must be either `true` or `false`",
        apply: MediaCodecFlags::with_pcmu,
    },
    CodecEnvSpec {
        key: "CODEC_PCMA",
        error_message: "CODEC_PCMA must be either `true` or `false`",
        apply: MediaCodecFlags::with_pcma,
    },
    CodecEnvSpec {
        key: "CODEC_VP8",
        error_message: "CODEC_VP8 must be either `true` or `false`",
        apply: MediaCodecFlags::with_vp8,
    },
    CodecEnvSpec {
        key: "CODEC_H264",
        error_message: "CODEC_H264 must be either `true` or `false`",
        apply: MediaCodecFlags::with_h264,
    },
    CodecEnvSpec {
        key: "CODEC_H265",
        error_message: "CODEC_H265 must be either `true` or `false`",
        apply: MediaCodecFlags::with_h265,
    },
    CodecEnvSpec {
        key: "CODEC_VP9",
        error_message: "CODEC_VP9 must be either `true` or `false`",
        apply: MediaCodecFlags::with_vp9,
    },
    CodecEnvSpec {
        key: "CODEC_AV1",
        error_message: "CODEC_AV1 must be either `true` or `false`",
        apply: MediaCodecFlags::with_av1,
    },
];

pub(super) fn load_media_codec_flags(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<MediaCodecFlags> {
    let mut flags = MediaCodecFlags::default();
    for spec in CODEC_ENV_SPECS {
        if let Some(enabled) = parse_optional_env(&mut get_var, spec.key, spec.error_message)? {
            flags = (spec.apply)(flags, enabled);
        }
    }
    Ok(flags)
}

#[cfg(test)]
mod tests {
    use super::{MediaCodecFlags, load_media_codec_flags};

    #[test]
    fn load_media_codec_flags_defaults_to_opus_and_vp8() {
        assert_eq!(
            load_media_codec_flags(|_| None).ok(),
            Some(MediaCodecFlags::default())
        );
    }

    #[test]
    fn load_media_codec_flags_applies_per_codec_overrides() {
        let flags = load_media_codec_flags(|key| match key {
            "CODEC_OPUS" => Some("false".to_owned()),
            "CODEC_H264" | "CODEC_AV1" => Some("true".to_owned()),
            _ => None,
        });
        assert_eq!(
            flags.ok(),
            Some(
                MediaCodecFlags::default()
                    .with_opus(false)
                    .with_h264(true)
                    .with_av1(true),
            )
        );
    }

    #[test]
    fn load_media_codec_flags_rejects_invalid_bool() {
        let flags = load_media_codec_flags(|key| match key {
            "CODEC_VP8" => Some("enabled".to_owned()),
            _ => None,
        });
        assert!(flags.is_err());
    }
}
