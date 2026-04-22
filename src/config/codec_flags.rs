use anyhow::Result;
use bitflags::bitflags;

use super::parsing::parse_optional_env;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct MediaCodecSet: u16 {
        const OPUS = 1 << 0;
        const PCMU = 1 << 1;
        const PCMA = 1 << 2;
        const VP8 = 1 << 3;
        const H264 = 1 << 4;
        const H265 = 1 << 5;
        const VP9 = 1 << 6;
        const AV1 = 1 << 7;
    }
}

#[derive(Debug, Clone, Copy)]
struct CodecEnvSpec {
    flag: MediaCodecSet,
    key: &'static str,
    error_message: &'static str,
}

const CODEC_ENV_SPECS: [CodecEnvSpec; 8] = [
    CodecEnvSpec {
        flag: MediaCodecSet::OPUS,
        key: "CODEC_OPUS",
        error_message: "CODEC_OPUS must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::PCMU,
        key: "CODEC_PCMU",
        error_message: "CODEC_PCMU must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::PCMA,
        key: "CODEC_PCMA",
        error_message: "CODEC_PCMA must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::VP8,
        key: "CODEC_VP8",
        error_message: "CODEC_VP8 must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::H264,
        key: "CODEC_H264",
        error_message: "CODEC_H264 must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::H265,
        key: "CODEC_H265",
        error_message: "CODEC_H265 must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::VP9,
        key: "CODEC_VP9",
        error_message: "CODEC_VP9 must be either `true` or `false`",
    },
    CodecEnvSpec {
        flag: MediaCodecSet::AV1,
        key: "CODEC_AV1",
        error_message: "CODEC_AV1 must be either `true` or `false`",
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCodecFlags {
    enabled: MediaCodecSet,
}

macro_rules! media_codec_accessors {
    ($($enabled:ident => $with:ident => $flag:ident),+ $(,)?) => {
        $(
            #[must_use]
            pub fn $enabled(self) -> bool {
                self.enabled.contains(MediaCodecSet::$flag)
            }

            #[must_use]
            pub fn $with(self, enabled: bool) -> Self {
                self.with_flag(MediaCodecSet::$flag, enabled)
            }
        )+
    };
}

impl MediaCodecFlags {
    #[must_use]
    fn with_flag(mut self, flag: MediaCodecSet, enabled: bool) -> Self {
        if enabled {
            self.enabled.insert(flag);
        } else {
            self.enabled.remove(flag);
        }
        self
    }

    media_codec_accessors!(
        opus_enabled => with_opus => OPUS,
        pcmu_enabled => with_pcmu => PCMU,
        pcma_enabled => with_pcma => PCMA,
        vp8_enabled => with_vp8 => VP8,
        h264_enabled => with_h264 => H264,
        h265_enabled => with_h265 => H265,
        vp9_enabled => with_vp9 => VP9,
        av1_enabled => with_av1 => AV1,
    );
}

impl Default for MediaCodecFlags {
    fn default() -> Self {
        Self {
            enabled: MediaCodecSet::OPUS | MediaCodecSet::VP8,
        }
    }
}

pub(super) fn load_media_codec_flags(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<MediaCodecFlags> {
    let mut flags = MediaCodecFlags::default();
    for spec in CODEC_ENV_SPECS {
        if let Some(enabled) = parse_optional_env(&mut get_var, spec.key, spec.error_message)? {
            flags = flags.with_flag(spec.flag, enabled);
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
