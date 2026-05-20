use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{AudioCodecPreference, CodecPreferences, VideoCodecPreference};

pub(super) fn load_codec_preferences(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<CodecPreferences> {
    let audio = match get_var("CODEC_AUDIO_PREFERENCE") {
        Some(value) => parse_audio_preferences(&value)?,
        None => Vec::new(),
    };
    let video = match get_var("CODEC_VIDEO_PREFERENCE") {
        Some(value) => parse_video_preferences(&value)?,
        None => Vec::new(),
    };
    Ok(CodecPreferences::default()
        .with_audio_order(&audio)
        .with_video_order(&video))
}

fn parse_audio_preferences(value: &str) -> Result<Vec<AudioCodecPreference>> {
    parse_codec_list(value, "CODEC_AUDIO_PREFERENCE", audio_codec_preference)
}

fn parse_video_preferences(value: &str) -> Result<Vec<VideoCodecPreference>> {
    parse_codec_list(value, "CODEC_VIDEO_PREFERENCE", video_codec_preference)
}

fn parse_codec_list<T>(
    value: &str,
    env_key: &str,
    parse_codec: impl Fn(&str) -> Option<T>,
) -> Result<Vec<T>>
where
    T: Copy + PartialEq,
{
    let mut codecs = Vec::new();
    for raw_codec in value.split(',') {
        let codec_name = raw_codec.trim();
        ensure!(
            !codec_name.is_empty(),
            "{env_key} cannot contain empty entries"
        );
        let codec = parse_codec(codec_name)
            .ok_or_else(|| anyhow!("{env_key} contains unsupported codec `{codec_name}`"))?;
        ensure!(
            !codecs.contains(&codec),
            "{env_key} cannot contain duplicate codec `{codec_name}`"
        );
        codecs.push(codec);
    }
    ensure!(!codecs.is_empty(), "{env_key} cannot be empty");
    Ok(codecs)
}

fn audio_codec_preference(codec_name: &str) -> Option<AudioCodecPreference> {
    match codec_name.to_ascii_lowercase().as_str() {
        "opus" => Some(AudioCodecPreference::Opus),
        "pcmu" => Some(AudioCodecPreference::Pcmu),
        "pcma" => Some(AudioCodecPreference::Pcma),
        _ => None,
    }
}

fn video_codec_preference(codec_name: &str) -> Option<VideoCodecPreference> {
    match codec_name.to_ascii_lowercase().as_str() {
        "vp8" => Some(VideoCodecPreference::Vp8),
        "h264" => Some(VideoCodecPreference::H264),
        "h265" => Some(VideoCodecPreference::H265),
        "vp9" => Some(VideoCodecPreference::Vp9),
        "av1" => Some(VideoCodecPreference::Av1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_core::prelude::{AudioCodecPreference, CodecPreferences, VideoCodecPreference};

    use super::load_codec_preferences;

    #[test]
    fn load_codec_preferences_defaults_to_canonical_order() {
        assert_eq!(
            load_codec_preferences(|_| None).ok(),
            Some(CodecPreferences::default())
        );
    }

    #[test]
    fn load_codec_preferences_accepts_partial_orders() {
        let preferences = load_codec_preferences(|key| match key {
            "CODEC_AUDIO_PREFERENCE" => Some("PCMU,opus".to_owned()),
            "CODEC_VIDEO_PREFERENCE" => Some("H264,VP9".to_owned()),
            _ => None,
        });
        assert_eq!(
            preferences.ok(),
            Some(
                CodecPreferences::default()
                    .with_audio_order(&[AudioCodecPreference::Pcmu, AudioCodecPreference::Opus])
                    .with_video_order(&[VideoCodecPreference::H264, VideoCodecPreference::Vp9]),
            )
        );
    }

    #[test]
    fn load_codec_preferences_rejects_unknown_codecs() {
        let preferences = load_codec_preferences(|key| match key {
            "CODEC_VIDEO_PREFERENCE" => Some("VP8,THEORA".to_owned()),
            _ => None,
        });
        assert!(preferences.is_err());
    }

    #[test]
    fn load_codec_preferences_rejects_duplicates() {
        let preferences = load_codec_preferences(|key| match key {
            "CODEC_AUDIO_PREFERENCE" => Some("opus,OPUS".to_owned()),
            _ => None,
        });
        assert!(preferences.is_err());
    }
}
