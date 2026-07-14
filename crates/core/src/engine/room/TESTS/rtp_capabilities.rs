use std::collections::BTreeSet;

use o_sfu_router::rtp::CodecSetting;

use super::{router_rtp_capabilities, router_rtp_capabilities_with_preferences};
use crate::{CodecPreferences, MediaCodecFlags, VideoCodecPreference};

#[test]
fn default_capabilities_match_the_browser_codec_baseline() {
    let capabilities = router_rtp_capabilities(MediaCodecFlags::default());
    let codec_names = capabilities
        .codecs()
        .map(|codec| codec.codec_name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(codec_names, vec!["opus", "VP8"]);
}

#[test]
fn capabilities_include_enabled_optional_codecs() {
    let capabilities = router_rtp_capabilities(
        MediaCodecFlags::default()
            .with_pcmu(true)
            .with_h264(true)
            .with_vp9(true),
    );
    let codec_names = capabilities
        .codecs()
        .map(|codec| codec.codec_name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        codec_names.get(..4),
        Some(
            &[
                String::from("opus"),
                String::from("PCMU"),
                String::from("VP8"),
                String::from("H264"),
            ][..]
        )
    );
    let h264_variants = capabilities
        .codecs()
        .filter(|codec| codec.codec_name() == "H264")
        .map(|codec| {
            let payload_type = codec.payload_type().unwrap_or(u8::MAX);
            let packetization_mode = codec
                .settings()
                .find_map(|setting| match setting {
                    CodecSetting::H264PacketizationMode(mode) => Some(mode.fmtp_value()),
                    _ => None,
                })
                .unwrap_or(u8::MAX);
            let profile_level_id = codec
                .settings()
                .find_map(|setting| match setting {
                    CodecSetting::H264ProfileLevelId(profile_level_id) => {
                        Some(profile_level_id.clone())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            (payload_type, packetization_mode, profile_level_id)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        h264_variants,
        BTreeSet::from([
            (35, 0, String::from("4d001f")),
            (108, 1, String::from("42e01f")),
            (114, 1, String::from("64001f")),
            (123, 1, String::from("4d001f")),
            (124, 0, String::from("42e01f")),
            (125, 0, String::from("42001f")),
            (127, 1, String::from("42001f")),
        ])
    );
    let vp9_profiles = capabilities
        .codecs()
        .filter(|codec| codec.codec_name() == "VP9")
        .map(|codec| {
            codec.settings().find_map(|setting| match setting {
                CodecSetting::Vp9ProfileId(profile_id) => Some(profile_id.value()),
                _ => None,
            })
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(vp9_profiles, BTreeSet::from([Some(0), Some(2)]));
    let rtx_associations = capabilities
        .codecs()
        .filter(|codec| codec.codec_name() == "rtx")
        .filter_map(|codec| {
            codec.settings().find_map(|setting| match setting {
                CodecSetting::RtxAssociation(payload_type) => Some(payload_type.value()),
                _ => None,
            })
        })
        .collect::<BTreeSet<_>>();
    assert!(!rtx_associations.contains(&96));
    assert!(!rtx_associations.contains(&35));
    assert!(!rtx_associations.contains(&108));
    assert!(!rtx_associations.contains(&114));
    assert!(!rtx_associations.contains(&123));
    assert!(!rtx_associations.contains(&124));
    assert!(!rtx_associations.contains(&125));
    assert!(!rtx_associations.contains(&127));
    assert!(rtx_associations.contains(&116));
    assert!(rtx_associations.contains(&118));
}

#[test]
fn capabilities_follow_configured_codec_preferences() {
    let capabilities = router_rtp_capabilities_with_preferences(
        MediaCodecFlags::default().with_h264(true).with_vp9(true),
        CodecPreferences::default()
            .with_video_order(&[VideoCodecPreference::H264, VideoCodecPreference::Vp9]),
    );
    let codec_names = capabilities
        .codecs()
        .map(|codec| codec.codec_name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        codec_names.get(..4),
        Some(
            &[
                String::from("opus"),
                String::from("H264"),
                String::from("H264"),
                String::from("H264"),
            ][..]
        )
    );
}
