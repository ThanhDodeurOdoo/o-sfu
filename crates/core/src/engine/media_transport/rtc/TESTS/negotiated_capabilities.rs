use std::collections::BTreeSet;

use o_sfu_rfc::rtp as rfc_rtp;

use super::client_rtp_capabilities_from_answer;

const CHROMIUM_OPTIONAL_CODECS_ANSWER: &str =
    include_str!("testdata/chromium_optional_codecs_answer.sdp");

#[test]
fn chromium_answer_projection_keeps_optional_video_profiles_and_rtx_pairs() {
    let projected = client_rtp_capabilities_from_answer(CHROMIUM_OPTIONAL_CODECS_ANSWER);
    assert!(
        projected.is_some(),
        "captured Chromium answer should project into client RTP capabilities"
    );
    let Some(projected) = projected else {
        return;
    };

    let h264_variants = projected
        .codecs()
        .filter(|codec| codec.codec_name() == "H264")
        .map(|codec| {
            let packetization_mode = codec
                .settings()
                .find_map(|setting| match setting {
                    o_sfu_router::CodecSetting::H264PacketizationMode(mode) => {
                        Some(mode.fmtp_value())
                    }
                    _ => None,
                })
                .unwrap_or(u8::MAX);
            let profile_level_id = codec
                .settings()
                .find_map(|setting| match setting {
                    o_sfu_router::CodecSetting::H264ProfileLevelId(profile_level_id) => {
                        Some(profile_level_id.clone())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            (packetization_mode, profile_level_id)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        h264_variants,
        BTreeSet::from([
            (0, String::from("42001f")),
            (0, String::from("42e01f")),
            (0, String::from("4d001f")),
            (1, String::from("42001f")),
            (1, String::from("42e01f")),
            (1, String::from("4d001f")),
        ])
    );

    let vp9_profiles = projected
        .codecs()
        .filter(|codec| codec.codec_name() == "VP9")
        .map(|codec| {
            codec
                .parameters()
                .find_map(|(key, value)| (key == rfc_rtp::fmtp::VP9_PROFILE_ID).then_some(value))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        vp9_profiles,
        BTreeSet::from([Some(String::from("0")), Some(String::from("2"))])
    );

    let optional_payload_types = projected
        .codecs()
        .filter(|codec| matches!(codec.codec_name(), "H264" | "VP9"))
        .filter_map(o_sfu_router::MediaCodecCapability::payload_type)
        .collect::<BTreeSet<_>>();
    let rtx_associations = projected
        .codecs()
        .filter(|codec| codec.codec_name() == "rtx")
        .filter_map(|codec| {
            codec.parameters().find_map(|(key, value)| {
                if key != rfc_rtp::fmtp::RTX_ASSOCIATION {
                    return None;
                }
                value.parse::<u8>().ok()
            })
        })
        .collect::<BTreeSet<_>>();
    assert!(optional_payload_types.is_subset(&rtx_associations));
}

#[test]
fn answer_projection_rejects_rtcp_mux_forbidden_payload_types() {
    let invalid_answer = CHROMIUM_OPTIONAL_CODECS_ANSWER
        .replace(
            "m=video 9 UDP/TLS/RTP/SAVPF 96",
            "m=video 9 UDP/TLS/RTP/SAVPF 72",
        )
        .replace("a=rtpmap:96 VP8/90000", "a=rtpmap:72 VP8/90000")
        .replace("a=rtcp-fb:96", "a=rtcp-fb:72")
        .replace("a=fmtp:97 apt=96", "a=fmtp:97 apt=72");

    assert!(client_rtp_capabilities_from_answer(&invalid_answer).is_none());
}
