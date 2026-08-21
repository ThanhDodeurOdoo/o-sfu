use std::collections::BTreeSet;

use o_sfu_rfc::rtp as rfc_rtp;
use o_sfu_router::{
    MediaKind,
    rtp::{CodecSetting, RtcpFeedbackKind},
};

use super::client_rtp_capabilities_from_answer;

const CHROMIUM_OPTIONAL_CODECS_ANSWER: &str =
    include_str!("testdata/chromium_optional_codecs_answer.sdp");

#[test]
fn chromium_answer_projection_keeps_video_repair_pairs() {
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
        .filter(|codec| codec.codec_name() == rfc_rtp::codec_name::H264)
        .map(|codec| {
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
        .filter(|codec| codec.codec_name() == rfc_rtp::codec_name::VP9)
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

    let codecs = projected.codecs().collect::<Vec<_>>();
    for primary in codecs
        .iter()
        .copied()
        .filter(|codec| codec.media_kind() == MediaKind::Video && !codec.codec().is_rtx())
    {
        assert!(
            primary
                .rtcp_feedback()
                .any(|feedback| { feedback.kind() == &RtcpFeedbackKind::Nack })
        );
        assert!(
            primary
                .rtcp_feedback()
                .any(|feedback| { feedback.kind() == &RtcpFeedbackKind::NackPli })
        );
        assert_eq!(
            codecs
                .iter()
                .filter(|codec| {
                    codec.codec().is_rtx()
                        && codec.rtx_associated_payload_type() == primary.payload_type()
                })
                .count(),
            1
        );
    }
    assert!(
        codecs
            .iter()
            .filter(|codec| codec.codec().is_rtx())
            .all(|codec| codec.media_kind() == MediaKind::Video
                && codec.rtcp_feedback().next().is_none())
    );
    assert!(
        codecs
            .iter()
            .filter(|codec| codec.media_kind() == MediaKind::Audio)
            .all(|codec| codec.rtx_associated_payload_type().is_none()
                && codec
                    .rtcp_feedback()
                    .all(|feedback| feedback.kind() != &RtcpFeedbackKind::Nack))
    );
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

#[test]
fn answer_projection_rejects_non_one_byte_extmap_ids() {
    for id in [15, 16, 255] {
        let invalid_answer = CHROMIUM_OPTIONAL_CODECS_ANSWER.replace(
            "a=extmap:13 urn:3gpp:video-orientation",
            &format!("a=extmap:{id} urn:3gpp:video-orientation"),
        );

        assert!(
            client_rtp_capabilities_from_answer(&invalid_answer).is_none(),
            "answer extmap id {id} must fail at the SDP edge"
        );
    }
}
