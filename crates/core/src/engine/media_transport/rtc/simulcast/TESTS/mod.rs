use o_sfu_rfc::{rtp as rfc_rtp, rtp::h264::PacketizationMode, webrtc};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{CodecSetting, MediaFormat, PayloadType, StreamBinding},
};
use str0m::media::{MediaKind, Rid as Str0mRid};

use super::*;
use crate::Bitrate;

const ANSWER_HIGH_MAX_BITRATE: Bitrate = Bitrate::from_kbps(900);

#[test]
fn answer_send_rid_projection_preserves_declared_bitrate() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={rid_attr}:hi {send} {max_br}=900000\r\n",
            "a={simulcast_attr}:{send} lo{separator}hi\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Ok(vec![
            NegotiatedRid {
                rid: Str0mRid::from(common::DEFAULT_LOW_RID),
                max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
            },
            NegotiatedRid {
                rid: Str0mRid::from(common::DEFAULT_HIGH_RID),
                max_bitrate: Some(ANSWER_HIGH_MAX_BITRATE),
            },
        ])
    );
}

#[test]
fn answer_send_rid_projection_keeps_only_accepted_simulcast_rids() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={rid_attr}:hi {send} {max_br}=900000\r\n",
            "a={simulcast_attr}:{send} lo\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Ok(vec![NegotiatedRid {
            rid: Str0mRid::from(common::DEFAULT_LOW_RID),
            max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
        }])
    );
}

#[test]
fn answer_send_rid_projection_preserves_simulcast_order() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:hi {send} {max_br}=900000\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={simulcast_attr}:{send} lo{separator}hi\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Ok(vec![
            NegotiatedRid {
                rid: Str0mRid::from(common::DEFAULT_LOW_RID),
                max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
            },
            NegotiatedRid {
                rid: Str0mRid::from(common::DEFAULT_HIGH_RID),
                max_bitrate: Some(ANSWER_HIGH_MAX_BITRATE),
            },
        ])
    );
}

#[test]
fn answer_send_rid_projection_keeps_offered_bitrate_when_answer_omits_it() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send}\r\n",
            "a={rid_attr}:hi {send}\r\n",
            "a={simulcast_attr}:{send} lo{separator}hi\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Ok(vec![
            NegotiatedRid {
                rid: Str0mRid::from(common::DEFAULT_LOW_RID),
                max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
            },
            NegotiatedRid {
                rid: Str0mRid::from(common::DEFAULT_HIGH_RID),
                max_bitrate: Some(ANSWER_HIGH_MAX_BITRATE),
            },
        ])
    );
}

#[test]
fn answer_send_rid_projection_rejects_max_bitrate_when_offer_has_none() {
    let answer = single_rid_answer("lo", Some("max-br=150000"));

    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("video_0"),
            &custom_upload_encodings("lo", None),
        ),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_send_rid_projection_accepts_lower_max_bitrate_than_offer() {
    let answer = single_rid_answer("lo", Some("max-br=149999"));

    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("video_0"),
            &custom_upload_encodings("lo", Some(150_000)),
        ),
        Ok(vec![NegotiatedRid {
            rid: Str0mRid::from("lo"),
            max_bitrate: Some(Bitrate::from_bps(149_999)),
        }])
    );
}

#[test]
fn answer_send_rid_projection_rejects_malformed_max_bitrate() {
    let answer = single_rid_answer("lo", Some("max-br=bad"));

    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("video_0"),
            &custom_upload_encodings("lo", Some(150_000)),
        ),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_send_rid_projection_rejects_valueless_max_bitrate() {
    let answer = single_rid_answer("lo", Some("max-br"));

    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("video_0"),
            &custom_upload_encodings("lo", Some(150_000)),
        ),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_send_rid_projection_rejects_duplicate_rid_declaration() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={rid_attr}:lo {send} {max_br}=149999\r\n",
            "a={simulcast_attr}:{send} lo\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
    );

    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("video_0"),
            &custom_upload_encodings("lo", Some(150_000)),
        ),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_send_rid_projection_rejects_unmodeled_restrictions() {
    for restrictions in ["max-br=150000;max-width=640", "pt=96;max-br=150000"] {
        let answer = single_rid_answer("lo", Some(restrictions));

        assert_eq!(
            send_rids_for_mid(
                &answer,
                Mid::from("video_0"),
                &custom_upload_encodings("lo", Some(150_000)),
            ),
            Err(SimulcastAnswerError)
        );
    }
}

#[test]
fn answer_send_rid_projection_requires_accepted_simulcast_send_list() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={rid_attr}:hi {send} {max_br}=900000\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
    );

    assert!(matches!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Ok(rids) if rids.is_empty()
    ));
}

#[test]
fn answer_send_rid_projection_rejects_simulcast_alternatives() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={rid_attr}:backup {send} {max_br}=450000\r\n",
            "a={rid_attr}:hi {send} {max_br}=900000\r\n",
            "a={simulcast_attr}:{send} {pause}lo{alternative}backup{separator}hi\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
        pause = webrtc::sdp::simulcast::INITIAL_PAUSE_PREFIX,
        alternative = webrtc::sdp::simulcast::ALTERNATIVE_SEPARATOR,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_send_rid_projection_matches_exact_mid_section() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "a=mid:cam\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:camera\r\n",
            "a={rid_attr}:wrong {send} {max_br}=111000\r\n",
            "a={simulcast_attr}:{send} wrong\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:cam\r\n",
            "a={rid_attr}:right {send} {max_br}=222000\r\n",
            "a={simulcast_attr}:{send} right\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
    );

    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("cam"),
            &custom_upload_encodings("right", Some(222_000))
        ),
        Ok(vec![NegotiatedRid {
            rid: Str0mRid::from("right"),
            max_bitrate: Some(Bitrate::from_bps(222_000)),
        }])
    );
    assert_eq!(
        send_rids_for_mid(
            &answer,
            Mid::from("camera"),
            &custom_upload_encodings("wrong", Some(111_000)),
        ),
        Ok(vec![NegotiatedRid {
            rid: Str0mRid::from("wrong"),
            max_bitrate: Some(Bitrate::from_bps(111_000)),
        }])
    );
}

#[test]
fn answer_send_rid_projection_rejects_invalid_rfc8852_ids() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:low-1 {send} {max_br}=150000\r\n",
            "a={rid_attr}:hi_2 {send} {max_br}=450000\r\n",
            "a={rid_attr}:hi2 {send} {max_br}=900000\r\n",
            "a={simulcast_attr}:{send} low-1{separator}hi_2{separator}hi2\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_send_rid_projection_rejects_extra_simulcast_streams() {
    let answer = format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:lo {send} {max_br}=150000\r\n",
            "a={rid_attr}:mid {send} {max_br}=450000\r\n",
            "a={rid_attr}:hi {send} {max_br}=900000\r\n",
            "a={simulcast_attr}:{send} lo{separator}mid{separator}hi\r\n"
        ),
        rid_attr = webrtc::sdp::attribute::RID,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
        max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
        separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
    );

    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("video_0"), &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn h264_and_vp8_profiles_are_promoted_simulcast_publication_paths() {
    let h264 = h264_parameters(PacketizationMode::NonInterleaved, "42e01f");

    assert!(publish_recv_simulcast(MediaKind::Video, &h264).is_some());
    assert_eq!(
        publish_upload_encodings(MediaKind::Video, &h264),
        vec![
            SessionUploadEncoding {
                rid: common::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: common::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
            },
        ]
    );

    let vp8 = vp8_parameters();

    assert!(publish_recv_simulcast(MediaKind::Video, &vp8).is_some());
    assert_eq!(
        publish_upload_encodings(MediaKind::Video, &vp8),
        vec![
            SessionUploadEncoding {
                rid: common::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: Some(4),
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: common::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: Some(1),
                max_framerate: None,
            },
        ]
    );
}

#[test]
fn h264_profile_accepts_only_the_promoted_chromium_matrix() {
    let parameters = h264_parameters(PacketizationMode::NonInterleaved, "42E01F");
    let profile = SimulcastCodecProfile::publish(
        MediaKind::Video,
        &parameters,
        VideoBitrateLimits::default(),
    );
    assert!(
        matches!(profile, Some(SimulcastCodecProfile::H264(_))),
        "H264 parameters should select the H264 interop profile"
    );
    let Some(SimulcastCodecProfile::H264(profile)) = profile else {
        return;
    };

    assert_eq!(
        profile.default_layers(),
        common::default_layer_specs(VideoBitrateLimits::default())
    );
    for parameters in [
        h264_parameters(PacketizationMode::SingleNalUnit, "42e01f"),
        h264_parameters(PacketizationMode::NonInterleaved, "42001f"),
        h264_parameters(PacketizationMode::NonInterleaved, "4d001f"),
        h264_parameters(PacketizationMode::NonInterleaved, "4de01f"),
    ] {
        assert!(
            publish_upload_encodings(MediaKind::Video, &parameters).is_empty(),
            "unsupported H264 variants must remain single-encoding"
        );
    }
}

fn vp8_parameters() -> RouterRtpParameters {
    video_parameters(MediaFormat::new(
        RouterMediaKind::Video,
        rfc_rtp::CodecName::Vp8,
        PayloadType::new(96),
        90_000,
    ))
}

fn h264_parameters(
    packetization_mode: PacketizationMode,
    profile_level_id: &str,
) -> RouterRtpParameters {
    video_parameters(
        MediaFormat::new(
            RouterMediaKind::Video,
            rfc_rtp::CodecName::H264,
            PayloadType::new(102),
            90_000,
        )
        .with_setting(CodecSetting::H264PacketizationMode(packetization_mode))
        .with_setting(CodecSetting::H264ProfileLevelId(
            profile_level_id.to_owned(),
        )),
    )
}

fn video_parameters(format: MediaFormat) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![format],
        Vec::new(),
        vec![
            StreamBinding::new().with_rid(common::DEFAULT_LOW_RID),
            StreamBinding::new().with_rid(common::DEFAULT_HIGH_RID),
        ],
    )
}

fn single_rid_answer(rid: &str, restrictions: Option<&str>) -> String {
    let restriction = restrictions.map_or(String::new(), |restriction| format!(" {restriction}"));
    format!(
        concat!(
            "v=0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:video_0\r\n",
            "a={rid_attr}:{rid} {send}{restriction}\r\n",
            "a={simulcast_attr}:{send} {rid}\r\n"
        ),
        rid = rid,
        rid_attr = webrtc::sdp::attribute::RID,
        restriction = restriction,
        simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
        send = webrtc::sdp::rid::DIRECTION_SEND,
    )
}

fn default_upload_encodings() -> Vec<SessionUploadEncoding> {
    vec![
        SessionUploadEncoding {
            rid: common::DEFAULT_LOW_RID.to_owned(),
            max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
            resolution_scale: Some(4),
            max_framerate: None,
        },
        SessionUploadEncoding {
            rid: common::DEFAULT_HIGH_RID.to_owned(),
            max_bitrate: Some(ANSWER_HIGH_MAX_BITRATE),
            resolution_scale: Some(1),
            max_framerate: None,
        },
    ]
}

fn custom_upload_encodings(rid: &str, max_bitrate: Option<u64>) -> Vec<SessionUploadEncoding> {
    vec![SessionUploadEncoding {
        rid: rid.to_owned(),
        max_bitrate: max_bitrate.map(Bitrate::from_bps),
        resolution_scale: None,
        max_framerate: None,
    }]
}
