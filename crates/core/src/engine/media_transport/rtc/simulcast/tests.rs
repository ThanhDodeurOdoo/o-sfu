use o_sfu_rfc::{rtp as rfc_rtp, webrtc};
use o_sfu_router::{CodecSetting, MediaFormat, MediaKind as RouterMediaKind, StreamBinding};
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
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
        send_rids_for_mid(&answer, Mid::from("cam")),
        Ok(vec![NegotiatedRid {
            rid: Str0mRid::from("right"),
            max_bitrate: Some(Bitrate::from_bps(222_000)),
        }])
    );
    assert_eq!(
        send_rids_for_mid(&answer, Mid::from("camera")),
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
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
        send_rids_for_mid(&answer, Mid::from("video_0")),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn h264_and_vp8_profiles_are_promoted_simulcast_publication_paths() {
    let h264 = h264_parameters(1, "42e01f");

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
    let parameters = h264_parameters(1, "42E01F");
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
        h264_parameters(0, "42e01f"),
        h264_parameters(1, "42001f"),
        h264_parameters(1, "4d001f"),
        h264_parameters(1, "4de01f"),
    ] {
        assert!(
            publish_upload_encodings(MediaKind::Video, &parameters).is_empty(),
            "unsupported H264 variants must remain single-encoding"
        );
    }
}

#[test]
fn h264_only_bootstrap_gets_default_simulcast_metadata() {
    let codec_flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let encodings =
        bootstrap_upload_encodings(MediaKind::Video, codec_flags, VideoBitrateLimits::default());
    assert!(
        bootstrap_recv_simulcast(MediaKind::Video, codec_flags, VideoBitrateLimits::default())
            .is_some()
    );
    assert_eq!(
        encodings,
        vec![
            SessionUploadEncoding {
                rid: common::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: common::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: Some(VideoBitrateLimits::default().max_video_bitrate()),
                resolution_scale: None,
                max_framerate: None,
            },
        ]
    );
}

fn vp8_parameters() -> RouterRtpParameters {
    video_parameters(MediaFormat::new(
        RouterMediaKind::Video,
        rfc_rtp::CodecName::Vp8,
        96,
        90_000,
    ))
}

fn h264_parameters(packetization_mode: u8, profile_level_id: &str) -> RouterRtpParameters {
    video_parameters(
        MediaFormat::new(
            RouterMediaKind::Video,
            rfc_rtp::CodecName::H264,
            102,
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
