use std::iter;

use o_sfu_rfc::rtp::{self, h264::PacketizationMode};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{CodecSetting, MediaFormat, MediaStream, PayloadType, StreamBinding},
};
use str0m::media::MediaKind;

use super::*;

#[test]
fn publication_profile_follows_first_primary_codec() {
    let h264 = h264_parameters();
    assert!(publish_recv_simulcast(MediaKind::Video, &h264).is_some());
    assert_eq!(
        publish_upload_encodings(MediaKind::Video, &h264),
        vec![
            SessionUploadEncoding {
                rid: rid::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: rid::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
            },
        ]
    );

    let vp8 = vp8_parameters();
    let h264_then_vp8 = video_parameters(h264.formats().chain(vp8.formats()).cloned());
    assert_eq!(
        publish_upload_encodings(MediaKind::Video, &h264_then_vp8),
        publish_upload_encodings(MediaKind::Video, &h264)
    );

    for codec in [
        rtp::CodecName::Av1,
        rtp::CodecName::Vp9,
        rtp::CodecName::H265,
    ] {
        let parameters = video_parameters(
            iter::once(MediaFormat::new(
                RouterMediaKind::Video,
                codec,
                PayloadType::new(98),
                90_000,
            ))
            .chain(vp8.formats().cloned()),
        );
        assert!(publish_recv_simulcast(MediaKind::Video, &parameters).is_none());
        assert!(publish_upload_encodings(MediaKind::Video, &parameters).is_empty());
    }

    assert!(publish_recv_simulcast(MediaKind::Video, &vp8).is_some());
    assert_eq!(
        publish_upload_encodings(MediaKind::Video, &vp8),
        vec![
            SessionUploadEncoding {
                rid: rid::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: Some(4),
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: rid::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: None,
                resolution_scale: Some(1),
                max_framerate: None,
            },
        ]
    );
}

fn vp8_parameters() -> MediaStream {
    video_parameters([MediaFormat::new(
        RouterMediaKind::Video,
        rtp::CodecName::Vp8,
        PayloadType::new(96),
        90_000,
    )])
}

fn h264_parameters() -> MediaStream {
    video_parameters([MediaFormat::new(
        RouterMediaKind::Video,
        rtp::CodecName::H264,
        PayloadType::new(102),
        90_000,
    )
    .with_setting(CodecSetting::H264PacketizationMode(
        PacketizationMode::NonInterleaved,
    ))
    .with_setting(CodecSetting::H264ProfileLevelId("42e01f".to_owned()))])
}

fn video_parameters(formats: impl IntoIterator<Item = MediaFormat>) -> MediaStream {
    MediaStream::new(
        formats.into_iter().collect(),
        Vec::new(),
        vec![
            StreamBinding::new().with_rid(rid::DEFAULT_LOW_RID),
            StreamBinding::new().with_rid(rid::DEFAULT_HIGH_RID),
        ],
    )
}
