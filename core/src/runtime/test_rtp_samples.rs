use o_sfu_rfc::{rtp, webrtc};
use o_sfu_router::{HeaderExtension, MediaFormat, MediaKind, MediaStream, StreamBinding};

const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;
const HEADER_EXTENSION_ID_MID: u8 = 1;

pub(crate) fn sample_simulcast_video_rtp_parameters(mid: Option<&str>) -> MediaStream {
    with_optional_mid(
        MediaStream::new(
            vec![video_codec_parameters()],
            vec![HeaderExtension::new(
                webrtc::RtpHeaderExtensionUri::Mid,
                HEADER_EXTENSION_ID_MID,
            )],
            vec![
                StreamBinding::new()
                    .with_ssrc(31_001)
                    .with_rid("lo")
                    .with_max_bitrate(150_000),
                StreamBinding::new()
                    .with_ssrc(31_002)
                    .with_rid("hi")
                    .with_max_bitrate(900_000),
            ],
        ),
        mid,
    )
}

fn with_optional_mid(parameters: MediaStream, mid: Option<&str>) -> MediaStream {
    match mid {
        Some(mid) => parameters.with_mid(mid.to_owned()),
        None => parameters,
    }
}

fn video_codec_parameters() -> MediaFormat {
    MediaFormat::new(
        MediaKind::Video,
        rtp::CodecName::Vp8,
        VIDEO_PAYLOAD_TYPE_VP8,
        90_000,
    )
}
