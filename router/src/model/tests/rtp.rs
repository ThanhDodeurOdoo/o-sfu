use crate::rfc::webrtc;
use crate::{
    MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCapabilities, RtpCodecCapability,
    RtpCodecParameters, RtpEncoding, RtpHeaderExtension, RtpParameters,
};

#[test]
fn codec_capability_builder_keeps_optional_fields() {
    let capability = RtpCodecCapability::new(MediaKind::Audio, "opus", 48_000)
        .with_preferred_payload_type(111)
        .with_channels(2)
        .with_parameter("useinbandfec", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));

    assert_eq!(capability.media_kind(), MediaKind::Audio);
    assert_eq!(capability.codec_name(), "opus");
    assert_eq!(capability.clock_rate(), 48_000);
    assert_eq!(capability.preferred_payload_type(), Some(111));
    assert_eq!(capability.channels(), Some(2));
    assert_eq!(
        capability.parameters().collect::<Vec<_>>(),
        vec![("useinbandfec", "1")]
    );
    assert_eq!(capability.rtcp_feedback().count(), 1);
}

#[test]
fn header_extensions_and_capabilities_expose_entries() {
    let capabilities = RtpCapabilities::new(
        vec![RtpCodecCapability::new(MediaKind::Video, "VP8", 90_000)],
        vec![RtpHeaderExtension::new(
            webrtc::rtp_header_extension_uri::MID,
            1,
        )],
    );

    assert_eq!(
        capabilities
            .codecs()
            .map(RtpCodecCapability::codec_name)
            .collect::<Vec<_>>(),
        vec!["VP8"]
    );
    assert_eq!(
        capabilities
            .header_extensions()
            .map(|header| (header.uri(), header.id(), header.encrypt()))
            .collect::<Vec<_>>(),
        vec![(webrtc::rtp_header_extension_uri::MID, 1, false)]
    );
}

#[test]
fn rtp_parameters_collect_codec_header_and_encoding_data() {
    let codec = RtpCodecParameters::new(MediaKind::Video, "H264", 102, 90_000)
        .with_parameter("packetization-mode", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    let header = RtpHeaderExtension::new(
        "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
        4,
    )
    .with_encryption(true);
    let encoding = RtpEncoding::new()
        .with_rid("f")
        .with_ssrc(12_345)
        .with_codec_payload_type(102)
        .with_max_bitrate(1_500_000);

    let parameters =
        RtpParameters::new(vec![codec], vec![header], vec![encoding]).with_mid("video-0");

    assert_eq!(parameters.mid(), Some("video-0"));
    assert_eq!(parameters.codecs().count(), 1);
    assert_eq!(parameters.header_extensions().count(), 1);
    assert_eq!(parameters.encodings().count(), 1);
}
