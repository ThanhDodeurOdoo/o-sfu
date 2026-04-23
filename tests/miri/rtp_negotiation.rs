//! Miri is use here i because RTP negotiation and RFC helpers are pure byte and
//! data-shaping logic with explicit payload remapping, fmtp parsing, and header
//! extension selection. These tests act as cross-target regression check for
//! endian-sensitive assumptions and keep the negotiation edge cases checked under the
//! interpreter without touching the live WebRTC stack.

use o_sfu_rfc::{
    rtp::{
        fmtp,
        h264::{LevelIdc, Profile, ProfileLevelId},
        header_extension,
    },
    webrtc,
};
use o_sfu_router::{
    HeaderExtension, MediaCapabilities, MediaCodecCapability, MediaFormat, MediaKind, MediaStream,
    RtcpFeedback, RtcpFeedbackKind, RtpNegotiationError, StreamBinding,
    derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};

#[test]
fn h264_profile_level_id_and_two_byte_profile_helpers_handle_edge_cases() {
    let parsed = ProfileLevelId::parse("42500b");
    assert_eq!(
        parsed.map(ProfileLevelId::profile),
        Some(Profile::ConstrainedBaseline)
    );
    assert_eq!(parsed.map(ProfileLevelId::level), Some(LevelIdc::Level1B));

    let malformed_tokens = ["", "42e0", "gggggg", "42e01g"];
    for token in malformed_tokens {
        assert_eq!(ProfileLevelId::parse(token), None);
    }

    assert_eq!(header_extension::two_byte_profile_id(0), Some(0x1000));
    assert_eq!(header_extension::two_byte_profile_id(0x0F), Some(0x100F));
    assert_eq!(header_extension::two_byte_profile_id(0x10), None);
}

#[test]
fn derive_consumable_parameters_remap_primary_payloads_rtx_apt_and_bindings() {
    let router_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
                .with_preferred_payload_type(101)
                .with_parameter(fmtp::H264_PACKETIZATION_MODE, "1")
                .with_parameter(fmtp::H264_PROFILE_LEVEL_ID, "4d0032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(102)
                .with_parameter(fmtp::RTX_ASSOCIATION, "101"),
        ],
        vec![
            HeaderExtension::new(webrtc::rtp_header_extension_uri::MID, 1),
            HeaderExtension::new(
                webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
                5,
            ),
        ],
    );
    let producer_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "H264", 111, 90_000)
                .with_parameter(fmtp::H264_PACKETIZATION_MODE, "1")
                .with_parameter(fmtp::H264_PROFILE_LEVEL_ID, "4d0032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 112, 90_000)
                .with_parameter(fmtp::RTX_ASSOCIATION, "111"),
        ],
        vec![
            HeaderExtension::new(webrtc::rtp_header_extension_uri::MID, 1),
            HeaderExtension::new(webrtc::rtp_header_extension_uri::ABS_SEND_TIME, 4),
        ],
        vec![
            StreamBinding::new()
                .with_rid("f")
                .with_ssrc(1_234)
                .with_codec_payload_type(111),
        ],
    )
    .with_mid("video-0");

    let consumable = derive_consumable_rtp_parameters(&producer_parameters, &router_capabilities);
    assert_eq!(consumable.as_ref().map(|_| ()), Ok(()));
    let Some(consumable) = consumable.ok() else {
        return;
    };

    let codecs = consumable.codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 2);
    assert_eq!(
        codecs.first().map(|format| format.payload_type()),
        Some(101)
    );
    assert_eq!(codecs.get(1).map(|format| format.payload_type()), Some(102));
    assert_eq!(
        codecs.get(1).and_then(|format| {
            format
                .parameters()
                .find_map(|(name, value)| (name == fmtp::RTX_ASSOCIATION).then_some(value))
        }),
        Some("101".to_owned())
    );
    assert_eq!(consumable.mid(), Some("video-0"));
    assert_eq!(
        consumable
            .header_extensions()
            .map(HeaderExtension::uri)
            .collect::<Vec<_>>(),
        vec![webrtc::rtp_header_extension_uri::MID]
    );
    assert_eq!(
        consumable
            .bindings()
            .next()
            .and_then(StreamBinding::payload_type),
        Some(101)
    );
}

#[test]
fn derive_consumable_parameters_reject_invalid_rtx_apt_values() {
    let router_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_preferred_payload_type(100),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(101)
                .with_parameter(fmtp::RTX_ASSOCIATION, "100"),
        ],
        vec![],
    );
    let producer_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000),
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000)
                .with_parameter(fmtp::RTX_ASSOCIATION, "bad"),
        ],
        vec![],
        vec![],
    );

    assert_eq!(
        derive_consumable_rtp_parameters(&producer_parameters, &router_capabilities),
        Err(RtpNegotiationError::InvalidAptParameter {
            codec_name: "rtx".to_owned(),
            payload_type: 97,
        })
    );
}

#[test]
fn consumer_negotiation_prunes_transport_cc_when_only_abs_send_time_survives() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000)
                .with_parameter(fmtp::RTX_ASSOCIATION, "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        )],
        vec![StreamBinding::new().with_ssrc(5_678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_preferred_payload_type(100)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(101)
                .with_parameter(fmtp::RTX_ASSOCIATION, "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        )],
    );

    let negotiated =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(negotiated.as_ref().map(|_| ()), Ok(()));
    let Some(negotiated) = negotiated.ok() else {
        return;
    };

    let first_codec_feedback = negotiated
        .codecs()
        .next()
        .map(|format| {
            format
                .rtcp_feedback()
                .map(RtcpFeedback::kind)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert_eq!(
        first_codec_feedback,
        vec![&RtcpFeedbackKind::NackPli, &RtcpFeedbackKind::GoogRemb]
    );
    assert_eq!(
        negotiated
            .header_extensions()
            .map(HeaderExtension::uri)
            .collect::<Vec<_>>(),
        vec![webrtc::rtp_header_extension_uri::ABS_SEND_TIME]
    );
}

#[test]
fn consumer_negotiation_treats_missing_vp9_profile_id_as_profile_zero_only() {
    let profile_zero_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", 98, 90_000)
                .with_parameter(fmtp::VP9_PROFILE_ID, "0")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5_678)],
    );
    let profile_two_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", 98, 90_000)
                .with_parameter(fmtp::VP9_PROFILE_ID, "2")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5_678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
    );

    assert!(
        negotiate_consumer_rtp_parameters(&profile_zero_parameters, &consumer_capabilities,)
            .is_ok()
    );
    assert_eq!(
        negotiate_consumer_rtp_parameters(&profile_two_parameters, &consumer_capabilities),
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
}

#[test]
fn consumer_negotiation_filters_rtx_bindings_when_apt_does_not_match() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000)
                .with_parameter(fmtp::RTX_ASSOCIATION, "96"),
        ],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(5_678)
                .with_codec_payload_type(96),
            StreamBinding::new()
                .with_ssrc(5_679)
                .with_codec_payload_type(97),
            StreamBinding::new().with_rid("fallback"),
        ],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_parameter(fmtp::RTX_ASSOCIATION, "120"),
        ],
        vec![],
    );

    let negotiated =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(negotiated.as_ref().map(|_| ()), Ok(()));
    let Some(negotiated) = negotiated.ok() else {
        return;
    };

    assert_eq!(
        negotiated
            .codecs()
            .map(MediaFormat::codec_name)
            .collect::<Vec<_>>(),
        vec!["VP8"]
    );
    let bindings = negotiated.bindings().collect::<Vec<_>>();
    assert_eq!(bindings.len(), 2);
    assert_eq!(
        bindings.first().and_then(|binding| binding.payload_type()),
        Some(96)
    );
    assert_eq!(
        bindings.get(1).and_then(|binding| binding.rid()),
        Some("fallback")
    );
}
