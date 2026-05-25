use o_sfu_rfc::webrtc;

use crate::{
    HeaderExtension, HeaderExtensionId, MediaCapabilities, MediaCodecCapability, MediaFormat,
    MediaKind, MediaStream, ParseDiagnostic, ParseDiagnosticKind, PayloadType, Rid, RtcpFeedback,
    RtcpFeedbackKind, RtpNegotiationError, StreamBinding, can_consume,
    derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};

#[test]
fn rtp_identifiers_validate_rfc_ranges() {
    assert_eq!(PayloadType::try_new(63).map(PayloadType::value), Some(63));
    assert_eq!(PayloadType::try_new(64), None);
    assert_eq!(PayloadType::try_new(95), None);
    assert_eq!(PayloadType::try_new(96).map(PayloadType::value), Some(96));
    assert_eq!(PayloadType::try_new(128), None);

    assert_eq!(
        HeaderExtensionId::try_new(1).map(HeaderExtensionId::value),
        Some(1)
    );
    assert_eq!(
        HeaderExtensionId::try_new(14).map(HeaderExtensionId::value),
        Some(14)
    );
    assert_eq!(HeaderExtensionId::try_new(0), None);
    assert_eq!(HeaderExtensionId::try_new(15), None);

    assert_eq!(Rid::try_new("hi").as_ref().map(Rid::as_str), Some("hi"));
    assert_eq!(Rid::try_new(""), None);
    assert_eq!(Rid::try_new("bad-rid"), None);
}

#[test]
fn invalid_rtx_apt_does_not_become_a_payload_type() {
    let capability =
        MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000).with_parameter("apt", "64");

    assert_eq!(capability.rtx_associated_payload_type(), None);
    assert_eq!(
        capability.parameters().collect::<Vec<_>>(),
        vec![("apt".to_owned(), "64".to_owned())]
    );
}

#[test]
fn codec_capability_builder_keeps_optional_fields() {
    let capability = MediaCodecCapability::new(MediaKind::Audio, "opus", 48_000)
        .with_preferred_payload_type(111)
        .with_channels(2)
        .with_parameter("useinbandfec", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));

    assert_eq!(capability.media_kind(), MediaKind::Audio);
    assert_eq!(capability.codec_name(), "opus");
    assert_eq!(capability.clock_rate(), 48_000);
    assert_eq!(capability.payload_type(), Some(111));
    assert_eq!(capability.channels(), Some(2));
    assert_eq!(
        capability.parameters().collect::<Vec<_>>(),
        vec![("useinbandfec".to_owned(), "1".to_owned())]
    );
    assert_eq!(capability.rtcp_feedback().count(), 1);
}

#[test]
fn header_extensions_and_capabilities_expose_entries() {
    let capabilities = MediaCapabilities::new(
        vec![MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::MID,
            1,
        )],
    );

    assert_eq!(
        capabilities
            .codecs()
            .map(MediaCodecCapability::codec_name)
            .collect::<Vec<_>>(),
        vec!["VP8"]
    );
    assert_eq!(
        capabilities
            .header_extensions()
            .map(|header| (header.uri(), header.id().value(), header.encrypt()))
            .collect::<Vec<_>>(),
        vec![(webrtc::rtp_header_extension_uri::MID, 1, false)]
    );
}

#[test]
fn rtp_parameters_collect_codec_header_and_encoding_data() {
    let codec = MediaFormat::new(MediaKind::Video, "H264", 102, 90_000)
        .with_parameter("packetization-mode", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    let header = HeaderExtension::new(
        "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
        4,
    )
    .with_encryption(true);
    let encoding = StreamBinding::new()
        .with_rid("f")
        .with_ssrc(12_345)
        .with_codec_payload_type(102)
        .with_max_bitrate(1_500_000);

    let parameters =
        MediaStream::new(vec![codec], vec![header], vec![encoding]).with_mid("video-0");

    assert_eq!(parameters.mid(), Some("video-0"));
    assert_eq!(parameters.codecs().count(), 1);
    assert_eq!(parameters.header_extensions().count(), 1);
    assert_eq!(parameters.encodings().count(), 1);
}

#[test]
fn derive_consumable_parameters_maps_payload_types_and_rtx_association() {
    let capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
                .with_preferred_payload_type(101)
                .with_parameter("packetization-mode", "1")
                .with_parameter("profile-level-id", "4d0032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(102)
                .with_parameter("apt", "101"),
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
                .with_parameter("packetization-mode", "1")
                .with_parameter("profile-level-id", "4d0032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 112, 90_000).with_parameter("apt", "111"),
        ],
        vec![
            HeaderExtension::new(webrtc::rtp_header_extension_uri::MID, 1),
            HeaderExtension::new(webrtc::rtp_header_extension_uri::ABS_SEND_TIME, 4),
        ],
        vec![
            StreamBinding::new()
                .with_rid("f")
                .with_ssrc(1234)
                .with_codec_payload_type(111),
        ],
    )
    .with_mid("video-0");

    let consumable_result = derive_consumable_rtp_parameters(&producer_parameters, &capabilities);
    assert!(consumable_result.is_ok());
    let Ok(consumable) = consumable_result else {
        return;
    };
    let codecs = consumable.codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 2);
    let Some(first_codec) = codecs.first() else {
        return;
    };
    assert_eq!(first_codec.payload_type(), 101);
    let Some(second_codec) = codecs.get(1) else {
        return;
    };
    assert_eq!(second_codec.payload_type(), 102);
    assert_eq!(
        second_codec
            .parameters()
            .find_map(|(name, value)| (name == "apt").then_some(value)),
        Some("101".to_owned())
    );
    assert_eq!(consumable.mid(), Some("video-0"));
    let header_extension_uris = consumable
        .header_extensions()
        .map(HeaderExtension::uri)
        .collect::<Vec<_>>();
    assert_eq!(
        header_extension_uris,
        vec![webrtc::rtp_header_extension_uri::MID,]
    );
    let first_encoding = consumable.encodings().next();
    assert!(first_encoding.is_some());
    let Some(first_encoding) = first_encoding else {
        return;
    };
    assert_eq!(first_encoding.payload_type(), Some(101));
}

#[test]
fn consumer_negotiation_keeps_abs_send_time_and_filters_transport_cc_feedback() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000).with_parameter("apt", "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        )],
        vec![StreamBinding::new().with_ssrc(5678)],
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
                .with_parameter("apt", "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        )],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };
    let codecs = negotiated.codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 2);
    let first_codec_feedback = codecs
        .first()
        .map(|codec| {
            codec
                .rtcp_feedback()
                .map(RtcpFeedback::kind)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert!(first_codec_feedback.contains(&&RtcpFeedbackKind::NackPli));
    assert!(first_codec_feedback.contains(&&RtcpFeedbackKind::GoogRemb));
    assert!(!first_codec_feedback.contains(&&RtcpFeedbackKind::TransportCc));
    let header_extension_uris = negotiated
        .header_extensions()
        .map(HeaderExtension::uri)
        .collect::<Vec<_>>();
    assert_eq!(
        header_extension_uris,
        vec![webrtc::rtp_header_extension_uri::ABS_SEND_TIME]
    );
}

#[test]
fn consumer_negotiation_fails_when_no_media_codec_matches() {
    let consumable_parameters = MediaStream::new(
        vec![MediaFormat::new(MediaKind::Audio, "opus", 111, 48_000)],
        vec![],
        vec![],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_preferred_payload_type(96),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn invalid_rtx_apt_is_reported_as_invalid_diagnostic() {
    let capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_preferred_payload_type(100),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(101)
                .with_parameter("apt", "100"),
        ],
        vec![],
    );
    let producer_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000),
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000).with_parameter("apt", "bad"),
        ],
        vec![],
        vec![],
    );

    let negotiation = derive_consumable_rtp_parameters(&producer_parameters, &capabilities);
    assert_eq!(
        negotiation,
        Err(RtpNegotiationError::InvalidAptParameter {
            codec_name: "rtx".to_owned(),
            payload_type: 97,
        })
    );
    let Err(error) = negotiation else {
        return;
    };
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
    assert_eq!(diagnostic.rfc_reference().document(), "RFC 4588",);
    assert_eq!(diagnostic.rfc_reference().section(), "section 8.1");
}

#[test]
fn incompatible_consumer_codec_is_reported_as_unsupported_diagnostic() {
    let consumable_parameters = MediaStream::new(
        vec![MediaFormat::new(MediaKind::Audio, "opus", 111, 48_000)],
        vec![],
        vec![],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)],
        vec![],
    );

    let negotiation =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiation,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    let Err(error) = negotiation else {
        return;
    };
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
    assert_eq!(diagnostic.rfc_reference().document(), "RFC 3264");
    assert_eq!(diagnostic.rfc_reference().section(), "section 6");
}

#[test]
fn consumer_negotiation_rejects_vp9_profile_mismatch() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", 98, 90_000)
                .with_parameter("profile-id", "2")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 99, 90_000).with_parameter("apt", "98"),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                .with_preferred_payload_type(100)
                .with_parameter("profile-id", "0")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(101)
                .with_parameter("apt", "100"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_treats_missing_vp9_profile_id_as_profile_zero() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", 98, 90_000)
                .with_parameter("profile-id", "2")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_missing_vp9_profile_id_for_profile_zero() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", 98, 90_000)
                .with_parameter("profile-id", "0")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_h264_when_capability_level_is_higher() {
    let consumable_parameters = h264_consumable_parameters("42e01f");
    let consumer_capabilities = h264_consumer_capabilities("42e032");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_when_capability_level_is_lower() {
    let consumable_parameters = h264_consumable_parameters("42e032");
    let consumer_capabilities = h264_consumer_capabilities("42e01f");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_level_1_1_for_level_1_capability() {
    let consumable_parameters = h264_consumable_parameters("42e00b");
    let consumer_capabilities = h264_consumer_capabilities("42e00a");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_h264_level_1b_for_level_1_1_capability() {
    let consumable_parameters = h264_consumable_parameters("42500b");
    let consumer_capabilities = h264_consumer_capabilities("42e00b");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_high_profile_when_capability_omits_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters("64001f");
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_level_1_1_when_capability_omits_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters("42a00b");
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_h264_when_both_sides_omit_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters_without_profile_level_id();
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_malformed_profile_level_id_when_capability_omits_it() {
    let consumable_parameters = h264_consumable_parameters("42e000");
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_zero_level_idc() {
    let consumable_parameters = h264_consumable_parameters("42e000");
    let consumer_capabilities = h264_consumer_capabilities("42e00b");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_matching_malformed_h264_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters("42e000");
    let consumer_capabilities = h264_consumer_capabilities("42e000");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!can_consume(&consumable_parameters, &consumer_capabilities));
}

fn h264_consumable_parameters(profile_level_id: &str) -> MediaStream {
    h264_consumable_parameters_with_profile_level_id(Some(profile_level_id))
}

fn h264_consumable_parameters_without_profile_level_id() -> MediaStream {
    h264_consumable_parameters_with_profile_level_id(None)
}

fn h264_consumable_parameters_with_profile_level_id(profile_level_id: Option<&str>) -> MediaStream {
    let mut format = MediaFormat::new(MediaKind::Video, "H264", 98, 90_000)
        .with_parameter("packetization-mode", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    if let Some(profile_level_id) = profile_level_id {
        format = format.with_parameter("profile-level-id", profile_level_id);
    }
    MediaStream::new(
        vec![format],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    )
}

fn h264_consumer_capabilities(profile_level_id: &str) -> MediaCapabilities {
    h264_consumer_capabilities_with_profile_level_id(Some(profile_level_id))
}

fn h264_consumer_capabilities_without_profile_level_id() -> MediaCapabilities {
    h264_consumer_capabilities_with_profile_level_id(None)
}

fn h264_consumer_capabilities_with_profile_level_id(
    profile_level_id: Option<&str>,
) -> MediaCapabilities {
    let mut capability = MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
        .with_parameter("packetization-mode", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    if let Some(profile_level_id) = profile_level_id {
        capability = capability.with_parameter("profile-level-id", profile_level_id);
    }
    MediaCapabilities::new(vec![capability], vec![])
}

#[test]
fn consumer_negotiation_filters_rtx_bindings_when_consumer_apt_does_not_match() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000).with_parameter("apt", "96"),
        ],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(5678)
                .with_codec_payload_type(96),
            StreamBinding::new()
                .with_ssrc(5679)
                .with_codec_payload_type(97),
            StreamBinding::new().with_rid("fallback"),
        ],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000).with_parameter("apt", "120"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };

    let codecs = negotiated.codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 1);
    let Some(codec) = codecs.first() else {
        return;
    };
    assert_eq!(codec.codec_name(), "VP8");
    let encodings = negotiated.encodings().collect::<Vec<_>>();
    assert_eq!(encodings.len(), 2);
    let Some(first_encoding) = encodings.first() else {
        return;
    };
    assert_eq!(first_encoding.payload_type(), Some(96));
    let Some(second_encoding) = encodings.get(1) else {
        return;
    };
    assert_eq!(second_encoding.rid(), Some("fallback"));
}

#[test]
fn consumer_negotiation_accepts_media_when_rtx_is_listed_first() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "rtx", 97, 90_000).with_parameter("apt", "96"),
            MediaFormat::new(MediaKind::Video, "VP8", 96, 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(5678)
                .with_codec_payload_type(96),
        ],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000).with_parameter("apt", "96"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };

    let codecs = negotiated.codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 2);
    let Some(first_codec) = codecs.first() else {
        return;
    };
    assert_eq!(first_codec.codec_name(), "VP8");
    let Some(second_codec) = codecs.get(1) else {
        return;
    };
    assert_eq!(second_codec.codec_name(), "rtx");
    assert!(can_consume(&consumable_parameters, &consumer_capabilities));
}
