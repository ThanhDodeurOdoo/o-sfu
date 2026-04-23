use std::collections::BTreeSet;

use o_sfu_rfc::webrtc;
use o_sfu_router::{
    CodecSetting, HeaderExtension, HeaderExtensionUri, MediaCapabilities, MediaCodecCapability,
    MediaFormat, MediaKind, MediaStream, PayloadType, RtcpFeedback, RtcpFeedbackKind,
    StreamBinding, derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};
use o_sfu_router::{RtpNegotiationError, can_consume};

// Proves that successful consumable-parameter derivation never leaves behind a
// stale media link: surviving primaries must come from router capabilities,
// RTX must still point at a surviving primary PT, and payload-bound stream
// bindings must either be rewritten to a live PT or dropped. This is high
// value because stale PT bugs here would quietly poison every later routing
// decision.
#[kani::proof]
fn derive_consumable_rtp_parameters_keeps_only_valid_primary_links() {
    let scenario = derive_scenario(kani::any::<u8>());
    let result = derive_consumable_rtp_parameters(
        &scenario.producer_parameters,
        &scenario.router_capabilities,
    );

    if let Ok(consumable) = result {
        let primary_payloads = consumable
            .formats()
            .filter(|format| format.codec_name() != "rtx")
            .map(|format| format.payload_type())
            .collect::<BTreeSet<_>>();
        assert!(!primary_payloads.is_empty());

        for format in consumable
            .formats()
            .filter(|format| format.codec_name() != "rtx")
        {
            assert!(scenario.router_capabilities.codecs().any(|capability| {
                capability.codec_name() == format.codec_name()
                    && capability.media_kind() == format.media_kind()
            }));
        }

        for format in consumable
            .formats()
            .filter(|format| format.codec_name() == "rtx")
        {
            let apt = format.parameters().find_map(|(key, value)| {
                (key == "apt").then(|| value.parse::<u8>().ok()).flatten()
            });
            assert!(apt.is_some());
            assert!(primary_payloads.contains(&apt.unwrap_or_default()));
        }

        let producer_header_extensions = scenario
            .producer_parameters
            .header_extensions()
            .map(|extension| extension.uri().to_owned())
            .collect::<BTreeSet<_>>();
        let router_header_extensions = scenario
            .router_capabilities
            .header_extensions()
            .map(|extension| extension.uri().to_owned())
            .collect::<BTreeSet<_>>();
        for extension in consumable.header_extensions() {
            assert!(producer_header_extensions.contains(extension.uri()));
            assert!(router_header_extensions.contains(extension.uri()));
        }

        for binding in consumable.bindings() {
            if let Some(payload_type) = binding.payload_type() {
                assert!(primary_payloads.contains(&payload_type));
            }
        }
    } else {
        assert!(matches!(
            result,
            Err(RtpNegotiationError::UnsupportedProducerCodec { .. }
                | RtpNegotiationError::InvalidAptParameter { .. }
                | RtpNegotiationError::MissingAssociatedMediaCodecForRtx { .. })
        ));
    }
}

// Proves that consumer negotiation only succeeds when it can return a coherent
// media subset: at least one primary codec survives, header extensions stay in
// the consumable/capability intersection, RTX still targets live primaries, and
// every binding references a negotiated PT. The inline `can_consume` equality
// check makes sure the cheap admission predicate stays aligned with the real
// negotiation result.
#[kani::proof]
fn negotiate_consumer_rtp_parameters_keeps_only_surviving_consumer_links() {
    let scenario = consumer_scenario(kani::any::<u8>());
    let result = negotiate_consumer_rtp_parameters(
        &scenario.consumable_parameters,
        &scenario.consumer_capabilities,
    );

    assert_eq!(
        can_consume(
            &scenario.consumable_parameters,
            &scenario.consumer_capabilities
        ),
        result.is_ok()
    );

    if let Ok(negotiated) = result {
        let primary_payloads = negotiated
            .formats()
            .filter(|format| format.codec_name() != "rtx")
            .map(|format| format.payload_type())
            .collect::<BTreeSet<_>>();
        assert!(!primary_payloads.is_empty());

        let consumable_header_extensions = scenario
            .consumable_parameters
            .header_extensions()
            .map(|extension| extension.uri().to_owned())
            .collect::<BTreeSet<_>>();
        let consumer_header_extensions = scenario
            .consumer_capabilities
            .header_extensions()
            .map(|extension| extension.uri().to_owned())
            .collect::<BTreeSet<_>>();
        for extension in negotiated.header_extensions() {
            assert!(consumable_header_extensions.contains(extension.uri()));
            assert!(consumer_header_extensions.contains(extension.uri()));
        }

        let negotiated_payloads = negotiated
            .formats()
            .map(MediaFormat::payload_type)
            .collect::<BTreeSet<_>>();
        for format in negotiated
            .formats()
            .filter(|format| format.codec_name() == "rtx")
        {
            let apt = format.parameters().find_map(|(key, value)| {
                (key == "apt").then(|| value.parse::<u8>().ok()).flatten()
            });
            assert!(apt.is_some());
            assert!(primary_payloads.contains(&apt.unwrap_or_default()));
        }

        for binding in negotiated.bindings() {
            if let Some(payload_type) = binding.payload_type() {
                assert!(negotiated_payloads.contains(&payload_type));
            }
        }
    } else {
        assert_eq!(result, Err(RtpNegotiationError::NoCompatibleConsumerCodec));
    }
}

#[derive(Debug, Clone)]
struct DeriveScenario {
    producer_parameters: MediaStream,
    router_capabilities: MediaCapabilities,
}

#[derive(Debug, Clone)]
struct ConsumerScenario {
    consumable_parameters: MediaStream,
    consumer_capabilities: MediaCapabilities,
}

fn derive_scenario(selector: u8) -> DeriveScenario {
    let include_second_primary = selector & 0b0000_0001 != 0;
    let include_rtx = selector & 0b0000_0010 != 0;
    let router_supports_second_primary = selector & 0b0000_0100 != 0;
    let router_supports_rtx = selector & 0b0000_1000 != 0;
    let include_mid = selector & 0b0001_0000 != 0;
    let include_mid_header = selector & 0b0010_0000 != 0;
    let include_abs_send_time = selector & 0b0100_0000 != 0;
    let second_binding_kind = (selector >> 7) & 0b1;

    let primary_payload_a = 111;
    let primary_payload_b = 112;
    let mapped_payload_a = 101;
    let mapped_payload_b = 102;
    let rtx_payload = 113;

    let mut producer_formats = vec![
        MediaFormat::new(MediaKind::Video, "VP8", primary_payload_a, 90_000)
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
    ];
    if include_second_primary {
        producer_formats.push(
            MediaFormat::new(MediaKind::Video, "H264", primary_payload_b, 90_000)
                .with_parameter("packetization-mode", "1")
                .with_parameter("profile-level-id", "42e01f")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        );
    }
    if include_rtx {
        producer_formats.push(
            MediaFormat::new(MediaKind::Video, "rtx", rtx_payload, 90_000).with_parameter(
                "apt",
                if include_second_primary && second_binding_kind == 1 {
                    primary_payload_b.to_string()
                } else {
                    primary_payload_a.to_string()
                },
            ),
        );
    }

    let mut producer_header_extensions = Vec::new();
    if include_mid_header {
        producer_header_extensions.push(HeaderExtension::new(
            webrtc::rtp_header_extension_uri::MID,
            1,
        ));
    }
    if include_abs_send_time {
        producer_header_extensions.push(HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        ));
    }

    let mut producer_bindings = vec![
        StreamBinding::new()
            .with_ssrc(1_000)
            .with_codec_payload_type(primary_payload_a),
    ];
    if include_second_primary {
        let mut binding = StreamBinding::new().with_ssrc(2_000);
        if second_binding_kind == 0 {
            binding = binding.with_codec_payload_type(primary_payload_b);
        } else if include_rtx {
            binding = binding.with_codec_payload_type(rtx_payload);
        }
        producer_bindings.push(binding);
    }

    let mut producer_parameters = MediaStream::new(
        producer_formats,
        producer_header_extensions,
        producer_bindings,
    );
    if include_mid {
        producer_parameters = producer_parameters.with_mid("video-0");
    }

    let mut router_codecs = vec![
        MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
            .with_preferred_payload_type(mapped_payload_a)
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
    ];
    if include_second_primary && router_supports_second_primary {
        router_codecs.push(
            MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
                .with_preferred_payload_type(mapped_payload_b)
                .with_parameter("packetization-mode", "1")
                .with_parameter("profile-level-id", "42e032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        );
    }
    if include_rtx && router_supports_rtx {
        let apt =
            if include_second_primary && router_supports_second_primary && second_binding_kind == 1
            {
                mapped_payload_b
            } else {
                mapped_payload_a
            };
        router_codecs.push(
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_preferred_payload_type(120)
                .with_setting(CodecSetting::RtxAssociation(PayloadType::new(apt))),
        );
    }

    let mut router_header_extensions = Vec::new();
    if include_mid_header {
        router_header_extensions.push(HeaderExtension::new(HeaderExtensionUri::Mid, 7));
    }
    if include_abs_send_time {
        router_header_extensions.push(HeaderExtension::new(HeaderExtensionUri::AbsSendTime, 8));
    }
    router_header_extensions.push(HeaderExtension::new(
        HeaderExtensionUri::TransportWideCcDraft01,
        9,
    ));

    DeriveScenario {
        producer_parameters,
        router_capabilities: MediaCapabilities::new(router_codecs, router_header_extensions),
    }
}

fn consumer_scenario(selector: u8) -> ConsumerScenario {
    let include_second_primary = selector & 0b0000_0001 != 0;
    let include_rtx = selector & 0b0000_0010 != 0;
    let consumer_supports_second_primary = selector & 0b0000_0100 != 0;
    let consumer_supports_rtx = selector & 0b0000_1000 != 0;
    let include_mid_header = selector & 0b0001_0000 != 0;
    let include_abs_send_time = selector & 0b0010_0000 != 0;
    let second_binding_is_rtx = selector & 0b0100_0000 != 0;

    let primary_payload_a = 101;
    let primary_payload_b = 102;
    let rtx_payload = 120;

    let mut consumable_formats = vec![
        MediaFormat::new(MediaKind::Video, "VP8", primary_payload_a, 90_000)
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
    ];
    if include_second_primary {
        consumable_formats.push(
            MediaFormat::new(MediaKind::Video, "H264", primary_payload_b, 90_000)
                .with_parameter("packetization-mode", "1")
                .with_parameter("profile-level-id", "42e01f")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        );
    }
    if include_rtx {
        consumable_formats.push(
            MediaFormat::new(MediaKind::Video, "rtx", rtx_payload, 90_000).with_parameter(
                "apt",
                if include_second_primary && second_binding_is_rtx {
                    primary_payload_b.to_string()
                } else {
                    primary_payload_a.to_string()
                },
            ),
        );
    }

    let mut consumable_extensions = Vec::new();
    if include_mid_header {
        consumable_extensions.push(HeaderExtension::new(HeaderExtensionUri::Mid, 1));
    }
    if include_abs_send_time {
        consumable_extensions.push(HeaderExtension::new(HeaderExtensionUri::AbsSendTime, 4));
    }

    let mut consumable_bindings = vec![
        StreamBinding::new()
            .with_ssrc(3_000)
            .with_codec_payload_type(primary_payload_a),
    ];
    if include_second_primary {
        let mut binding = StreamBinding::new().with_ssrc(4_000);
        if second_binding_is_rtx && include_rtx {
            binding = binding.with_codec_payload_type(rtx_payload);
        } else {
            binding = binding.with_codec_payload_type(primary_payload_b);
        }
        consumable_bindings.push(binding);
    }

    let consumable_parameters = MediaStream::new(
        consumable_formats,
        consumable_extensions,
        consumable_bindings,
    );

    let mut consumer_codecs = vec![
        MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
    ];
    if include_second_primary && consumer_supports_second_primary {
        consumer_codecs.push(
            MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
                .with_parameter("packetization-mode", "1")
                .with_parameter("profile-level-id", "42e032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        );
    }
    if include_rtx && consumer_supports_rtx {
        let apt = if include_second_primary
            && consumer_supports_second_primary
            && second_binding_is_rtx
        {
            primary_payload_b
        } else {
            primary_payload_a
        };
        consumer_codecs.push(
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_setting(CodecSetting::RtxAssociation(PayloadType::new(apt))),
        );
    }

    let mut consumer_extensions = Vec::new();
    if include_mid_header {
        consumer_extensions.push(HeaderExtension::new(HeaderExtensionUri::Mid, 11));
    }
    if include_abs_send_time {
        consumer_extensions.push(HeaderExtension::new(HeaderExtensionUri::AbsSendTime, 12));
    }
    consumer_extensions.push(HeaderExtension::new(
        HeaderExtensionUri::TransportWideCcDraft01,
        13,
    ));

    ConsumerScenario {
        consumable_parameters,
        consumer_capabilities: MediaCapabilities::new(consumer_codecs, consumer_extensions),
    }
}
