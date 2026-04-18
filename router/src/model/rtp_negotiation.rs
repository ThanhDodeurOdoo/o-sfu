//! RTP capability matching between producers, routers, and consumers.
//!
//! These helpers keep codec and header-extension negotiation outside the pure
//! router state machine while still using the same typed RTP domain model.

use std::collections::BTreeSet;

use super::{
    CodecSetting, HeaderExtension, HeaderExtensionUri, MediaCapabilities, MediaCodec,
    MediaCodecCapability, MediaFormat, MediaKind, MediaStream, ParseDiagnostic,
    ParseDiagnosticKind, ParseDiagnosticSpec, PayloadType, RfcReference, RtcpFeedback,
    RtcpFeedbackKind, StreamBinding,
};

/// Failure raised while deriving or negotiating RTP parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtpNegotiationError {
    UnsupportedProducerCodec {
        codec_name: String,
        payload_type: u8,
    },
    InvalidAptParameter {
        codec_name: String,
        payload_type: u8,
    },
    MissingAssociatedMediaCodecForRtx {
        payload_type: u8,
        associated_payload_type: u8,
    },
    NoCompatibleConsumerCodec,
}

const RFC_3264_SECTION_6: RfcReference = RfcReference::new(
    "RFC 3264",
    "section 6",
    "https://www.rfc-editor.org/rfc/rfc3264#section-6",
);
const RFC_4588_SECTION_8_1: RfcReference = RfcReference::new(
    "RFC 4588",
    "section 8.1",
    "https://www.rfc-editor.org/rfc/rfc4588#section-8.1",
);

impl ParseDiagnostic for RtpNegotiationError {
    fn diagnostic(&self) -> ParseDiagnosticSpec {
        match self {
            Self::UnsupportedProducerCodec { .. } => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::UnsupportedFeature,
                "producer codec is valid but not supported by router capabilities",
                RFC_3264_SECTION_6,
                "capture producer media stream and router media capabilities and replay derive_consumable_rtp_parameters",
            ),
            Self::InvalidAptParameter { .. } => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::InvalidInput,
                "RTX codec has an invalid or missing apt parameter",
                RFC_4588_SECTION_8_1,
                "capture producer media stream and replay derive_consumable_rtp_parameters to inspect RTX linkage",
            ),
            Self::MissingAssociatedMediaCodecForRtx { .. } => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::InvalidInput,
                "RTX codec references an associated payload type that is not negotiated",
                RFC_4588_SECTION_8_1,
                "capture producer media stream and replay derive_consumable_rtp_parameters to inspect RTX linkage",
            ),
            Self::NoCompatibleConsumerCodec => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::UnsupportedFeature,
                "consumer capabilities have no compatible media codec with the consumable set",
                RFC_3264_SECTION_6,
                "capture consumable media stream and consumer capabilities and replay negotiate_consumer_rtp_parameters",
            ),
        }
    }
}

/// Derive consumable media stream data from a producer stream and router capabilities.
///
/// # Errors
///
/// Returns [`RtpNegotiationError::UnsupportedProducerCodec`] when a producer media codec does not
/// match any router media codec capability, [`RtpNegotiationError::InvalidAptParameter`] when a
/// RTX codec carries an invalid `apt` parameter, or
/// [`RtpNegotiationError::MissingAssociatedMediaCodecForRtx`] when a RTX codec references a media
/// payload that is not part of the negotiated media codec set.
pub fn derive_consumable_rtp_parameters(
    producer_parameters: &MediaStream,
    router_capabilities: &MediaCapabilities,
) -> Result<MediaStream, RtpNegotiationError> {
    let mut mapped_media_payload_by_original_payload = Vec::<(PayloadType, PayloadType)>::new();
    let mut consumable_formats = Vec::new();

    for producer_format in producer_parameters.formats() {
        if producer_format.codec().is_rtx() {
            continue;
        }
        let Some(capability_format) =
            find_matching_media_capability(producer_format, router_capabilities)
        else {
            return Err(RtpNegotiationError::UnsupportedProducerCodec {
                codec_name: producer_format.codec().as_str().to_owned(),
                payload_type: producer_format.payload_type(),
            });
        };
        let mapped_payload_type = mapped_payload_type(capability_format, producer_format);
        mapped_media_payload_by_original_payload
            .push((producer_format.payload_type_id(), mapped_payload_type));
        let feedback = intersect_feedback(
            producer_format.rtcp_feedback(),
            capability_format.rtcp_feedback(),
        );
        consumable_formats.push(clone_format_with_overrides(
            producer_format,
            mapped_payload_type,
            None,
            &feedback,
        ));
    }

    for producer_format in producer_parameters.formats() {
        if !producer_format.codec().is_rtx() {
            continue;
        }
        let associated_payload_type = parse_rtx_associated_payload(producer_format)?;
        let Some(mapped_associated_payload_type) = mapped_media_payload_by_original_payload
            .iter()
            .find_map(|(original, mapped)| {
                (*original == associated_payload_type).then_some(*mapped)
            })
        else {
            return Err(RtpNegotiationError::MissingAssociatedMediaCodecForRtx {
                payload_type: producer_format.payload_type(),
                associated_payload_type: associated_payload_type.value(),
            });
        };
        let Some(capability_format) = find_matching_rtx_capability(
            producer_format,
            mapped_associated_payload_type,
            router_capabilities,
        ) else {
            continue;
        };
        let mapped_payload_type = mapped_payload_type(capability_format, producer_format);
        let feedback = intersect_feedback(
            producer_format.rtcp_feedback(),
            capability_format.rtcp_feedback(),
        );
        consumable_formats.push(clone_format_with_overrides(
            producer_format,
            mapped_payload_type,
            Some(mapped_associated_payload_type),
            &feedback,
        ));
    }

    let header_extensions = router_capabilities
        .header_extensions()
        .map(clone_header_extension)
        .collect::<Vec<_>>();
    let bindings = producer_parameters
        .bindings()
        .map(|binding| {
            clone_binding_with_payload_mapping(binding, &mapped_media_payload_by_original_payload)
        })
        .collect::<Vec<_>>();

    let mut consumable = MediaStream::new(consumable_formats, header_extensions, bindings);
    if let Some(mid) = producer_parameters.mid() {
        consumable = consumable.with_mid(mid);
    }
    Ok(consumable)
}

/// Negotiate consumer media stream data from a consumable stream and consumer capabilities.
///
/// # Errors
///
/// Returns [`RtpNegotiationError::NoCompatibleConsumerCodec`] when no compatible media codec can
/// be negotiated.
pub fn negotiate_consumer_rtp_parameters(
    consumable_parameters: &MediaStream,
    consumer_capabilities: &MediaCapabilities,
) -> Result<MediaStream, RtpNegotiationError> {
    let negotiated_header_extensions =
        negotiate_header_extensions(consumable_parameters, consumer_capabilities);
    let feedback_policy = bwe_feedback_policy(&negotiated_header_extensions);

    let mut negotiated_formats = consumable_parameters
        .formats()
        .filter_map(|format| {
            let capability_format =
                find_matching_media_or_rtx_capability(format, consumer_capabilities)?;
            let feedback =
                intersect_feedback(format.rtcp_feedback(), capability_format.rtcp_feedback());
            let feedback = apply_bwe_feedback_policy(feedback, feedback_policy);
            Some(clone_format_with_overrides(
                format,
                format.payload_type_id(),
                None,
                &feedback,
            ))
        })
        .collect::<Vec<_>>();
    negotiated_formats = drop_unpaired_rtx_formats(negotiated_formats);

    if negotiated_formats
        .first()
        .is_none_or(|format| format.codec().is_rtx())
    {
        return Err(RtpNegotiationError::NoCompatibleConsumerCodec);
    }

    let bindings = consumable_parameters
        .bindings()
        .map(clone_binding)
        .collect::<Vec<_>>();

    let mut negotiated =
        MediaStream::new(negotiated_formats, negotiated_header_extensions, bindings);
    if let Some(mid) = consumable_parameters.mid() {
        negotiated = negotiated.with_mid(mid);
    }
    Ok(negotiated)
}

/// Check whether a consumer capability set can negotiate at least one media codec.
///
/// This is the boolean gateway used by router-core when it only needs the final
/// compatibility result and does not need the fully negotiated RTP output.
#[must_use]
pub fn can_consume(
    consumable_parameters: &MediaStream,
    consumer_capabilities: &MediaCapabilities,
) -> bool {
    negotiate_consumer_rtp_parameters(consumable_parameters, consumer_capabilities).is_ok()
}

fn mapped_payload_type(
    capability_format: &MediaCodecCapability,
    producer_format: &MediaFormat,
) -> PayloadType {
    capability_format
        .payload_type_id()
        .unwrap_or(producer_format.payload_type_id())
}

fn find_matching_media_capability<'a>(
    producer_format: &MediaFormat,
    router_capabilities: &'a MediaCapabilities,
) -> Option<&'a MediaCodecCapability> {
    router_capabilities.codecs().find(|capability_format| {
        !capability_format.codec().is_rtx()
            && codec_match_ignoring_payload_type(producer_format, capability_format)
    })
}

fn find_matching_rtx_capability<'a>(
    producer_format: &MediaFormat,
    mapped_associated_payload_type: PayloadType,
    router_capabilities: &'a MediaCapabilities,
) -> Option<&'a MediaCodecCapability> {
    router_capabilities.codecs().find(|capability_format| {
        capability_format.codec().is_rtx()
            && codec_match_ignoring_payload_type(producer_format, capability_format)
            && capability_format.settings().any(|setting| {
                matches!(
                    setting,
                    CodecSetting::RtxAssociation(payload_type)
                    if *payload_type == mapped_associated_payload_type
                )
            })
    })
}

fn find_matching_media_or_rtx_capability<'a>(
    format: &MediaFormat,
    capabilities: &'a MediaCapabilities,
) -> Option<&'a MediaCodecCapability> {
    capabilities
        .codecs()
        .find(|capability_format| codec_match_ignoring_payload_type(format, capability_format))
}

fn codec_match_ignoring_payload_type(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    if format.media_kind() != capability_format.media_kind()
        || format.codec() != capability_format.codec()
        || format.clock_rate() != capability_format.clock_rate()
    {
        return false;
    }
    if normalized_channels(format.media_kind(), format.channels())
        != normalized_channels(capability_format.media_kind(), capability_format.channels())
    {
        return false;
    }
    critical_codec_settings_match(format, capability_format)
}

fn critical_codec_settings_match(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    match format.codec() {
        MediaCodec::H264 => h264_critical_settings_match(format, capability_format),
        MediaCodec::Other(codec_name) if codec_name.eq_ignore_ascii_case("VP9") => {
            vp9_critical_settings_match(format, capability_format)
        }
        _ => true,
    }
}

fn h264_critical_settings_match(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    let format_packetization_mode = format
        .settings()
        .find_map(|setting| match setting {
            CodecSetting::H264PacketizationMode(mode) => Some(*mode),
            _ => None,
        })
        .unwrap_or(0);
    let capability_packetization_mode = capability_format
        .settings()
        .find_map(|setting| match setting {
            CodecSetting::H264PacketizationMode(mode) => Some(*mode),
            _ => None,
        })
        .unwrap_or(0);
    if format_packetization_mode != capability_packetization_mode {
        return false;
    }
    match (
        format.settings().find_map(|setting| match setting {
            CodecSetting::H264ProfileLevelId(profile_level_id) => Some(profile_level_id.as_str()),
            _ => None,
        }),
        capability_format
            .settings()
            .find_map(|setting| match setting {
                CodecSetting::H264ProfileLevelId(profile_level_id) => {
                    Some(profile_level_id.as_str())
                }
                _ => None,
            }),
    ) {
        (Some(format_profile_level_id), Some(capability_profile_level_id)) => {
            format_profile_level_id == capability_profile_level_id
        }
        _ => true,
    }
}

fn vp9_critical_settings_match(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    match (
        format.settings().find_map(|setting| match setting {
            CodecSetting::Vp9ProfileId(profile_id) => Some(*profile_id),
            _ => None,
        }),
        capability_format
            .settings()
            .find_map(|setting| match setting {
                CodecSetting::Vp9ProfileId(profile_id) => Some(*profile_id),
                _ => None,
            }),
    ) {
        (Some(format_profile_id), Some(capability_profile_id)) => {
            format_profile_id == capability_profile_id
        }
        _ => true,
    }
}

fn parse_rtx_associated_payload(format: &MediaFormat) -> Result<PayloadType, RtpNegotiationError> {
    format
        .settings()
        .find_map(|setting| match setting {
            CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
            _ => None,
        })
        .ok_or_else(|| RtpNegotiationError::InvalidAptParameter {
            codec_name: format.codec().as_str().to_owned(),
            payload_type: format.payload_type(),
        })
}

fn clone_format_with_overrides(
    source: &MediaFormat,
    payload_type: PayloadType,
    apt_override: Option<PayloadType>,
    feedback: &[RtcpFeedback],
) -> MediaFormat {
    let mut format = MediaFormat::new(
        source.media_kind(),
        source.codec().clone(),
        payload_type,
        source.clock_rate(),
    );
    if let Some(channels) = source.channels() {
        format = format.with_channels(channels);
    }
    for setting in source
        .settings()
        .filter(|setting| !matches!(setting, CodecSetting::RtxAssociation(_)))
        .cloned()
    {
        format = format.with_setting(setting);
    }
    if let Some(apt) = apt_override {
        format = format.with_setting(CodecSetting::RtxAssociation(apt));
    } else if let Some(apt) = source.settings().find_map(|setting| match setting {
        CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
        _ => None,
    }) {
        format = format.with_setting(CodecSetting::RtxAssociation(apt));
    }
    for entry in feedback {
        format = format.with_rtcp_feedback(entry.clone());
    }
    format
}

fn clone_header_extension(source: &HeaderExtension) -> HeaderExtension {
    let mut extension = HeaderExtension::new(source.uri_kind().clone(), source.id());
    if source.encrypt() {
        extension = extension.with_encryption(true);
    }
    extension
}

fn clone_binding(source: &StreamBinding) -> StreamBinding {
    let mut binding = StreamBinding::new();
    if let Some(ssrc) = source.ssrc_id() {
        binding = binding.with_ssrc(ssrc);
    }
    if let Some(rid) = source.rid_id() {
        binding = binding.with_rid(rid.clone());
    }
    if let Some(payload_type) = source.payload_type_id() {
        binding = binding.with_payload_type(payload_type);
    }
    if let Some(max_bitrate) = source.max_bitrate() {
        binding = binding.with_max_bitrate(max_bitrate);
    }
    binding
}

fn clone_binding_with_payload_mapping(
    source: &StreamBinding,
    mapped_media_payload_by_original_payload: &[(PayloadType, PayloadType)],
) -> StreamBinding {
    let mut binding = StreamBinding::new();
    if let Some(ssrc) = source.ssrc_id() {
        binding = binding.with_ssrc(ssrc);
    }
    if let Some(rid) = source.rid_id() {
        binding = binding.with_rid(rid.clone());
    }
    if let Some(payload_type) = source.payload_type_id() {
        let mapped_payload_type = mapped_media_payload_by_original_payload
            .iter()
            .find_map(|(original, mapped)| (*original == payload_type).then_some(*mapped))
            .unwrap_or(payload_type);
        binding = binding.with_payload_type(mapped_payload_type);
    }
    if let Some(max_bitrate) = source.max_bitrate() {
        binding = binding.with_max_bitrate(max_bitrate);
    }
    binding
}

fn intersect_feedback<'a>(
    format_feedback: impl Iterator<Item = &'a RtcpFeedback>,
    capability_feedback: impl Iterator<Item = &'a RtcpFeedback>,
) -> Vec<RtcpFeedback> {
    let capability_feedback = capability_feedback.cloned().collect::<Vec<_>>();
    format_feedback
        .filter(|feedback| capability_feedback.contains(feedback))
        .cloned()
        .collect()
}

fn normalized_channels(media_kind: MediaKind, channels: Option<u16>) -> Option<u16> {
    if media_kind == MediaKind::Audio {
        Some(channels.unwrap_or(1))
    } else {
        None
    }
}

fn negotiate_header_extensions(
    consumable_parameters: &MediaStream,
    consumer_capabilities: &MediaCapabilities,
) -> Vec<HeaderExtension> {
    let supported_uris = consumer_capabilities
        .header_extensions()
        .map(|extension| extension.uri_kind().clone())
        .collect::<BTreeSet<_>>();
    consumable_parameters
        .header_extensions()
        .filter(|extension| supported_uris.contains(extension.uri_kind()))
        .map(clone_header_extension)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BweFeedbackPolicy {
    PreferTransportCc,
    PreferGoogRemb,
    DisableBoth,
}

fn bwe_feedback_policy(header_extensions: &[HeaderExtension]) -> BweFeedbackPolicy {
    if header_extensions.iter().any(|extension| {
        matches!(
            extension.uri_kind(),
            HeaderExtensionUri::TransportWideCcDraft01
        )
    }) {
        return BweFeedbackPolicy::PreferTransportCc;
    }
    if header_extensions
        .iter()
        .any(|extension| matches!(extension.uri_kind(), HeaderExtensionUri::AbsSendTime))
    {
        return BweFeedbackPolicy::PreferGoogRemb;
    }
    BweFeedbackPolicy::DisableBoth
}

fn apply_bwe_feedback_policy(
    feedback: Vec<RtcpFeedback>,
    policy: BweFeedbackPolicy,
) -> Vec<RtcpFeedback> {
    feedback
        .into_iter()
        .filter(|entry| match policy {
            BweFeedbackPolicy::PreferTransportCc => {
                !matches!(entry.kind(), RtcpFeedbackKind::GoogRemb)
            }
            BweFeedbackPolicy::PreferGoogRemb => {
                !matches!(entry.kind(), RtcpFeedbackKind::TransportCc)
            }
            BweFeedbackPolicy::DisableBoth => !matches!(
                entry.kind(),
                RtcpFeedbackKind::TransportCc | RtcpFeedbackKind::GoogRemb
            ),
        })
        .collect()
}

fn drop_unpaired_rtx_formats(formats: Vec<MediaFormat>) -> Vec<MediaFormat> {
    let media_payload_types = formats
        .iter()
        .filter(|format| !format.codec().is_rtx())
        .map(MediaFormat::payload_type_id)
        .collect::<BTreeSet<_>>();
    formats
        .into_iter()
        .filter(|format| {
            if !format.codec().is_rtx() {
                return true;
            }
            format.settings().any(|setting| {
                matches!(
                    setting,
                    CodecSetting::RtxAssociation(payload_type)
                    if media_payload_types.contains(payload_type)
                )
            })
        })
        .collect()
}
