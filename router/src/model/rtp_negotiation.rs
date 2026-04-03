use std::collections::{BTreeMap, BTreeSet};

use super::{
    MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCapabilities, RtpCodecCapability,
    RtpCodecParameters, RtpEncoding, RtpHeaderExtension, RtpParameters,
};
use crate::rfc::webrtc;

const RTP_PARAMETER_APT: &str = "apt";
const H264_PACKETIZATION_MODE_PARAMETER: &str = "packetization-mode";
const H264_PROFILE_LEVEL_ID_PARAMETER: &str = "profile-level-id";
const CODEC_NAME_H264: &str = "h264";
const CODEC_NAME_RTX: &str = "rtx";

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

/// Derive consumable RTP parameters from producer parameters and router capabilities.
///
/// # Errors
///
/// Returns [`RtpNegotiationError::UnsupportedProducerCodec`] when a producer media codec does not
/// match any router media codec capability, [`RtpNegotiationError::InvalidAptParameter`] when a
/// RTX codec carries an invalid `apt` parameter, or
/// [`RtpNegotiationError::MissingAssociatedMediaCodecForRtx`] when a RTX codec references a media
/// payload that is not part of the negotiated media codec set.
pub fn derive_consumable_rtp_parameters(
    producer_parameters: &RtpParameters,
    router_capabilities: &RtpCapabilities,
) -> Result<RtpParameters, RtpNegotiationError> {
    let mut mapped_media_payload_by_original_payload = BTreeMap::new();
    let mut consumable_codecs = Vec::new();

    for producer_codec in producer_parameters.codecs() {
        if is_rtx_codec_name(producer_codec.codec_name()) {
            continue;
        }
        let Some(capability_codec) =
            find_matching_media_capability(producer_codec, router_capabilities)
        else {
            return Err(RtpNegotiationError::UnsupportedProducerCodec {
                codec_name: producer_codec.codec_name().to_owned(),
                payload_type: producer_codec.payload_type(),
            });
        };
        let mapped_payload_type = mapped_payload_type(capability_codec, producer_codec);
        mapped_media_payload_by_original_payload
            .insert(producer_codec.payload_type(), mapped_payload_type);
        let feedback = intersect_feedback(
            producer_codec.rtcp_feedback(),
            capability_codec.rtcp_feedback(),
        );
        consumable_codecs.push(clone_codec_with_overrides(
            producer_codec,
            mapped_payload_type,
            None,
            &feedback,
        ));
    }

    for producer_codec in producer_parameters.codecs() {
        if !is_rtx_codec_name(producer_codec.codec_name()) {
            continue;
        }
        let associated_payload_type = parse_rtx_associated_payload(
            producer_codec.codec_name(),
            producer_codec.payload_type(),
            producer_codec.parameters(),
        )?;
        let Some(mapped_associated_payload_type) =
            mapped_media_payload_by_original_payload.get(&associated_payload_type)
        else {
            return Err(RtpNegotiationError::MissingAssociatedMediaCodecForRtx {
                payload_type: producer_codec.payload_type(),
                associated_payload_type,
            });
        };
        let Some(capability_codec) = find_matching_rtx_capability(
            producer_codec,
            *mapped_associated_payload_type,
            router_capabilities,
        ) else {
            continue;
        };
        let mapped_payload_type = mapped_payload_type(capability_codec, producer_codec);
        let feedback = intersect_feedback(
            producer_codec.rtcp_feedback(),
            capability_codec.rtcp_feedback(),
        );
        consumable_codecs.push(clone_codec_with_overrides(
            producer_codec,
            mapped_payload_type,
            Some(*mapped_associated_payload_type),
            &feedback,
        ));
    }

    let header_extensions = router_capabilities
        .header_extensions()
        .map(clone_header_extension)
        .collect::<Vec<_>>();
    let encodings = producer_parameters
        .encodings()
        .map(|encoding| {
            clone_encoding_with_payload_mapping(encoding, &mapped_media_payload_by_original_payload)
        })
        .collect::<Vec<_>>();

    let mut consumable = RtpParameters::new(consumable_codecs, header_extensions, encodings);
    if let Some(mid) = producer_parameters.mid() {
        consumable = consumable.with_mid(mid.to_owned());
    }
    Ok(consumable)
}

/// Negotiate consumer RTP parameters from consumable parameters and consumer capabilities.
///
/// # Errors
///
/// Returns [`RtpNegotiationError::NoCompatibleConsumerCodec`] when no compatible media codec can
/// be negotiated.
pub fn negotiate_consumer_rtp_parameters(
    consumable_parameters: &RtpParameters,
    consumer_capabilities: &RtpCapabilities,
) -> Result<RtpParameters, RtpNegotiationError> {
    let negotiated_header_extensions =
        negotiate_header_extensions(consumable_parameters, consumer_capabilities);
    let feedback_policy = bwe_feedback_policy(&negotiated_header_extensions);

    let mut negotiated_codecs = consumable_parameters
        .codecs()
        .filter_map(|codec| {
            let capability_codec =
                find_matching_media_or_rtx_capability(codec, consumer_capabilities)?;
            let feedback =
                intersect_feedback(codec.rtcp_feedback(), capability_codec.rtcp_feedback());
            let feedback = apply_bwe_feedback_policy(feedback, feedback_policy);
            Some(clone_codec_with_overrides(
                codec,
                codec.payload_type(),
                None,
                &feedback,
            ))
        })
        .collect::<Vec<_>>();
    negotiated_codecs = drop_unpaired_rtx_codecs(negotiated_codecs);

    if negotiated_codecs
        .first()
        .is_none_or(|codec| is_rtx_codec_name(codec.codec_name()))
    {
        return Err(RtpNegotiationError::NoCompatibleConsumerCodec);
    }

    let encodings = consumable_parameters
        .encodings()
        .map(clone_encoding)
        .collect::<Vec<_>>();

    let mut negotiated =
        RtpParameters::new(negotiated_codecs, negotiated_header_extensions, encodings);
    if let Some(mid) = consumable_parameters.mid() {
        negotiated = negotiated.with_mid(mid.to_owned());
    }
    Ok(negotiated)
}

#[must_use]
pub fn can_consume(
    consumable_parameters: &RtpParameters,
    consumer_capabilities: &RtpCapabilities,
) -> bool {
    negotiate_consumer_rtp_parameters(consumable_parameters, consumer_capabilities).is_ok()
}

fn is_rtx_codec_name(codec_name: &str) -> bool {
    codec_name.eq_ignore_ascii_case(CODEC_NAME_RTX)
}

fn mapped_payload_type(
    capability_codec: &RtpCodecCapability,
    producer_codec: &RtpCodecParameters,
) -> u8 {
    capability_codec
        .preferred_payload_type()
        .unwrap_or(producer_codec.payload_type())
}

fn find_matching_media_capability<'a>(
    producer_codec: &RtpCodecParameters,
    router_capabilities: &'a RtpCapabilities,
) -> Option<&'a RtpCodecCapability> {
    router_capabilities.codecs().find(|capability_codec| {
        !is_rtx_codec_name(capability_codec.codec_name())
            && codec_match_ignoring_payload_type(producer_codec, capability_codec)
    })
}

fn find_matching_rtx_capability<'a>(
    producer_codec: &RtpCodecParameters,
    mapped_associated_payload_type: u8,
    router_capabilities: &'a RtpCapabilities,
) -> Option<&'a RtpCodecCapability> {
    router_capabilities.codecs().find(|capability_codec| {
        if !is_rtx_codec_name(capability_codec.codec_name()) {
            return false;
        }
        if !codec_match_ignoring_payload_type(producer_codec, capability_codec) {
            return false;
        }
        parse_optional_u8_from_parameters(capability_codec.parameters(), RTP_PARAMETER_APT)
            .is_some_and(|payload_type| payload_type == mapped_associated_payload_type)
    })
}

fn find_matching_media_or_rtx_capability<'a>(
    codec: &RtpCodecParameters,
    capabilities: &'a RtpCapabilities,
) -> Option<&'a RtpCodecCapability> {
    capabilities
        .codecs()
        .find(|capability_codec| codec_match_ignoring_payload_type(codec, capability_codec))
}

fn codec_match_ignoring_payload_type(
    codec: &RtpCodecParameters,
    capability_codec: &RtpCodecCapability,
) -> bool {
    if codec.media_kind() != capability_codec.media_kind()
        || !codec
            .codec_name()
            .eq_ignore_ascii_case(capability_codec.codec_name())
        || codec.clock_rate() != capability_codec.clock_rate()
    {
        return false;
    }
    if normalized_channels(codec.media_kind(), codec.channels())
        != normalized_channels(capability_codec.media_kind(), capability_codec.channels())
    {
        return false;
    }
    critical_codec_parameters_match(codec, capability_codec)
}

fn critical_codec_parameters_match(
    codec: &RtpCodecParameters,
    capability_codec: &RtpCodecCapability,
) -> bool {
    if !codec.codec_name().eq_ignore_ascii_case(CODEC_NAME_H264) {
        return true;
    }
    let codec_parameters = collect_parameter_map(codec.parameters());
    let capability_parameters = collect_parameter_map(capability_codec.parameters());
    let codec_packetization_mode = codec_parameters
        .get(H264_PACKETIZATION_MODE_PARAMETER)
        .map_or("0", String::as_str);
    let capability_packetization_mode = capability_parameters
        .get(H264_PACKETIZATION_MODE_PARAMETER)
        .map_or("0", String::as_str);
    if codec_packetization_mode != capability_packetization_mode {
        return false;
    }
    match (
        codec_parameters.get(H264_PROFILE_LEVEL_ID_PARAMETER),
        capability_parameters.get(H264_PROFILE_LEVEL_ID_PARAMETER),
    ) {
        (Some(codec_profile_level_id), Some(capability_profile_level_id)) => {
            codec_profile_level_id == capability_profile_level_id
        }
        _ => true,
    }
}

fn parse_rtx_associated_payload<'a>(
    codec_name: &str,
    payload_type: u8,
    parameters: impl Iterator<Item = (&'a str, &'a str)>,
) -> Result<u8, RtpNegotiationError> {
    parse_optional_u8_from_parameters(parameters, RTP_PARAMETER_APT).ok_or_else(|| {
        RtpNegotiationError::InvalidAptParameter {
            codec_name: codec_name.to_owned(),
            payload_type,
        }
    })
}

fn parse_optional_u8_from_parameters<'a>(
    mut parameters: impl Iterator<Item = (&'a str, &'a str)>,
    name: &str,
) -> Option<u8> {
    parameters.find_map(|(parameter_name, value): (&str, &str)| {
        if parameter_name != name {
            return None;
        }
        value.parse::<u8>().ok()
    })
}

fn collect_parameter_map<'a>(
    parameters: impl Iterator<Item = (&'a str, &'a str)>,
) -> BTreeMap<String, String> {
    parameters
        .map(|(name, value): (&str, &str)| (name.to_owned(), value.to_owned()))
        .collect()
}

fn clone_codec_with_overrides(
    source: &RtpCodecParameters,
    payload_type: u8,
    apt_override: Option<u8>,
    feedback: &[RtcpFeedback],
) -> RtpCodecParameters {
    let mut codec = RtpCodecParameters::new(
        source.media_kind(),
        source.codec_name().to_owned(),
        payload_type,
        source.clock_rate(),
    );
    if let Some(channels) = source.channels() {
        codec = codec.with_channels(channels);
    }
    let mut parameters = collect_parameter_map(source.parameters());
    if let Some(apt) = apt_override {
        parameters.insert(RTP_PARAMETER_APT.to_owned(), apt.to_string());
    }
    for (name, value) in parameters {
        codec = codec.with_parameter(name, value);
    }
    for entry in feedback {
        codec = codec.with_rtcp_feedback(entry.clone());
    }
    codec
}

fn clone_header_extension(source: &RtpHeaderExtension) -> RtpHeaderExtension {
    let mut extension = RtpHeaderExtension::new(source.uri().to_owned(), source.id());
    if source.encrypt() {
        extension = extension.with_encryption(true);
    }
    extension
}

fn clone_encoding(source: &RtpEncoding) -> RtpEncoding {
    let mut encoding = RtpEncoding::new();
    if let Some(ssrc) = source.ssrc() {
        encoding = encoding.with_ssrc(ssrc);
    }
    if let Some(rid) = source.rid() {
        encoding = encoding.with_rid(rid.to_owned());
    }
    if let Some(codec_payload_type) = source.codec_payload_type() {
        encoding = encoding.with_codec_payload_type(codec_payload_type);
    }
    if let Some(max_bitrate) = source.max_bitrate() {
        encoding = encoding.with_max_bitrate(max_bitrate);
    }
    encoding
}

fn clone_encoding_with_payload_mapping(
    source: &RtpEncoding,
    mapped_media_payload_by_original_payload: &BTreeMap<u8, u8>,
) -> RtpEncoding {
    let mut encoding = RtpEncoding::new();
    if let Some(ssrc) = source.ssrc() {
        encoding = encoding.with_ssrc(ssrc);
    }
    if let Some(rid) = source.rid() {
        encoding = encoding.with_rid(rid.to_owned());
    }
    if let Some(codec_payload_type) = source.codec_payload_type() {
        let mapped_payload_type = mapped_media_payload_by_original_payload
            .get(&codec_payload_type)
            .copied()
            .unwrap_or(codec_payload_type);
        encoding = encoding.with_codec_payload_type(mapped_payload_type);
    }
    if let Some(max_bitrate) = source.max_bitrate() {
        encoding = encoding.with_max_bitrate(max_bitrate);
    }
    encoding
}

fn intersect_feedback<'a>(
    codec_feedback: impl Iterator<Item = &'a RtcpFeedback>,
    capability_feedback: impl Iterator<Item = &'a RtcpFeedback>,
) -> Vec<RtcpFeedback> {
    let capability_feedback = capability_feedback.cloned().collect::<Vec<_>>();
    codec_feedback
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
    consumable_parameters: &RtpParameters,
    consumer_capabilities: &RtpCapabilities,
) -> Vec<RtpHeaderExtension> {
    let supported_uris = consumer_capabilities
        .header_extensions()
        .map(RtpHeaderExtension::uri)
        .collect::<BTreeSet<_>>();
    consumable_parameters
        .header_extensions()
        .filter(|extension| supported_uris.contains(extension.uri()))
        .map(clone_header_extension)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BweFeedbackPolicy {
    PreferTransportCc,
    PreferGoogRemb,
    DisableBoth,
}

fn bwe_feedback_policy(header_extensions: &[RtpHeaderExtension]) -> BweFeedbackPolicy {
    if header_extensions.iter().any(|extension| {
        extension.uri() == webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01
    }) {
        return BweFeedbackPolicy::PreferTransportCc;
    }
    if header_extensions
        .iter()
        .any(|extension| extension.uri() == webrtc::rtp_header_extension_uri::ABS_SEND_TIME)
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

fn drop_unpaired_rtx_codecs(codecs: Vec<RtpCodecParameters>) -> Vec<RtpCodecParameters> {
    let media_payload_types = codecs
        .iter()
        .filter(|codec| !is_rtx_codec_name(codec.codec_name()))
        .map(RtpCodecParameters::payload_type)
        .collect::<BTreeSet<_>>();
    codecs
        .into_iter()
        .filter(|codec| {
            if !is_rtx_codec_name(codec.codec_name()) {
                return true;
            }
            parse_optional_u8_from_parameters(codec.parameters(), RTP_PARAMETER_APT)
                .is_some_and(|apt| media_payload_types.contains(&apt))
        })
        .collect()
}
