use o_sfu_rfc::{
    rtp::{
        codec_name, fmtp,
        h264::{LevelIdc, PacketizationMode, Profile, ProfileLevelId},
    },
    webrtc,
};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{HeaderExtension as RouterHeaderExtension, MediaCapabilities},
};
use str0m::{
    Rtc, RtcConfig,
    format::{Codec, CodecConfig, FormatParams, PayloadParams},
    media::{Frequency, MediaKind},
    rtp::Extension,
};

use super::rtp_projection;
use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    engine::media_transport::TransportAdapterError,
};

const VP8_PAYLOAD_TYPE: u8 = 96;

const H264_PROFILES: &[(u8, u8, PacketizationMode, Profile)] = &[
    (
        127,
        121,
        PacketizationMode::NonInterleaved,
        Profile::Baseline,
    ),
    (
        125,
        107,
        PacketizationMode::SingleNalUnit,
        Profile::Baseline,
    ),
    (
        108,
        109,
        PacketizationMode::NonInterleaved,
        Profile::ConstrainedBaseline,
    ),
    (
        124,
        120,
        PacketizationMode::SingleNalUnit,
        Profile::ConstrainedBaseline,
    ),
    (123, 119, PacketizationMode::NonInterleaved, Profile::Main),
    (35, 36, PacketizationMode::SingleNalUnit, Profile::Main),
    (114, 115, PacketizationMode::NonInterleaved, Profile::High),
];

#[derive(Debug)]
pub(in crate::engine::media_transport) struct RtpProfile {
    config: RtcConfig,
    router_capabilities: MediaCapabilities,
    audio_names: Vec<String>,
    video_names: Vec<String>,
    simulcast_codec: Option<Codec>,
}

impl RtpProfile {
    pub(in crate::engine::media_transport) fn compile(
        flags: MediaCodecFlags,
        preferences: CodecPreferences,
    ) -> Result<Self, TransportAdapterError> {
        let mut config = Rtc::builder()
            .clear_codecs()
            .clear_extension_map()
            .set_extension(1, Extension::AudioLevel)
            .set_extension(2, Extension::AbsoluteSendTime)
            .set_extension(3, Extension::TransportSequenceNumber)
            .set_extension(4, Extension::RtpMid)
            .set_extension(10, Extension::RtpStreamId)
            .set_extension(13, Extension::VideoOrientation)
            .set_rtp_mode(true);
        let codecs = config.codec_config();
        for codec in preferences.audio_order() {
            if !codec.enabled_by(flags) {
                continue;
            }
            match codec {
                AudioCodecPreference::Opus => codecs.enable_opus(true),
                AudioCodecPreference::Pcmu => codecs.enable_pcmu(true),
                AudioCodecPreference::Pcma => codecs.enable_pcma(true),
            }
        }
        for codec in preferences.video_order() {
            if !codec.enabled_by(flags) {
                continue;
            }
            match codec {
                VideoCodecPreference::Vp8 => codecs.add_config(
                    VP8_PAYLOAD_TYPE.into(),
                    Some(97.into()),
                    Codec::Vp8,
                    Frequency::NINETY_KHZ,
                    None,
                    FormatParams::default(),
                ),
                VideoCodecPreference::H264 => add_h264_codecs(codecs),
                VideoCodecPreference::H265 => codecs.enable_h265(true),
                VideoCodecPreference::Vp9 => codecs.enable_vp9(true),
                VideoCodecPreference::Av1 => codecs.enable_av1(true),
            }
        }
        let simulcast_codec = preferences
            .video_order()
            .into_iter()
            .find(|codec| codec.enabled_by(flags))
            .and_then(|codec| match codec {
                VideoCodecPreference::Vp8 => Some(Codec::Vp8),
                VideoCodecPreference::H264 => Some(Codec::H264),
                VideoCodecPreference::H265
                | VideoCodecPreference::Vp9
                | VideoCodecPreference::Av1 => None,
            });
        let mut router_codecs = Vec::new();
        let mut audio_names = Vec::new();
        let mut video_names = Vec::new();
        for payload in config.codec_config().params() {
            let kind = rtp_projection::media_kind(payload);
            router_codecs.push(rtp_projection::media_capability(kind, payload)?);
            router_codecs.extend(rtp_projection::rtx_capability(kind, payload)?);
            let names = if kind == RouterMediaKind::Audio {
                &mut audio_names
            } else {
                &mut video_names
            };
            let name = payload.spec().codec.to_string();
            if !names.contains(&name) {
                names.push(name);
            }
        }
        let header_extensions = config
            .extension_map()
            .iter()
            .map(rtp_projection::header_extension)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            config,
            router_capabilities: MediaCapabilities::new(router_codecs, header_extensions),
            audio_names,
            video_names,
            simulcast_codec,
        })
    }

    pub(super) fn session_config(&self) -> RtcConfig {
        self.config.clone()
    }

    pub(in crate::engine::media_transport) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }

    pub(super) fn codec_names(&self, kind: MediaKind) -> &[String] {
        if kind.is_video() {
            &self.video_names
        } else {
            &self.audio_names
        }
    }

    pub(super) fn simulcast_codec(&self) -> Option<Codec> {
        self.simulcast_codec
    }

    pub(super) fn validate_answer_sdp(answer_sdp: &str) -> Result<(), TransportAdapterError> {
        if answer_has_forbidden_downstream_repair(answer_sdp) {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(())
    }

    pub(super) fn strip_downstream_repair(offer_sdp: &str) -> String {
        project_repair_by_direction(offer_sdp, "a=sendonly")
    }

    pub(super) fn strip_downstream_answer_repair(answer_sdp: &str) -> String {
        project_repair_by_direction(answer_sdp, "a=recvonly")
    }

    pub(super) fn project_answer_payloads(&self, payloads: &[PayloadParams]) -> Vec<PayloadParams> {
        let mut config = self.config.clone();
        let codecs = config.codec_config();
        payloads
            .iter()
            .filter_map(|remote| {
                codecs
                    .match_params(*remote)
                    .map(|local| negotiated_payload(remote, local))
            })
            .collect()
    }

    pub(super) fn project_downstream_answer_payloads(
        &self,
        payloads: &[PayloadParams],
    ) -> Vec<PayloadParams> {
        self.project_answer_payloads(payloads)
            .iter()
            .map(without_retransmission)
            .collect()
    }

    pub(super) fn project_answer_header_extension(
        &self,
        extension: (u8, &Extension),
    ) -> Result<Option<RouterHeaderExtension>, TransportAdapterError> {
        let projected = rtp_projection::header_extension(extension)?;
        Ok(self
            .router_capabilities
            .header_extensions()
            .any(|allowed| allowed == &projected)
            .then_some(projected))
    }
}

fn project_repair_by_direction(sdp: &str, stripped_direction: &str) -> String {
    let mut sections = vec![Vec::new()];
    for line in sdp.lines() {
        if line.starts_with("m=") {
            sections.push(Vec::new());
        }
        if let Some(section) = sections.last_mut() {
            section.push(line);
        }
    }
    let mut projected = String::with_capacity(sdp.len());
    for section in sections {
        append_sdp_section(&mut projected, &section, stripped_direction);
    }
    projected
}

fn add_h264_codecs(codecs: &mut CodecConfig) {
    for &(payload_type, resend_payload_type, packetization_mode, profile) in H264_PROFILES {
        codecs.add_h264(
            payload_type.into(),
            Some(resend_payload_type.into()),
            packetization_mode == PacketizationMode::NonInterleaved,
            ProfileLevelId::new(profile, LevelIdc::Level3_1).packed_value(),
        );
    }
}

fn forbidden_answer_attribute(line: &str) -> bool {
    let Some(attribute) = line.strip_prefix(webrtc::sdp::ATTRIBUTE_PREFIX) else {
        return false;
    };
    let Some((name, value)) = attribute.split_once(':') else {
        return false;
    };
    if name.eq_ignore_ascii_case(webrtc::sdp::attribute::RTPMAP) {
        return value
            .split_ascii_whitespace()
            .nth(1)
            .and_then(|encoding| encoding.split('/').next())
            .is_some_and(|codec| codec.eq_ignore_ascii_case(codec_name::RTX));
    }
    if name.eq_ignore_ascii_case(webrtc::sdp::attribute::FMTP) {
        return value
            .split(|byte: char| byte.is_ascii_whitespace() || byte == ';')
            .filter_map(|parameter| parameter.split_once('='))
            .any(|(key, _value)| key.eq_ignore_ascii_case(fmtp::RTX_ASSOCIATION));
    }
    if name.eq_ignore_ascii_case(webrtc::sdp::attribute::RTCP_FB) {
        let mut feedback = value.split_ascii_whitespace().skip(1);
        return feedback
            .next()
            .is_some_and(|kind| kind.eq_ignore_ascii_case(webrtc::rtcp_feedback::kind::NACK))
            && feedback.next().is_none();
    }
    if name.eq_ignore_ascii_case(webrtc::sdp::attribute::EXTMAP) {
        return value.split_ascii_whitespace().nth(1).is_some_and(|uri| {
            uri.eq_ignore_ascii_case(webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID)
        });
    }
    name.eq_ignore_ascii_case(webrtc::sdp::attribute::SSRC_GROUP)
        && value
            .split_ascii_whitespace()
            .next()
            .is_some_and(|semantics| {
                semantics.eq_ignore_ascii_case(webrtc::sdp::ssrc_group_semantics::FID)
            })
}

fn answer_has_forbidden_downstream_repair(answer_sdp: &str) -> bool {
    let mut section = Vec::new();
    for line in answer_sdp.lines() {
        if line.starts_with("m=") {
            if answer_section_has_forbidden_repair(&section) {
                return true;
            }
            section.clear();
        }
        section.push(line);
    }
    answer_section_has_forbidden_repair(&section)
}

fn answer_section_has_forbidden_repair(section: &[&str]) -> bool {
    let media = section.first().is_some_and(|line| line.starts_with("m="));
    let receives_server_media = !section
        .iter()
        .any(|line| matches!(*line, "a=sendonly" | "a=inactive"));
    (!media || receives_server_media) && section.iter().any(|line| forbidden_answer_attribute(line))
}

fn append_sdp_section(output: &mut String, section: &[&str], stripped_direction: &str) {
    let strips_repair = section.contains(&stripped_direction);
    if !strips_repair {
        append_sdp_lines(output, section.iter().copied());
        return;
    }
    let rtx_payload_types = section
        .iter()
        .filter_map(|line| {
            let value = line.strip_prefix("a=rtpmap:")?;
            let mut fields = value.split_ascii_whitespace();
            let payload_type = fields.next()?;
            fields
                .next()?
                .split('/')
                .next()?
                .eq_ignore_ascii_case(codec_name::RTX)
                .then_some(payload_type)
        })
        .collect::<Vec<_>>();
    let repair_ssrcs = section
        .iter()
        .filter_map(|line| {
            line.strip_prefix("a=ssrc-group:FID ")?
                .split_ascii_whitespace()
                .nth(1)
        })
        .collect::<Vec<_>>();
    for line in section {
        if line.starts_with("m=") {
            let fields = line
                .split_ascii_whitespace()
                .enumerate()
                .filter_map(|(index, field)| {
                    (index < 3 || !rtx_payload_types.contains(&field)).then_some(field)
                });
            append_sdp_line(output, &fields.collect::<Vec<_>>().join(" "));
        } else if offer_repair_line(line, &rtx_payload_types, &repair_ssrcs) {
            append_sdp_line(output, line);
        }
    }
}

fn offer_repair_line(line: &str, rtx_payload_types: &[&str], repair_ssrcs: &[&str]) -> bool {
    if line.starts_with("a=ssrc-group:FID ")
        || line.contains(webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID)
    {
        return false;
    }
    if let Some(value) = line.strip_prefix("a=ssrc:")
        && value
            .split_ascii_whitespace()
            .next()
            .is_some_and(|ssrc| repair_ssrcs.contains(&ssrc))
    {
        return false;
    }
    for prefix in ["a=rtpmap:", "a=fmtp:", "a=rtcp-fb:"] {
        if let Some(value) = line.strip_prefix(prefix)
            && value
                .split_ascii_whitespace()
                .next()
                .is_some_and(|pt| rtx_payload_types.contains(&pt))
        {
            return false;
        }
    }
    !forbidden_answer_attribute(line)
}

fn append_sdp_lines<'a>(output: &mut String, lines: impl IntoIterator<Item = &'a str>) {
    for line in lines {
        append_sdp_line(output, line);
    }
}

fn append_sdp_line(output: &mut String, line: &str) {
    output.push_str(line);
    output.push_str("\r\n");
}

fn without_retransmission(payload: &PayloadParams) -> PayloadParams {
    let mut projected = PayloadParams::new(payload.pt(), None, payload.spec());
    projected.set_fb_transport_cc(payload.fb_transport_cc());
    projected.set_fb_nack(false);
    projected.set_fb_pli(payload.fb_pli());
    projected.set_fb_fir(payload.fb_fir());
    projected.set_fb_remb(payload.fb_remb());
    projected
}

fn negotiated_payload(remote: &PayloadParams, local: &PayloadParams) -> PayloadParams {
    let resend = local.resend().and_then(|_| remote.resend());
    let mut projected = PayloadParams::new(remote.pt(), resend, remote.spec());
    projected.set_fb_transport_cc(remote.fb_transport_cc() && local.fb_transport_cc());
    projected.set_fb_nack(remote.fb_nack() && local.fb_nack());
    projected.set_fb_pli(remote.fb_pli() && local.fb_pli());
    projected.set_fb_fir(remote.fb_fir() && local.fb_fir());
    projected.set_fb_remb(remote.fb_remb() && local.fb_remb());
    projected
}

#[cfg(test)]
#[path = "TESTS/profile.rs"]
mod tests;
