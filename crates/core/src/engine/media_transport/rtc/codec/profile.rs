use o_sfu_rfc::{rtp, webrtc};
use o_sfu_router::{MediaKind as RouterMediaKind, rtp::MediaCapabilities};
use str0m::{
    Rtc, RtcConfig,
    format::{Codec, CodecConfig, PayloadParams},
    media::MediaKind,
    rtp::Extension,
};

use super::capabilities;
use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    engine::media_transport::TransportAdapterError,
};

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
                VideoCodecPreference::Vp8 => codecs.enable_vp8(true),
                VideoCodecPreference::H264 => codecs.enable_h264(true),
                VideoCodecPreference::H265 => codecs.enable_h265(true),
                VideoCodecPreference::Vp9 => codecs.enable_vp9(true),
                VideoCodecPreference::Av1 => codecs.enable_av1(true),
            }
        }
        let payloads = codecs.params().iter().map(without_retransmission).collect();
        *codecs = CodecConfig::new_from_payload_params(payloads);
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
            let kind = capabilities::media_kind(payload);
            router_codecs.push(capabilities::media_capability(kind, payload)?);
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
            .map(capabilities::header_extension)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            config,
            router_capabilities: MediaCapabilities::new(router_codecs, header_extensions),
            audio_names,
            video_names,
            simulcast_codec,
        })
    }

    pub(in crate::engine::media_transport::rtc) fn session_config(&self) -> RtcConfig {
        self.config.clone()
    }

    pub(in crate::engine::media_transport) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }

    pub(in crate::engine::media_transport::rtc) fn codec_names(
        &self,
        kind: MediaKind,
    ) -> &[String] {
        if kind.is_video() {
            &self.video_names
        } else {
            &self.audio_names
        }
    }

    pub(super) fn simulcast_codec(&self) -> Option<Codec> {
        self.simulcast_codec
    }

    /// Rejects answer attributes that would recreate an unsupported repair leg.
    ///
    /// Bare Generic NACK is feedback rather than an RTX identity, so only RTX
    /// payloads, `apt`, repaired RID and FID topology are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] for RTX payloads, `apt`
    /// mappings, repaired RID extensions or FID SSRC groups.
    pub(in crate::engine::media_transport::rtc) fn validate_answer_sdp(
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        if answer_sdp.lines().any(is_retransmission_attribute) {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(())
    }
}

/// Projects str0m's default video profile onto o-sfu's forwarding model.
///
/// Generic NACK identifies missing RTP packets. O-sfu does not retain forwarded
/// packets in a retransmission cache and local `RtpWrite`s stay non-nackable, so
/// advertising RTX would promise repair that cannot be produced. PLI and FIR
/// remain because they ask the encoder for decoder resynchronization instead.
/// Generic NACK and PLI follow
/// [RFC 4585 section 6.2.1](https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1),
/// [RFC 4585 section 6.3.1](https://www.rfc-editor.org/rfc/rfc4585.html#section-6.3.1).
/// FIR follows
/// [RFC 5104 section 4.3.1](https://www.rfc-editor.org/rfc/rfc5104.html#section-4.3.1).
/// RTX negotiation follows
/// [RFC 4588 section 8.1](https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1).
fn without_retransmission(payload: &PayloadParams) -> PayloadParams {
    let mut projected = PayloadParams::new(payload.pt(), None, payload.spec());
    projected.set_fb_transport_cc(payload.fb_transport_cc());
    projected.set_fb_nack(false);
    projected.set_fb_pli(payload.fb_pli());
    projected.set_fb_fir(payload.fb_fir());
    projected.set_fb_remb(payload.fb_remb());
    projected
}

fn is_retransmission_attribute(line: &str) -> bool {
    const RTPMAP: &str = "rtpmap";
    const FMTP: &str = "fmtp";
    const SSRC_GROUP: &str = "ssrc-group";
    const FLOW_IDENTIFICATION: &str = "FID";

    let Some(value) = line.strip_prefix("a=") else {
        return false;
    };
    let Some((name, value)) = value.split_once(':') else {
        return false;
    };
    if name.eq_ignore_ascii_case(RTPMAP) {
        return value
            .split_ascii_whitespace()
            .nth(1)
            .and_then(|encoding| encoding.split('/').next())
            .is_some_and(|codec| codec.eq_ignore_ascii_case(rtp::codec_name::RTX));
    }
    if name.eq_ignore_ascii_case(FMTP) {
        return value
            .split(|character: char| character.is_ascii_whitespace() || character == ';')
            .filter_map(|parameter| parameter.split_once('='))
            .any(|(key, _value)| key.eq_ignore_ascii_case(rtp::fmtp::RTX_ASSOCIATION));
    }
    if name.eq_ignore_ascii_case(webrtc::sdp::attribute::EXTMAP) {
        return value.split_ascii_whitespace().nth(1).is_some_and(|uri| {
            uri.eq_ignore_ascii_case(webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID)
        });
    }
    name.eq_ignore_ascii_case(SSRC_GROUP)
        && value
            .split_ascii_whitespace()
            .next()
            .is_some_and(|semantics| semantics.eq_ignore_ascii_case(FLOW_IDENTIFICATION))
}

#[cfg(test)]
#[path = "../TESTS/profile.rs"]
mod tests;
