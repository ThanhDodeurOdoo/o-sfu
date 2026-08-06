use o_sfu_router::{MediaKind as RouterMediaKind, rtp::MediaCapabilities};
use str0m::{
    change::SdpAnswer,
    format::{Codec, PayloadParams},
};

use super::{profile::RtpProfile, rtp_projection};
use crate::engine::media_transport::TransportAdapterError;
#[cfg(any(test, feature = "fuzzing"))]
use crate::{CodecPreferences, MediaCodecFlags};

#[must_use]
#[cfg(any(test, feature = "fuzzing"))]
pub fn client_rtp_capabilities_from_answer(answer_sdp: &str) -> Option<MediaCapabilities> {
    let answer = SdpAnswer::from_sdp_string(answer_sdp).ok()?;
    let profile = RtpProfile::compile(
        MediaCodecFlags::default()
            .with_opus(true)
            .with_pcmu(true)
            .with_pcma(true)
            .with_vp8(true)
            .with_h264(true)
            .with_h265(true)
            .with_vp9(true)
            .with_av1(true),
        CodecPreferences::default(),
    )
    .ok()?;
    client_rtp_capabilities_from_sdp_answer(&answer, &profile).unwrap_or_default()
}

pub(super) fn client_rtp_capabilities_from_sdp_answer(
    answer: &SdpAnswer,
    profile: &RtpProfile,
) -> Result<Option<MediaCapabilities>, TransportAdapterError> {
    let mut codecs = Vec::new();
    let mut header_extensions = Vec::new();

    for media_line in &answer.media_lines {
        if media_line.disabled {
            continue;
        }
        let rtp_parameters = profile.project_downstream_answer_payloads(&media_line.rtp_params());
        let Some(media_kind) = media_kind_label(&rtp_parameters) else {
            continue;
        };
        for payload in &rtp_parameters {
            let codec = rtp_projection::media_capability(media_kind, payload)?;
            if !codecs.contains(&codec) {
                codecs.push(codec);
            }
        }
        for (id, extension) in media_line.extmaps() {
            let Some(header_extension) =
                profile.project_answer_header_extension((id, extension))?
            else {
                continue;
            };
            if !header_extensions.contains(&header_extension) {
                header_extensions.push(header_extension);
            }
        }
    }

    if codecs.is_empty() {
        return Ok(None);
    }

    Ok(Some(MediaCapabilities::new(codecs, header_extensions)))
}

fn media_kind_label(payloads: &[PayloadParams]) -> Option<RouterMediaKind> {
    payloads
        .iter()
        .find(|payload| payload.spec().codec != Codec::Rtx)
        .or_else(|| payloads.first())
        .map(rtp_projection::media_kind)
}

#[cfg(test)]
#[path = "TESTS/negotiated_capabilities.rs"]
mod tests;
