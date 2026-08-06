use o_sfu_router::{MediaKind as RouterMediaKind, rtp::MediaCapabilities};
use str0m::{
    change::SdpAnswer,
    format::{Codec, PayloadParams},
};

use super::rtp_projection;
use crate::engine::media_transport::TransportAdapterError;

#[must_use]
#[cfg(any(test, feature = "fuzzing"))]
pub fn client_rtp_capabilities_from_answer(answer_sdp: &str) -> Option<MediaCapabilities> {
    let answer = SdpAnswer::from_sdp_string(answer_sdp).ok()?;
    client_rtp_capabilities_from_sdp_answer(&answer).unwrap_or_default()
}

pub(super) fn client_rtp_capabilities_from_sdp_answer(
    answer: &SdpAnswer,
) -> Result<Option<MediaCapabilities>, TransportAdapterError> {
    let mut codecs = Vec::new();
    let mut header_extensions = Vec::new();

    for media_line in &answer.media_lines {
        if media_line.disabled {
            continue;
        }
        let rtp_parameters = media_line.rtp_params();
        let Some(media_kind) = media_kind_label(&rtp_parameters) else {
            continue;
        };
        for payload in &rtp_parameters {
            if payload.spec().codec == Codec::Rtx {
                continue;
            }
            let codec = rtp_projection::media_capability(media_kind, payload)?;
            if !codecs.contains(&codec) {
                codecs.push(codec);
            }
        }
        for (id, extension) in media_line.extmaps() {
            let header_extension = rtp_projection::header_extension((id, extension))?;
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
