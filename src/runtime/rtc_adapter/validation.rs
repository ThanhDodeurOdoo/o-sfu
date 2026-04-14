#[cfg(any(test, feature = "internal-benchmarks"))]
use o_sfu_router::ParseDiagnosticKind;
#[cfg(test)]
use std::str::FromStr;
#[cfg(test)]
use str0m::config::Fingerprint;
#[cfg(test)]
use tracing::debug;
#[cfg(any(test, feature = "internal-benchmarks"))]
use tracing::{error, warn};

#[cfg(any(test, feature = "internal-benchmarks"))]
use super::ice;
#[cfg(test)]
use super::{
    dtls, sdp,
    state::{ParsedRemoteIceCredentials, RtcSessionState},
};
use crate::runtime::transport_adapter::TransportAdapterError;
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::runtime::transport_bootstrap::{SessionTransportBootstrap, TransportIceCandidate};
#[cfg(test)]
use crate::runtime::transport_connect::{
    TransportConnectDtlsParameters, TransportConnectIceParameters,
};

#[cfg(any(test, feature = "internal-benchmarks"))]
const CANDIDATE_COMPONENT_ID_RTP: u16 = 1;

#[cfg(test)]
pub(super) fn validate_sdp_offer(sdp_offer: &str) -> Result<(), TransportAdapterError> {
    let parsed_offer = sdp::parse_offer_sdp(sdp_offer).map_err(|diagnostic| {
        map_sdp_diagnostic_to_adapter_error(diagnostic.as_ref(), diagnostic.replay_context())
    })?;
    log_validated_sdp_media_sections(parsed_offer.media_sections());
    Ok(())
}

#[cfg(test)]
pub(super) fn parse_dtls_parameters(
    dtls_parameters: &TransportConnectDtlsParameters,
) -> Result<dtls::ParsedDtlsParameters, TransportAdapterError> {
    match dtls::parse_dtls_parameters(dtls_parameters) {
        Ok(parsed) => Ok(parsed),
        Err(diagnostic) => match diagnostic.kind() {
            ParseDiagnosticKind::InvalidInput => {
                error!(
                    summary = diagnostic.summary(),
                    rfc_document = diagnostic.rfc_reference().document(),
                    rfc_section = diagnostic.rfc_reference().section(),
                    rfc_url = diagnostic.rfc_reference().url(),
                    replay_context = diagnostic.replay_context(),
                    "invalid DTLS payload on rtc adapter boundary"
                );
                Err(TransportAdapterError::InvalidInput)
            }
            ParseDiagnosticKind::UnsupportedFeature => {
                warn!(
                    summary = diagnostic.summary(),
                    rfc_document = diagnostic.rfc_reference().document(),
                    rfc_section = diagnostic.rfc_reference().section(),
                    rfc_url = diagnostic.rfc_reference().url(),
                    replay_context = diagnostic.replay_context(),
                    "unsupported DTLS feature on rtc adapter boundary"
                );
                Err(TransportAdapterError::UnsupportedFeature)
            }
        },
    }
}

#[cfg(test)]
pub(super) fn validate_dtls_parameters(
    dtls_parameters: &TransportConnectDtlsParameters,
) -> Result<(), TransportAdapterError> {
    parse_dtls_parameters(dtls_parameters).map(|_parsed| ())
}

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(super) fn validate_bootstrap_payload(
    payload: &SessionTransportBootstrap,
) -> Result<(), TransportAdapterError> {
    validate_ice_candidates(
        payload.download_transport.id.as_str(),
        payload.download_transport.ice_candidates.as_slice(),
    )?;
    validate_ice_candidates(
        payload.upload_transport.id.as_str(),
        payload.upload_transport.ice_candidates.as_slice(),
    )?;
    Ok(())
}

#[cfg(test)]
pub(super) fn parse_remote_fingerprint(
    fingerprint: &dtls::ParsedDtlsFingerprint,
) -> Result<Fingerprint, TransportAdapterError> {
    let fingerprint_string = format!("{} {}", fingerprint.algorithm(), fingerprint.value());
    Fingerprint::from_str(&fingerprint_string).map_err(|_error| TransportAdapterError::InvalidInput)
}

#[cfg(test)]
pub(super) fn parse_remote_ice_credentials(
    ice_parameters: Option<&TransportConnectIceParameters>,
) -> Result<Option<ParsedRemoteIceCredentials>, TransportAdapterError> {
    let Some(ice_parameters) = ice_parameters else {
        return Ok(None);
    };
    let Some(username_fragment) = ice_parameters.username_fragment.as_deref() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let Some(password) = ice_parameters.password.as_deref() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    Ok(Some(ParsedRemoteIceCredentials {
        username_fragment: username_fragment.to_owned(),
        password: password.to_owned(),
    }))
}

#[cfg(test)]
pub(super) fn ensure_remote_fingerprint_compatibility(
    session_state: &RtcSessionState,
    remote_fingerprint: &str,
) -> Result<(), TransportAdapterError> {
    let Some(existing_fingerprint) = session_state.remote_dtls_fingerprint.as_deref() else {
        return Ok(());
    };
    if existing_fingerprint == remote_fingerprint {
        Ok(())
    } else {
        Err(TransportAdapterError::InvalidInput)
    }
}

#[cfg(test)]
pub(super) fn ensure_remote_ice_credentials_compatibility(
    session_state: &RtcSessionState,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
) -> Result<(), TransportAdapterError> {
    let Some(existing_credentials) = session_state.remote_ice_credentials.as_ref() else {
        return Ok(());
    };
    let Some(remote_ice_credentials) = remote_ice_credentials else {
        return Ok(());
    };
    if existing_credentials == remote_ice_credentials {
        Ok(())
    } else {
        Err(TransportAdapterError::InvalidInput)
    }
}

#[cfg(test)]
pub(super) fn local_dtls_active_role(parsed_role: dtls::ParsedDtlsRole) -> bool {
    match parsed_role {
        dtls::ParsedDtlsRole::Server => true,
        dtls::ParsedDtlsRole::Auto | dtls::ParsedDtlsRole::Client => false,
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
fn validate_ice_candidates(
    transport_id: &str,
    candidates: &[TransportIceCandidate],
) -> Result<(), TransportAdapterError> {
    for candidate in candidates {
        let line = candidate_to_sdp_line(candidate);
        match ice::parse_ice_candidate(line.as_str()) {
            Ok(_parsed) => {}
            Err(diagnostic) => match diagnostic.kind() {
                ParseDiagnosticKind::InvalidInput => {
                    error!(
                        transport_id,
                        summary = diagnostic.summary(),
                        rfc_document = diagnostic.rfc_reference().document(),
                        rfc_section = diagnostic.rfc_reference().section(),
                        rfc_url = diagnostic.rfc_reference().url(),
                        replay_context = diagnostic.replay_context(),
                        "invalid bootstrap ICE candidate on rtc adapter boundary"
                    );
                    return Err(TransportAdapterError::InvalidInput);
                }
                ParseDiagnosticKind::UnsupportedFeature => {
                    warn!(
                        transport_id,
                        summary = diagnostic.summary(),
                        rfc_document = diagnostic.rfc_reference().document(),
                        rfc_section = diagnostic.rfc_reference().section(),
                        rfc_url = diagnostic.rfc_reference().url(),
                        replay_context = diagnostic.replay_context(),
                        "unsupported bootstrap ICE candidate on rtc adapter boundary"
                    );
                    return Err(TransportAdapterError::UnsupportedFeature);
                }
            },
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "internal-benchmarks"))]
fn candidate_to_sdp_line(candidate: &TransportIceCandidate) -> String {
    format!(
        "candidate:{} {CANDIDATE_COMPONENT_ID_RTP} {} {} {} {} typ {}",
        candidate.foundation,
        candidate.protocol.as_str(),
        candidate.priority,
        candidate.ip,
        candidate.port,
        candidate.candidate_type.as_str(),
    )
}

#[cfg(test)]
fn map_sdp_diagnostic_to_adapter_error(
    diagnostic: &sdp::SdpParseDiagnostic,
    replay_context: &str,
) -> TransportAdapterError {
    match diagnostic {
        sdp::SdpParseDiagnostic::InvalidInput { context, .. } => {
            error!(
                summary = diagnostic.summary(),
                expected = context.expected(),
                got = context.got(),
                line_number = context.line_number().map_or(0, |line| line),
                line = context.line().unwrap_or(""),
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                rfc_url = diagnostic.rfc_reference().url(),
                replay_context,
                "invalid SDP offer on rtc adapter boundary"
            );
            TransportAdapterError::InvalidInput
        }
        sdp::SdpParseDiagnostic::UnsupportedFeature { context, .. } => {
            warn!(
                summary = diagnostic.summary(),
                got = context.got(),
                line_number = context.line_number(),
                line = context.line(),
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                rfc_url = diagnostic.rfc_reference().url(),
                replay_context,
                "unsupported SDP feature on rtc adapter boundary"
            );
            TransportAdapterError::UnsupportedFeature
        }
    }
}

#[cfg(test)]
fn log_validated_sdp_media_sections(media_sections: &[sdp::ParsedMediaSection]) {
    debug!(
        media_section_count = media_sections.len(),
        "validated SDP offer on rtc adapter boundary"
    );
    for section in media_sections {
        debug!(
            media_kind = ?section.media_kind(),
            port = section.port(),
            transport_protocol = ?section.transport_protocol(),
            payload_format_count = section.formats().len(),
            "parsed SDP media section"
        );
    }
}
