use std::net::IpAddr;

use super::parse_diagnostic::{AdapterParseDiagnostic, ParseResult};
use crate::rfc::webrtc;
use o_sfu_router::RfcReference;
use tracing::{error, trace, warn};

const ICE_REPLAY_CONTEXT_HINT: &str = "raw ICE candidate line";
const EXPECTED_CANDIDATE_FORMAT: &str =
    "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>";

const RFC_8445_SECTION_5_1_1: RfcReference = RfcReference::new(
    "RFC 8445",
    "5.1.1",
    "https://www.rfc-editor.org/rfc/rfc8445#section-5.1.1",
);
const RFC_5245_SECTION_15_1: RfcReference = RfcReference::new(
    "RFC 5245",
    "15.1",
    "https://www.rfc-editor.org/rfc/rfc5245#section-15.1",
);

pub(super) type IceParseResult<T> = ParseResult<T, IceInvalidContext, IceUnsupportedContext>;
pub(super) type IceParseDiagnostic =
    AdapterParseDiagnostic<IceInvalidContext, IceUnsupportedContext>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IceInvalidContext {
    expected: String,
    got: String,
    raw_candidate: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IceUnsupportedContext {
    got: String,
    raw_candidate: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedIceCandidate {
    foundation: String,
    component_id: u16,
    transport_protocol: webrtc::IceTransport,
    priority: u32,
    address: IpAddr,
    port: u16,
    candidate_type: webrtc::IceCandidateType,
}

#[cfg(test)]
impl ParsedIceCandidate {
    #[must_use]
    pub(super) fn component_id(&self) -> u16 {
        self.component_id
    }

    #[must_use]
    pub(super) fn transport_protocol(&self) -> webrtc::IceTransport {
        self.transport_protocol
    }

    #[must_use]
    pub(super) fn candidate_type(&self) -> webrtc::IceCandidateType {
        self.candidate_type
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "The parser keeps the full ICE candidate token mapping in one place so diagnostics preserve exact field-level context."
)]
pub(super) fn parse_ice_candidate(raw_candidate: &str) -> IceParseResult<ParsedIceCandidate> {
    trace!(candidate = %raw_candidate, "parsing incoming ICE candidate");
    let normalized = raw_candidate.trim();
    let normalized = normalized
        .strip_prefix(webrtc::ice::candidate_attribute::PREFIX)
        .unwrap_or(normalized);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let foundation_token = required_token(&tokens, 0, normalized, raw_candidate)?;
    let component_id_token = required_token(&tokens, 1, normalized, raw_candidate)?;
    let transport_token = required_token(&tokens, 2, normalized, raw_candidate)?;
    let priority_token = required_token(&tokens, 3, normalized, raw_candidate)?;
    let address_token = required_token(&tokens, 4, normalized, raw_candidate)?;
    let port_token = required_token(&tokens, 5, normalized, raw_candidate)?;
    let candidate_label_token = required_token(&tokens, 6, normalized, raw_candidate)?;
    let candidate_type_token = required_token(&tokens, 7, normalized, raw_candidate)?;
    let extension_tokens = tokens.get(8..).unwrap_or(&[]);

    let foundation = (*foundation_token).to_owned();
    let component_id = parse_component_id(component_id_token, raw_candidate)?;
    let transport_protocol = parse_transport(transport_token, raw_candidate)?;
    let priority = parse_priority(priority_token, raw_candidate)?;
    let address = parse_address(address_token, raw_candidate)?;
    let port = parse_port(port_token, raw_candidate)?;
    ensure_type_label(candidate_label_token, raw_candidate)?;
    let candidate_type = parse_candidate_type(candidate_type_token, raw_candidate)?;
    ensure_supported_extensions(extension_tokens, raw_candidate)?;
    Ok(ParsedIceCandidate {
        foundation,
        component_id,
        transport_protocol,
        priority,
        address,
        port,
        candidate_type,
    })
}

fn required_token<'a>(
    tokens: &'a [&str],
    index: usize,
    normalized_candidate: &str,
    raw_candidate: &str,
) -> IceParseResult<&'a str> {
    tokens.get(index).copied().ok_or_else(|| {
        boxed_diagnostic(invalid_input(
            "ICE candidate line is incomplete",
            String::from(EXPECTED_CANDIDATE_FORMAT),
            normalized_candidate.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        ))
    })
}

fn parse_component_id(token: &str, raw_candidate: &str) -> IceParseResult<u16> {
    let component_id = token.parse::<u16>().map_err(|_error| {
        let diagnostic = invalid_input(
            "ICE candidate component ID is invalid",
            String::from("1 or 2"),
            token.to_owned(),
            RFC_8445_SECTION_5_1_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        Box::new(diagnostic)
    })?;
    if component_id != webrtc::ice::component::RTP && component_id != webrtc::ice::component::RTCP {
        let diagnostic = invalid_input(
            "ICE candidate component ID is out of range",
            String::from("1 or 2"),
            token.to_owned(),
            RFC_8445_SECTION_5_1_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    if component_id != webrtc::ice::component::RTP {
        let diagnostic = unsupported_feature(
            "ICE RTCP component candidates are not supported yet",
            token,
            RFC_8445_SECTION_5_1_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    Ok(component_id)
}

fn parse_transport(token: &str, raw_candidate: &str) -> IceParseResult<webrtc::IceTransport> {
    match webrtc::IceTransport::parse(token) {
        Some(webrtc::IceTransport::Udp) => Ok(webrtc::IceTransport::Udp),
        Some(webrtc::IceTransport::Tcp) => {
            let diagnostic = unsupported_feature(
                "ICE TCP candidates are not supported yet",
                token,
                RFC_8445_SECTION_5_1_1,
                raw_candidate,
            );
            Err(boxed_diagnostic(diagnostic))
        }
        None => {
            let diagnostic = invalid_input(
                "ICE candidate transport token is invalid",
                String::from("udp"),
                token.to_owned(),
                RFC_8445_SECTION_5_1_1,
                raw_candidate,
            );
            Err(boxed_diagnostic(diagnostic))
        }
    }
}

fn parse_priority(token: &str, raw_candidate: &str) -> IceParseResult<u32> {
    token.parse::<u32>().map_err(|_error| {
        let diagnostic = invalid_input(
            "ICE candidate priority is invalid",
            String::from("u32"),
            token.to_owned(),
            RFC_8445_SECTION_5_1_1,
            raw_candidate,
        );
        boxed_diagnostic(diagnostic)
    })
}

fn parse_address(token: &str, raw_candidate: &str) -> IceParseResult<IpAddr> {
    token.parse::<IpAddr>().map_err(|_error| {
        let diagnostic = unsupported_feature(
            "ICE candidate address format is valid but not supported yet",
            token,
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        boxed_diagnostic(diagnostic)
    })
}

fn parse_port(token: &str, raw_candidate: &str) -> IceParseResult<u16> {
    token.parse::<u16>().map_err(|_error| {
        let diagnostic = invalid_input(
            "ICE candidate port is invalid",
            String::from("0-65535"),
            token.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        Box::new(diagnostic)
    })
}

fn ensure_type_label(token: &str, raw_candidate: &str) -> IceParseResult<()> {
    if token == webrtc::ice::candidate_attribute::TYPE_LABEL {
        return Ok(());
    }
    let diagnostic = invalid_input(
        "ICE candidate is missing `typ` token before candidate type",
        String::from(webrtc::ice::candidate_attribute::TYPE_LABEL),
        token.to_owned(),
        RFC_5245_SECTION_15_1,
        raw_candidate,
    );
    Err(boxed_diagnostic(diagnostic))
}

fn parse_candidate_type(
    token: &str,
    raw_candidate: &str,
) -> IceParseResult<webrtc::IceCandidateType> {
    webrtc::IceCandidateType::parse(token).map_or_else(
        || {
            let diagnostic = unsupported_feature(
                "ICE candidate type is valid but not supported yet",
                token,
                RFC_5245_SECTION_15_1,
                raw_candidate,
            );
            Err(boxed_diagnostic(diagnostic))
        },
        Ok,
    )
}

fn ensure_supported_extensions(tokens: &[&str], raw_candidate: &str) -> IceParseResult<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    let diagnostic = unsupported_feature(
        "ICE candidate extension attributes are not supported yet",
        &tokens.join(" "),
        RFC_5245_SECTION_15_1,
        raw_candidate,
    );
    Err(boxed_diagnostic(diagnostic))
}

fn invalid_input(
    summary: &'static str,
    expected: String,
    got: String,
    rfc_reference: RfcReference,
    raw_candidate: &str,
) -> IceParseDiagnostic {
    IceParseDiagnostic::invalid_input(
        summary,
        rfc_reference,
        ICE_REPLAY_CONTEXT_HINT,
        IceInvalidContext {
            expected,
            got,
            raw_candidate: raw_candidate.to_owned(),
        },
        raw_candidate.to_owned(),
    )
}

fn unsupported_feature(
    summary: &'static str,
    got: &str,
    rfc_reference: RfcReference,
    raw_candidate: &str,
) -> IceParseDiagnostic {
    IceParseDiagnostic::unsupported_feature(
        summary,
        rfc_reference,
        ICE_REPLAY_CONTEXT_HINT,
        IceUnsupportedContext {
            got: got.to_owned(),
            raw_candidate: raw_candidate.to_owned(),
        },
        raw_candidate.to_owned(),
    )
}

fn boxed_diagnostic(diagnostic: IceParseDiagnostic) -> Box<IceParseDiagnostic> {
    log_diagnostic(&diagnostic);
    Box::new(diagnostic)
}

fn log_diagnostic(diagnostic: &IceParseDiagnostic) {
    match diagnostic {
        IceParseDiagnostic::InvalidInput { context, .. } => {
            error!(
                summary = diagnostic.summary(),
                expected = context.expected,
                got = context.got,
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                "invalid ICE candidate"
            );
        }
        IceParseDiagnostic::UnsupportedFeature { context, .. } => {
            warn!(
                summary = diagnostic.summary(),
                got = context.got,
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                "unsupported ICE candidate feature"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_router::ParseDiagnosticKind;

    use super::parse_ice_candidate;
    use crate::rfc::webrtc;

    #[test]
    fn parse_ice_candidate_accepts_supported_udp_host_candidate() {
        let candidate = "candidate:1 1 udp 2113937151 203.0.113.10 54400 typ host";
        let result = parse_ice_candidate(candidate);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.component_id(), 1);
        assert_eq!(parsed.transport_protocol(), webrtc::IceTransport::Udp);
        assert_eq!(parsed.candidate_type(), webrtc::IceCandidateType::Host);
    }

    #[test]
    fn parse_ice_candidate_rejects_incomplete_line() {
        let result = parse_ice_candidate("candidate:1 1 udp");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
        assert_eq!(diagnostic.summary(), "ICE candidate line is incomplete");
    }

    #[test]
    fn parse_ice_candidate_marks_tcp_transport_as_unsupported() {
        let candidate = "candidate:1 1 tcp 2113937151 203.0.113.10 9 typ host";
        let result = parse_ice_candidate(candidate);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "ICE TCP candidates are not supported yet"
        );
    }

    #[test]
    fn parse_ice_candidate_marks_rtcp_component_as_unsupported() {
        let candidate = "candidate:1 2 udp 2113937151 203.0.113.10 54400 typ host";
        let result = parse_ice_candidate(candidate);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "ICE RTCP component candidates are not supported yet"
        );
    }

    #[test]
    fn parse_ice_candidate_marks_extensions_as_unsupported() {
        let candidate = "candidate:1 1 udp 2113937151 203.0.113.10 54400 typ host generation 0";
        let result = parse_ice_candidate(candidate);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "ICE candidate extension attributes are not supported yet"
        );
    }

    #[test]
    fn parse_ice_candidate_preserves_replay_context() {
        let result = parse_ice_candidate("candidate:1 1 udp 2113937151 203.0.113.10 x typ host");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert!(
            diagnostic
                .replay_context()
                .contains("candidate:1 1 udp 2113937151 203.0.113.10 x typ host")
        );
    }
}
