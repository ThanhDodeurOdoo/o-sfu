use std::net::IpAddr;

use tracing::{error, trace, warn};

const CANDIDATE_PREFIX: &str = "candidate:";
const CANDIDATE_TYPE_TOKEN: &str = "typ";

const ICE_TRANSPORT_UDP: &str = "udp";
const ICE_TRANSPORT_TCP: &str = "tcp";

const ICE_CANDIDATE_TYPE_HOST: &str = "host";
const ICE_CANDIDATE_TYPE_SERVER_REFLEXIVE: &str = "srflx";
const ICE_CANDIDATE_TYPE_PEER_REFLEXIVE: &str = "prflx";
const ICE_CANDIDATE_TYPE_RELAYED: &str = "relay";

const RFC_8445_SECTION_5_1_1: IceRfcReference = IceRfcReference::new(
    "RFC 8445",
    "5.1.1",
    "https://www.rfc-editor.org/rfc/rfc8445#section-5.1.1",
);
const RFC_5245_SECTION_15_1: IceRfcReference = IceRfcReference::new(
    "RFC 5245",
    "15.1",
    "https://www.rfc-editor.org/rfc/rfc5245#section-15.1",
);

pub(super) type IceParseResult<T> = Result<T, Box<IceParseDiagnostic>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IceDiagnosticKind {
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IceRfcReference {
    document: &'static str,
    section: &'static str,
    url: &'static str,
}

impl IceRfcReference {
    #[must_use]
    pub const fn new(document: &'static str, section: &'static str, url: &'static str) -> Self {
        Self {
            document,
            section,
            url,
        }
    }

    #[must_use]
    pub fn document(&self) -> &'static str {
        self.document
    }

    #[must_use]
    pub fn section(&self) -> &'static str {
        self.section
    }

    #[must_use]
    pub fn url(&self) -> &'static str {
        self.url
    }
}

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
pub(super) enum IceParseDiagnostic {
    InvalidInput {
        summary: &'static str,
        rfc_reference: IceRfcReference,
        context: Box<IceInvalidContext>,
    },
    UnsupportedFeature {
        summary: &'static str,
        rfc_reference: IceRfcReference,
        context: Box<IceUnsupportedContext>,
    },
}

impl IceParseDiagnostic {
    #[must_use]
    pub(super) fn kind(&self) -> IceDiagnosticKind {
        match self {
            Self::InvalidInput { .. } => IceDiagnosticKind::InvalidInput,
            Self::UnsupportedFeature { .. } => IceDiagnosticKind::UnsupportedFeature,
        }
    }

    #[must_use]
    pub(super) fn summary(&self) -> &'static str {
        match self {
            Self::InvalidInput { summary, .. } | Self::UnsupportedFeature { summary, .. } => {
                summary
            }
        }
    }

    #[must_use]
    pub(super) fn rfc_reference(&self) -> IceRfcReference {
        match self {
            Self::InvalidInput { rfc_reference, .. }
            | Self::UnsupportedFeature { rfc_reference, .. } => *rfc_reference,
        }
    }

    #[must_use]
    pub(super) fn replay_context(&self) -> &str {
        match self {
            Self::InvalidInput { context, .. } => &context.raw_candidate,
            Self::UnsupportedFeature { context, .. } => &context.raw_candidate,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IceTransportProtocol {
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IceCandidateType {
    Host,
    ServerReflexive,
    PeerReflexive,
    Relayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedIceCandidate {
    foundation: String,
    component_id: u16,
    transport_protocol: IceTransportProtocol,
    priority: u32,
    address: IpAddr,
    port: u16,
    candidate_type: IceCandidateType,
}

#[cfg(test)]
impl ParsedIceCandidate {
    #[must_use]
    pub(super) fn component_id(&self) -> u16 {
        self.component_id
    }

    #[must_use]
    pub(super) fn transport_protocol(&self) -> IceTransportProtocol {
        self.transport_protocol
    }

    #[must_use]
    pub(super) fn candidate_type(&self) -> IceCandidateType {
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
        .strip_prefix(CANDIDATE_PREFIX)
        .unwrap_or(normalized);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 8 {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    let Some(foundation_token) = tokens.first() else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(component_id_token) = tokens.get(1) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(transport_token) = tokens.get(2) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(priority_token) = tokens.get(3) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(address_token) = tokens.get(4) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(port_token) = tokens.get(5) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(candidate_label_token) = tokens.get(6) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(candidate_type_token) = tokens.get(7) else {
        let diagnostic = invalid_input(
            "ICE candidate line is incomplete",
            String::from(
                "<foundation> <component-id> <transport> <priority> <connection-address> <port> typ <type>",
            ),
            normalized.to_owned(),
            RFC_5245_SECTION_15_1,
            raw_candidate,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
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
    if component_id != 1 && component_id != 2 {
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
    if component_id != 1 {
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

fn parse_transport(token: &str, raw_candidate: &str) -> IceParseResult<IceTransportProtocol> {
    let lower = token.to_ascii_lowercase();
    match lower.as_str() {
        ICE_TRANSPORT_UDP => Ok(IceTransportProtocol::Udp),
        ICE_TRANSPORT_TCP => {
            let diagnostic = unsupported_feature(
                "ICE TCP candidates are not supported yet",
                token,
                RFC_8445_SECTION_5_1_1,
                raw_candidate,
            );
            log_diagnostic(&diagnostic);
            Err(Box::new(diagnostic))
        }
        _ => {
            let diagnostic = invalid_input(
                "ICE candidate transport token is invalid",
                String::from("udp"),
                token.to_owned(),
                RFC_8445_SECTION_5_1_1,
                raw_candidate,
            );
            log_diagnostic(&diagnostic);
            Err(Box::new(diagnostic))
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
        log_diagnostic(&diagnostic);
        Box::new(diagnostic)
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
        log_diagnostic(&diagnostic);
        Box::new(diagnostic)
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
    if token == CANDIDATE_TYPE_TOKEN {
        return Ok(());
    }
    let diagnostic = invalid_input(
        "ICE candidate is missing `typ` token before candidate type",
        String::from("typ"),
        token.to_owned(),
        RFC_5245_SECTION_15_1,
        raw_candidate,
    );
    log_diagnostic(&diagnostic);
    Err(Box::new(diagnostic))
}

fn parse_candidate_type(token: &str, raw_candidate: &str) -> IceParseResult<IceCandidateType> {
    match token {
        ICE_CANDIDATE_TYPE_HOST => Ok(IceCandidateType::Host),
        ICE_CANDIDATE_TYPE_SERVER_REFLEXIVE => Ok(IceCandidateType::ServerReflexive),
        ICE_CANDIDATE_TYPE_PEER_REFLEXIVE => Ok(IceCandidateType::PeerReflexive),
        ICE_CANDIDATE_TYPE_RELAYED => Ok(IceCandidateType::Relayed),
        _ => {
            let diagnostic = unsupported_feature(
                "ICE candidate type is valid but not supported yet",
                token,
                RFC_5245_SECTION_15_1,
                raw_candidate,
            );
            log_diagnostic(&diagnostic);
            Err(Box::new(diagnostic))
        }
    }
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
    log_diagnostic(&diagnostic);
    Err(Box::new(diagnostic))
}

fn invalid_input(
    summary: &'static str,
    expected: String,
    got: String,
    rfc_reference: IceRfcReference,
    raw_candidate: &str,
) -> IceParseDiagnostic {
    IceParseDiagnostic::InvalidInput {
        summary,
        rfc_reference,
        context: Box::new(IceInvalidContext {
            expected,
            got,
            raw_candidate: raw_candidate.to_owned(),
        }),
    }
}

fn unsupported_feature(
    summary: &'static str,
    got: &str,
    rfc_reference: IceRfcReference,
    raw_candidate: &str,
) -> IceParseDiagnostic {
    IceParseDiagnostic::UnsupportedFeature {
        summary,
        rfc_reference,
        context: Box::new(IceUnsupportedContext {
            got: got.to_owned(),
            raw_candidate: raw_candidate.to_owned(),
        }),
    }
}

fn log_diagnostic(diagnostic: &IceParseDiagnostic) {
    match diagnostic {
        IceParseDiagnostic::InvalidInput {
            summary,
            rfc_reference,
            context,
        } => {
            error!(
                summary,
                expected = context.expected,
                got = context.got,
                rfc_document = rfc_reference.document(),
                rfc_section = rfc_reference.section(),
                "invalid ICE candidate"
            );
        }
        IceParseDiagnostic::UnsupportedFeature {
            summary,
            rfc_reference,
            context,
        } => {
            warn!(
                summary,
                got = context.got,
                rfc_document = rfc_reference.document(),
                rfc_section = rfc_reference.section(),
                "unsupported ICE candidate feature"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IceCandidateType, IceDiagnosticKind, IceTransportProtocol, parse_ice_candidate};

    #[test]
    fn parse_ice_candidate_accepts_supported_udp_host_candidate() {
        let candidate = "candidate:1 1 udp 2113937151 203.0.113.10 54400 typ host";
        let result = parse_ice_candidate(candidate);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.component_id(), 1);
        assert_eq!(parsed.transport_protocol(), IceTransportProtocol::Udp);
        assert_eq!(parsed.candidate_type(), IceCandidateType::Host);
    }

    #[test]
    fn parse_ice_candidate_rejects_incomplete_line() {
        let result = parse_ice_candidate("candidate:1 1 udp");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), IceDiagnosticKind::InvalidInput);
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
        assert_eq!(diagnostic.kind(), IceDiagnosticKind::UnsupportedFeature);
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
        assert_eq!(diagnostic.kind(), IceDiagnosticKind::UnsupportedFeature);
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
        assert_eq!(diagnostic.kind(), IceDiagnosticKind::UnsupportedFeature);
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
