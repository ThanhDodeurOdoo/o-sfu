use tracing::{error, trace, warn};

const MEDIA_DESCRIPTION_PREFIX: &str = "m=";
const SUPPORTED_TRANSPORT_PROTOCOL: &str = "UDP/TLS/RTP/SAVPF";
const VALID_BUT_UNSUPPORTED_TRANSPORT_PROTOCOLS: [&str; 5] = [
    "UDP/TLS/RTP/SAVP",
    "RTP/SAVPF",
    "RTP/SAVP",
    "UDP/DTLS/SCTP",
    "TCP/DTLS/SCTP",
];
const EXPECTED_MEDIA_LINE_FORMAT: &str = "<media> <port> <proto> <fmt>...";

const MEDIA_KIND_AUDIO: &str = "audio";
const MEDIA_KIND_VIDEO: &str = "video";
const MEDIA_KIND_APPLICATION: &str = "application";

const RFC_8866_SECTION_5_14: SdpRfcReference = SdpRfcReference::new(
    "RFC 8866",
    "5.14",
    "https://www.rfc-editor.org/rfc/rfc8866#section-5.14",
);
const RFC_8829_SECTION_5_8: SdpRfcReference = SdpRfcReference::new(
    "RFC 8829",
    "5.8",
    "https://www.rfc-editor.org/rfc/rfc8829#section-5.8",
);

pub(super) type SdpParseResult<T> = Result<T, Box<SdpParseDiagnostic>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SdpDiagnosticKind {
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SdpRfcReference {
    document: &'static str,
    section: &'static str,
    url: &'static str,
}

impl SdpRfcReference {
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
pub(super) struct SdpInvalidContext {
    expected: String,
    got: String,
    line_number: Option<usize>,
    line: Option<String>,
    raw_sdp: String,
}

impl SdpInvalidContext {
    #[must_use]
    pub(super) fn line_number(&self) -> Option<usize> {
        self.line_number
    }

    #[must_use]
    pub(super) fn line(&self) -> Option<&str> {
        self.line.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SdpUnsupportedContext {
    got: String,
    line_number: usize,
    line: String,
    raw_sdp: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SdpParseDiagnostic {
    InvalidInput {
        summary: &'static str,
        rfc_reference: SdpRfcReference,
        context: Box<SdpInvalidContext>,
    },
    UnsupportedFeature {
        summary: &'static str,
        rfc_reference: SdpRfcReference,
        context: Box<SdpUnsupportedContext>,
    },
}

impl SdpParseDiagnostic {
    #[must_use]
    pub(super) fn kind(&self) -> SdpDiagnosticKind {
        match self {
            Self::InvalidInput { .. } => SdpDiagnosticKind::InvalidInput,
            Self::UnsupportedFeature { .. } => SdpDiagnosticKind::UnsupportedFeature,
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
    pub(super) fn rfc_reference(&self) -> SdpRfcReference {
        match self {
            Self::InvalidInput { rfc_reference, .. }
            | Self::UnsupportedFeature { rfc_reference, .. } => *rfc_reference,
        }
    }

    #[must_use]
    pub(super) fn replay_context(&self) -> &str {
        match self {
            Self::InvalidInput { context, .. } => &context.raw_sdp,
            Self::UnsupportedFeature { context, .. } => &context.raw_sdp,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedOfferSdp {
    media_sections: Vec<ParsedMediaSection>,
}

impl ParsedOfferSdp {
    #[must_use]
    pub(super) fn media_sections(&self) -> &[ParsedMediaSection] {
        &self.media_sections
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedMediaSection {
    media_kind: ParsedMediaKind,
    port: u16,
    transport_protocol: ParsedTransportProtocol,
    formats: Vec<String>,
}

impl ParsedMediaSection {
    #[must_use]
    pub(super) fn media_kind(&self) -> ParsedMediaKind {
        self.media_kind
    }

    #[must_use]
    pub(super) fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub(super) fn transport_protocol(&self) -> ParsedTransportProtocol {
        self.transport_protocol
    }

    #[must_use]
    pub(super) fn formats(&self) -> &[String] {
        &self.formats
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParsedMediaKind {
    Audio,
    Video,
    Application,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParsedTransportProtocol {
    UdpTlsRtpSavpf,
}

struct MediaLineTokens<'a> {
    media_kind_token: &'a str,
    port_token: &'a str,
    transport_protocol_token: &'a str,
    formats: Vec<&'a str>,
}

pub(super) fn parse_offer_sdp(raw_sdp: &str) -> SdpParseResult<ParsedOfferSdp> {
    trace!(sdp = %raw_sdp, "parsing incoming SDP offer");
    let mut media_sections = Vec::new();
    for (index, raw_line) in raw_sdp.lines().enumerate() {
        let line_number = index + 1;
        let line = raw_line.trim_end_matches('\r');
        let Some(media_description) = line.strip_prefix(MEDIA_DESCRIPTION_PREFIX) else {
            continue;
        };
        let media_section =
            parse_media_description_line(raw_sdp, line_number, line, media_description.trim())?;
        media_sections.push(media_section);
    }
    if media_sections.is_empty() {
        let diagnostic = invalid_input(
            "SDP offer did not contain any media description line",
            String::from(MEDIA_DESCRIPTION_PREFIX),
            String::from("no media lines found"),
            None,
            None,
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    Ok(ParsedOfferSdp { media_sections })
}

fn parse_media_description_line(
    raw_sdp: &str,
    line_number: usize,
    line: &str,
    media_description: &str,
) -> SdpParseResult<ParsedMediaSection> {
    let tokens = extract_media_line_tokens(raw_sdp, line_number, line, media_description)?;
    let media_kind = parse_media_kind(
        tokens.media_kind_token,
        raw_sdp,
        line_number,
        line,
        RFC_8866_SECTION_5_14,
    )?;
    let port = parse_port(tokens.port_token, raw_sdp, line_number, line)?;
    let transport_protocol =
        parse_transport_protocol(tokens.transport_protocol_token, raw_sdp, line_number, line)?;
    Ok(ParsedMediaSection {
        media_kind,
        port,
        transport_protocol,
        formats: tokens
            .formats
            .into_iter()
            .map(ToString::to_string)
            .collect(),
    })
}

fn extract_media_line_tokens<'a>(
    raw_sdp: &str,
    line_number: usize,
    line: &'a str,
    media_description: &'a str,
) -> SdpParseResult<MediaLineTokens<'a>> {
    let mut tokens = media_description.split_whitespace();
    let media_kind_token = tokens.next();
    let port_token = tokens.next();
    let transport_protocol_token = tokens.next();
    let formats = tokens.collect::<Vec<_>>();
    let has_minimum_tokens =
        media_kind_token.is_some() && port_token.is_some() && transport_protocol_token.is_some();
    if !has_minimum_tokens || formats.is_empty() {
        let diagnostic = invalid_input(
            "SDP media description line is incomplete",
            String::from(EXPECTED_MEDIA_LINE_FORMAT),
            media_description.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    let Some(media_kind_token) = media_kind_token else {
        let diagnostic = invalid_input(
            "SDP media description line is incomplete",
            String::from(EXPECTED_MEDIA_LINE_FORMAT),
            media_description.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(port_token) = port_token else {
        let diagnostic = invalid_input(
            "SDP media description line is incomplete",
            String::from(EXPECTED_MEDIA_LINE_FORMAT),
            media_description.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(transport_protocol_token) = transport_protocol_token else {
        let diagnostic = invalid_input(
            "SDP media description line is incomplete",
            String::from(EXPECTED_MEDIA_LINE_FORMAT),
            media_description.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    Ok(MediaLineTokens {
        media_kind_token,
        port_token,
        transport_protocol_token,
        formats,
    })
}

fn parse_media_kind(
    token: &str,
    raw_sdp: &str,
    line_number: usize,
    line: &str,
    rfc_reference: SdpRfcReference,
) -> SdpParseResult<ParsedMediaKind> {
    let media_kind = match token {
        MEDIA_KIND_AUDIO => ParsedMediaKind::Audio,
        MEDIA_KIND_VIDEO => ParsedMediaKind::Video,
        MEDIA_KIND_APPLICATION => ParsedMediaKind::Application,
        _ => {
            let diagnostic = unsupported_feature(
                "SDP media kind is valid but not supported yet",
                token,
                line_number,
                line,
                rfc_reference,
                raw_sdp,
            );
            log_diagnostic(&diagnostic);
            return Err(Box::new(diagnostic));
        }
    };
    Ok(media_kind)
}

fn parse_port(token: &str, raw_sdp: &str, line_number: usize, line: &str) -> SdpParseResult<u16> {
    token.parse::<u16>().map_err(|_error| {
        let diagnostic = invalid_input(
            "SDP media description has an invalid port field",
            String::from("0-65535"),
            token.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        log_diagnostic(&diagnostic);
        Box::new(diagnostic)
    })
}

fn parse_transport_protocol(
    token: &str,
    raw_sdp: &str,
    line_number: usize,
    line: &str,
) -> SdpParseResult<ParsedTransportProtocol> {
    let protocol = match token {
        SUPPORTED_TRANSPORT_PROTOCOL => ParsedTransportProtocol::UdpTlsRtpSavpf,
        known_transport if VALID_BUT_UNSUPPORTED_TRANSPORT_PROTOCOLS.contains(&known_transport) => {
            let diagnostic = unsupported_feature(
                "SDP transport protocol is valid but not supported yet",
                token,
                line_number,
                line,
                RFC_8829_SECTION_5_8,
                raw_sdp,
            );
            log_diagnostic(&diagnostic);
            return Err(Box::new(diagnostic));
        }
        _ => {
            let diagnostic = invalid_input(
                "SDP media description has an invalid transport protocol token",
                String::from(SUPPORTED_TRANSPORT_PROTOCOL),
                token.to_owned(),
                Some(line_number),
                Some(line),
                RFC_8829_SECTION_5_8,
                raw_sdp,
            );
            log_diagnostic(&diagnostic);
            return Err(Box::new(diagnostic));
        }
    };
    Ok(protocol)
}

fn invalid_input(
    summary: &'static str,
    expected: String,
    got: String,
    line_number: Option<usize>,
    line: Option<&str>,
    rfc_reference: SdpRfcReference,
    raw_sdp: &str,
) -> SdpParseDiagnostic {
    SdpParseDiagnostic::InvalidInput {
        summary,
        rfc_reference,
        context: Box::new(SdpInvalidContext {
            expected,
            got,
            line_number,
            line: line.map(ToString::to_string),
            raw_sdp: raw_sdp.to_owned(),
        }),
    }
}

fn unsupported_feature(
    summary: &'static str,
    got: &str,
    line_number: usize,
    line: &str,
    rfc_reference: SdpRfcReference,
    raw_sdp: &str,
) -> SdpParseDiagnostic {
    SdpParseDiagnostic::UnsupportedFeature {
        summary,
        rfc_reference,
        context: Box::new(SdpUnsupportedContext {
            got: got.to_owned(),
            line_number,
            line: line.to_owned(),
            raw_sdp: raw_sdp.to_owned(),
        }),
    }
}

fn log_diagnostic(diagnostic: &SdpParseDiagnostic) {
    match diagnostic {
        SdpParseDiagnostic::InvalidInput {
            summary,
            rfc_reference,
            context,
        } => {
            error!(
                summary,
                expected = context.expected,
                got = context.got,
                line_number = context.line_number.map_or(0, |line| line),
                rfc_document = rfc_reference.document(),
                rfc_section = rfc_reference.section(),
                "invalid SDP input"
            );
        }
        SdpParseDiagnostic::UnsupportedFeature {
            summary,
            rfc_reference,
            context,
        } => {
            warn!(
                summary,
                got = context.got,
                line_number = context.line_number,
                line = context.line,
                rfc_document = rfc_reference.document(),
                rfc_section = rfc_reference.section(),
                "unsupported SDP feature"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ParsedMediaKind, ParsedTransportProtocol, SdpDiagnosticKind, SdpParseDiagnostic,
        parse_offer_sdp,
    };

    const VALID_OFFER_SDP: &str = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
t=0 0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
a=mid:1\r\n";

    #[test]
    fn parse_offer_sdp_extracts_supported_media_sections() {
        let result = parse_offer_sdp(VALID_OFFER_SDP);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.media_sections().len(), 2);
        let Some(first_media_section) = parsed.media_sections().first() else {
            return;
        };
        assert_eq!(first_media_section.media_kind(), ParsedMediaKind::Audio);
        assert_eq!(
            first_media_section.transport_protocol(),
            ParsedTransportProtocol::UdpTlsRtpSavpf
        );
        assert_eq!(first_media_section.port(), 9);
        assert_eq!(first_media_section.formats(), ["111".to_owned()]);
        let second_media_section = parsed.media_sections().get(1);
        assert!(second_media_section.is_some());
        let Some(second_media_section) = second_media_section else {
            return;
        };
        assert_eq!(second_media_section.media_kind(), ParsedMediaKind::Video);
    }

    #[test]
    fn parse_offer_sdp_rejects_offer_without_media_lines() {
        let result = parse_offer_sdp("v=0\r\ns=-\r\nt=0 0\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "SDP offer did not contain any media description line"
        );
        assert!(diagnostic.replay_context().contains("v=0"));
    }

    #[test]
    fn parse_offer_sdp_rejects_incomplete_media_description() {
        let result = parse_offer_sdp("m=audio 9 UDP/TLS/RTP/SAVPF\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "SDP media description line is incomplete"
        );
    }

    #[test]
    fn parse_offer_sdp_rejects_invalid_port_field() {
        let result = parse_offer_sdp("m=audio x UDP/TLS/RTP/SAVPF 111\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "SDP media description has an invalid port field"
        );
    }

    #[test]
    fn parse_offer_sdp_marks_known_transport_profile_as_unsupported() {
        let result = parse_offer_sdp("m=audio 9 RTP/SAVPF 111\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "SDP transport protocol is valid but not supported yet"
        );
    }

    #[test]
    fn parse_offer_sdp_rejects_unknown_transport_profile_token_as_invalid() {
        let result = parse_offer_sdp("m=audio 9 INVALID/PROTO 111\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "SDP media description has an invalid transport protocol token"
        );
    }

    #[test]
    fn parse_offer_sdp_marks_unknown_media_kind_as_unsupported() {
        let result = parse_offer_sdp("m=text 9 UDP/TLS/RTP/SAVPF 111\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "SDP media kind is valid but not supported yet"
        );
    }

    #[test]
    fn parse_offer_sdp_preserves_line_context_on_invalid_diagnostic() {
        let result = parse_offer_sdp("m=audio x UDP/TLS/RTP/SAVPF 111\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), SdpDiagnosticKind::InvalidInput);
        let SdpParseDiagnostic::InvalidInput { context, .. } = diagnostic.as_ref() else {
            return;
        };
        assert_eq!(context.line_number(), Some(1));
        assert_eq!(context.line(), Some("m=audio x UDP/TLS/RTP/SAVPF 111"));
    }
}
