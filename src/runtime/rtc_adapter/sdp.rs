use super::parse_diagnostic::{AdapterParseDiagnostic, ParseResult};
use crate::rfc::webrtc;
use o_sfu_router::RfcReference;
use tracing::{error, trace, warn};

const MEDIA_DESCRIPTION_PREFIX: &str = "m=";
const VALID_BUT_UNSUPPORTED_TRANSPORT_PROTOCOLS: [&str; 5] = [
    webrtc::sdp::transport_protocol::UDP_TLS_RTP_SAVP,
    webrtc::sdp::transport_protocol::RTP_SAVPF,
    webrtc::sdp::transport_protocol::RTP_SAVP,
    webrtc::sdp::transport_protocol::UDP_DTLS_SCTP,
    webrtc::sdp::transport_protocol::TCP_DTLS_SCTP,
];
const EXPECTED_MEDIA_LINE_FORMAT: &str = "<media> <port> <proto> <fmt>...";

const SDP_REPLAY_CONTEXT_HINT: &str = "raw SDP offer payload";

const RFC_8866_SECTION_5_14: RfcReference = RfcReference::new(
    "RFC 8866",
    "5.14",
    "https://www.rfc-editor.org/rfc/rfc8866#section-5.14",
);
const RFC_8829_SECTION_5_8: RfcReference = RfcReference::new(
    "RFC 8829",
    "5.8",
    "https://www.rfc-editor.org/rfc/rfc8829#section-5.8",
);

pub(super) type SdpParseResult<T> = ParseResult<T, SdpInvalidContext, SdpUnsupportedContext>;
pub(super) type SdpParseDiagnostic =
    AdapterParseDiagnostic<SdpInvalidContext, SdpUnsupportedContext>;

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
    pub(super) fn expected(&self) -> &str {
        &self.expected
    }

    #[must_use]
    pub(super) fn got(&self) -> &str {
        &self.got
    }

    #[must_use]
    pub(super) fn line_number(&self) -> Option<usize> {
        self.line_number
    }

    #[must_use]
    pub(super) fn line(&self) -> Option<&str> {
        self.line.as_deref()
    }
}

impl SdpUnsupportedContext {
    #[must_use]
    pub(super) fn got(&self) -> &str {
        &self.got
    }

    #[must_use]
    pub(super) fn line_number(&self) -> usize {
        self.line_number
    }

    #[must_use]
    pub(super) fn line(&self) -> &str {
        &self.line
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
        return Err(boxed_diagnostic(diagnostic));
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
    let media_kind_token =
        required_media_line_token(tokens.next(), raw_sdp, line_number, line, media_description)?;
    let port_token =
        required_media_line_token(tokens.next(), raw_sdp, line_number, line, media_description)?;
    let transport_protocol_token =
        required_media_line_token(tokens.next(), raw_sdp, line_number, line, media_description)?;
    let formats = tokens.collect::<Vec<_>>();
    if formats.is_empty() {
        let diagnostic = invalid_input(
            "SDP media description line is incomplete",
            String::from(EXPECTED_MEDIA_LINE_FORMAT),
            media_description.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        );
        return Err(boxed_diagnostic(diagnostic));
    }
    Ok(MediaLineTokens {
        media_kind_token,
        port_token,
        transport_protocol_token,
        formats,
    })
}

fn required_media_line_token<'a>(
    token: Option<&'a str>,
    raw_sdp: &str,
    line_number: usize,
    line: &'a str,
    media_description: &'a str,
) -> SdpParseResult<&'a str> {
    token.ok_or_else(|| {
        boxed_diagnostic(invalid_input(
            "SDP media description line is incomplete",
            String::from(EXPECTED_MEDIA_LINE_FORMAT),
            media_description.to_owned(),
            Some(line_number),
            Some(line),
            RFC_8866_SECTION_5_14,
            raw_sdp,
        ))
    })
}

fn parse_media_kind(
    token: &str,
    raw_sdp: &str,
    line_number: usize,
    line: &str,
    rfc_reference: RfcReference,
) -> SdpParseResult<ParsedMediaKind> {
    let media_kind = match token {
        webrtc::media_kind::AUDIO => ParsedMediaKind::Audio,
        webrtc::media_kind::VIDEO => ParsedMediaKind::Video,
        webrtc::media_kind::APPLICATION => ParsedMediaKind::Application,
        _ => {
            let diagnostic = unsupported_feature(
                "SDP media kind is valid but not supported yet",
                token,
                line_number,
                line,
                rfc_reference,
                raw_sdp,
            );
            return Err(boxed_diagnostic(diagnostic));
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
        boxed_diagnostic(diagnostic)
    })
}

fn parse_transport_protocol(
    token: &str,
    raw_sdp: &str,
    line_number: usize,
    line: &str,
) -> SdpParseResult<ParsedTransportProtocol> {
    let protocol = match token {
        webrtc::sdp::transport_protocol::UDP_TLS_RTP_SAVPF => {
            ParsedTransportProtocol::UdpTlsRtpSavpf
        }
        known_transport if VALID_BUT_UNSUPPORTED_TRANSPORT_PROTOCOLS.contains(&known_transport) => {
            let diagnostic = unsupported_feature(
                "SDP transport protocol is valid but not supported yet",
                token,
                line_number,
                line,
                RFC_8829_SECTION_5_8,
                raw_sdp,
            );
            return Err(boxed_diagnostic(diagnostic));
        }
        _ => {
            let diagnostic = invalid_input(
                "SDP media description has an invalid transport protocol token",
                String::from(webrtc::sdp::transport_protocol::UDP_TLS_RTP_SAVPF),
                token.to_owned(),
                Some(line_number),
                Some(line),
                RFC_8829_SECTION_5_8,
                raw_sdp,
            );
            return Err(boxed_diagnostic(diagnostic));
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
    rfc_reference: RfcReference,
    raw_sdp: &str,
) -> SdpParseDiagnostic {
    SdpParseDiagnostic::invalid_input(
        summary,
        rfc_reference,
        SDP_REPLAY_CONTEXT_HINT,
        SdpInvalidContext {
            expected,
            got,
            line_number,
            line: line.map(ToString::to_string),
            raw_sdp: raw_sdp.to_owned(),
        },
        raw_sdp.to_owned(),
    )
}

fn unsupported_feature(
    summary: &'static str,
    got: &str,
    line_number: usize,
    line: &str,
    rfc_reference: RfcReference,
    raw_sdp: &str,
) -> SdpParseDiagnostic {
    SdpParseDiagnostic::unsupported_feature(
        summary,
        rfc_reference,
        SDP_REPLAY_CONTEXT_HINT,
        SdpUnsupportedContext {
            got: got.to_owned(),
            line_number,
            line: line.to_owned(),
            raw_sdp: raw_sdp.to_owned(),
        },
        raw_sdp.to_owned(),
    )
}

fn boxed_diagnostic(diagnostic: SdpParseDiagnostic) -> Box<SdpParseDiagnostic> {
    log_diagnostic(&diagnostic);
    Box::new(diagnostic)
}

fn log_diagnostic(diagnostic: &SdpParseDiagnostic) {
    match diagnostic {
        SdpParseDiagnostic::InvalidInput { context, .. } => {
            error!(
                summary = diagnostic.summary(),
                expected = context.expected,
                got = context.got,
                line_number = context.line_number.map_or(0, |line| line),
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                "invalid SDP input"
            );
        }
        SdpParseDiagnostic::UnsupportedFeature { context, .. } => {
            warn!(
                summary = diagnostic.summary(),
                got = context.got,
                line_number = context.line_number,
                line = context.line,
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                "unsupported SDP feature"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_router::ParseDiagnosticKind;

    use super::{ParsedMediaKind, ParsedTransportProtocol, SdpParseDiagnostic, parse_offer_sdp};

    const FIREFOX_OFFER_AUDIO_ONLY: &str = include_str!("testdata/firefox_offer_audio_only.sdp");
    const CHROME_OFFER_AUDIO_ONLY: &str = include_str!("testdata/chrome_offer_audio_only.sdp");
    const SAFARI_DATA_CHANNEL_OFFER: &str = include_str!("testdata/safari_datachannel_offer.sdp");

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
    fn parse_offer_sdp_accepts_firefox_offer_fixture() {
        let result = parse_offer_sdp(FIREFOX_OFFER_AUDIO_ONLY);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.media_sections().len(), 1);
        let Some(media_section) = parsed.media_sections().first() else {
            return;
        };
        assert_eq!(media_section.media_kind(), ParsedMediaKind::Audio);
        assert_eq!(media_section.port(), 9);
        assert_eq!(
            media_section.transport_protocol(),
            ParsedTransportProtocol::UdpTlsRtpSavpf
        );
        assert_eq!(
            media_section.formats(),
            [
                "109".to_owned(),
                "9".to_owned(),
                "0".to_owned(),
                "8".to_owned(),
                "101".to_owned()
            ]
        );
    }

    #[test]
    fn parse_offer_sdp_accepts_chrome_offer_fixture() {
        let result = parse_offer_sdp(CHROME_OFFER_AUDIO_ONLY);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.media_sections().len(), 1);
        let Some(media_section) = parsed.media_sections().first() else {
            return;
        };
        assert_eq!(media_section.media_kind(), ParsedMediaKind::Audio);
        assert_eq!(media_section.port(), 9);
        assert_eq!(
            media_section.transport_protocol(),
            ParsedTransportProtocol::UdpTlsRtpSavpf
        );
        assert_eq!(media_section.formats().first(), Some(&"111".to_owned()));
        assert_eq!(media_section.formats().last(), Some(&"126".to_owned()));
    }

    #[test]
    fn parse_offer_sdp_marks_safari_datachannel_offer_as_unsupported_fixture() {
        let result = parse_offer_sdp(SAFARI_DATA_CHANNEL_OFFER);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "SDP transport protocol is valid but not supported yet"
        );
        let SdpParseDiagnostic::UnsupportedFeature { context, .. } = diagnostic.as_ref() else {
            return;
        };
        assert_eq!(context.got(), "UDP/DTLS/SCTP");
        assert_eq!(context.line_number(), 8);
        assert!(context.line().contains("webrtc-datachannel"));
    }

    #[test]
    fn parse_offer_sdp_rejects_offer_without_media_lines() {
        let result = parse_offer_sdp("v=0\r\ns=-\r\nt=0 0\r\n");
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
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
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
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
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
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
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
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
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
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
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
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
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
        let SdpParseDiagnostic::InvalidInput { context, .. } = diagnostic.as_ref() else {
            return;
        };
        assert_eq!(context.line_number(), Some(1));
        assert_eq!(context.line(), Some("m=audio x UDP/TLS/RTP/SAVPF 111"));
    }
}
