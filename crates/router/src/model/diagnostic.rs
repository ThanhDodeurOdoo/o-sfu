//! Structured diagnostics for parse and negotiation failures.

/// High-level failure class used at protocol-sensitive boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseDiagnosticKind {
    InvalidInput,
    UnsupportedFeature,
}

/// RFC rule cited by an internal parse diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RfcReference {
    document: &'static str,
    section: &'static str,
    url: &'static str,
}

impl RfcReference {
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

/// Stable diagnostic payload that callers can log or surface upstream.
///
/// `kind` separates invalid from unsupported input, `summary` is the short
/// operator-facing explanation,
/// `rfc_reference` points to the rule that defined
/// `replay_context` describes the minimum capture needed to
/// reproduce the failure deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDiagnosticSpec {
    kind: ParseDiagnosticKind,
    summary: &'static str,
    rfc_reference: RfcReference,
    replay_context: &'static str,
}

impl ParseDiagnosticSpec {
    #[must_use]
    pub const fn new(
        kind: ParseDiagnosticKind,
        summary: &'static str,
        rfc_reference: RfcReference,
        replay_context: &'static str,
    ) -> Self {
        Self {
            kind,
            summary,
            rfc_reference,
            replay_context,
        }
    }

    #[must_use]
    pub fn kind(&self) -> ParseDiagnosticKind {
        self.kind
    }

    #[must_use]
    pub fn summary(&self) -> &'static str {
        self.summary
    }

    #[must_use]
    pub fn rfc_reference(&self) -> RfcReference {
        self.rfc_reference
    }

    #[must_use]
    pub fn replay_context(&self) -> &'static str {
        self.replay_context
    }
}

/// Trait for errors that can explain themselves with a structured diagnostic.
pub trait ParseDiagnostic {
    fn diagnostic(&self) -> ParseDiagnosticSpec;
}
