#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseDiagnosticKind {
    InvalidInput,
    UnsupportedFeature,
}

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

pub trait ParseDiagnostic {
    fn diagnostic(&self) -> ParseDiagnosticSpec;
}
