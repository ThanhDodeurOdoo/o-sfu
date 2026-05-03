use o_sfu_router::{ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference};

pub(super) type ParseResult<T, InvalidContext, UnsupportedContext> =
    Result<T, Box<AdapterParseDiagnostic<InvalidContext, UnsupportedContext>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AdapterParseDiagnostic<InvalidContext, UnsupportedContext> {
    InvalidInput {
        spec: ParseDiagnosticSpec,
        context: Box<InvalidContext>,
        replay_context: String,
    },
    UnsupportedFeature {
        spec: ParseDiagnosticSpec,
        context: Box<UnsupportedContext>,
        replay_context: String,
    },
}

impl<InvalidContext, UnsupportedContext>
    AdapterParseDiagnostic<InvalidContext, UnsupportedContext>
{
    #[must_use]
    pub(super) fn invalid_input(
        summary: &'static str,
        rfc_reference: RfcReference,
        replay_context_hint: &'static str,
        context: InvalidContext,
        replay_context: String,
    ) -> Self {
        Self::InvalidInput {
            spec: ParseDiagnosticSpec::new(
                ParseDiagnosticKind::InvalidInput,
                summary,
                rfc_reference,
                replay_context_hint,
            ),
            context: Box::new(context),
            replay_context,
        }
    }

    #[must_use]
    pub(super) fn unsupported_feature(
        summary: &'static str,
        rfc_reference: RfcReference,
        replay_context_hint: &'static str,
        context: UnsupportedContext,
        replay_context: String,
    ) -> Self {
        Self::UnsupportedFeature {
            spec: ParseDiagnosticSpec::new(
                ParseDiagnosticKind::UnsupportedFeature,
                summary,
                rfc_reference,
                replay_context_hint,
            ),
            context: Box::new(context),
            replay_context,
        }
    }

    #[must_use]
    pub(super) fn kind(&self) -> ParseDiagnosticKind {
        self.spec().kind()
    }

    #[must_use]
    pub(super) fn summary(&self) -> &'static str {
        self.spec().summary()
    }

    #[must_use]
    pub(super) fn rfc_reference(&self) -> RfcReference {
        self.spec().rfc_reference()
    }

    #[must_use]
    pub(super) fn replay_context(&self) -> &str {
        match self {
            Self::InvalidInput { replay_context, .. }
            | Self::UnsupportedFeature { replay_context, .. } => replay_context,
        }
    }

    #[must_use]
    fn spec(&self) -> ParseDiagnosticSpec {
        match self {
            Self::InvalidInput { spec, .. } | Self::UnsupportedFeature { spec, .. } => *spec,
        }
    }
}
