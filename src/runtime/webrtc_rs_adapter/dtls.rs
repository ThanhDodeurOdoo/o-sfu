use tracing::{error, trace, warn};

use crate::signaling::webrtc::DtlsParameters;

const ROLE_PATH: &str = "$.role";
const FINGERPRINTS_PATH: &str = "$.fingerprints";
const SUPPORTED_FINGERPRINT_ALGORITHM: &str = "sha-256";
const SUPPORTED_SHA256_FINGERPRINT_BYTE_LEN: usize = 32;
const VALID_BUT_UNSUPPORTED_FINGERPRINT_ALGORITHMS: [&str; 4] =
    ["sha-1", "sha-224", "sha-384", "sha-512"];

const RFC_5763_SECTION_5: DtlsRfcReference = DtlsRfcReference::new(
    "RFC 5763",
    "5",
    "https://www.rfc-editor.org/rfc/rfc5763#section-5",
);
const RFC_4572_SECTION_5: DtlsRfcReference = DtlsRfcReference::new(
    "RFC 4572",
    "5",
    "https://www.rfc-editor.org/rfc/rfc4572#section-5",
);

pub(super) type DtlsParseResult<T> = Result<T, Box<DtlsParseDiagnostic>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DtlsDiagnosticKind {
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DtlsRfcReference {
    document: &'static str,
    section: &'static str,
    url: &'static str,
}

impl DtlsRfcReference {
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
pub(super) struct DtlsInvalidContext {
    expected: String,
    got: String,
    json_path: &'static str,
    raw_dtls_parameters: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DtlsUnsupportedContext {
    got: String,
    json_path: &'static str,
    raw_dtls_parameters: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DtlsParseDiagnostic {
    InvalidInput {
        summary: &'static str,
        rfc_reference: DtlsRfcReference,
        context: Box<DtlsInvalidContext>,
    },
    UnsupportedFeature {
        summary: &'static str,
        rfc_reference: DtlsRfcReference,
        context: Box<DtlsUnsupportedContext>,
    },
}

impl DtlsParseDiagnostic {
    #[must_use]
    pub(super) fn kind(&self) -> DtlsDiagnosticKind {
        match self {
            Self::InvalidInput { .. } => DtlsDiagnosticKind::InvalidInput,
            Self::UnsupportedFeature { .. } => DtlsDiagnosticKind::UnsupportedFeature,
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
    pub(super) fn rfc_reference(&self) -> DtlsRfcReference {
        match self {
            Self::InvalidInput { rfc_reference, .. }
            | Self::UnsupportedFeature { rfc_reference, .. } => *rfc_reference,
        }
    }

    #[must_use]
    pub(super) fn replay_context(&self) -> &str {
        match self {
            Self::InvalidInput { context, .. } => &context.raw_dtls_parameters,
            Self::UnsupportedFeature { context, .. } => &context.raw_dtls_parameters,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParsedDtlsRole {
    Auto,
    Client,
    Server,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedDtlsFingerprint {
    algorithm: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedDtlsParameters {
    role: ParsedDtlsRole,
    fingerprints: Vec<ParsedDtlsFingerprint>,
}

#[cfg(test)]
impl ParsedDtlsParameters {
    #[must_use]
    pub(super) fn role(&self) -> ParsedDtlsRole {
        self.role
    }

    #[must_use]
    pub(super) fn fingerprints(&self) -> &[ParsedDtlsFingerprint] {
        &self.fingerprints
    }
}

pub(super) fn parse_dtls_parameters(
    raw_dtls_parameters: &DtlsParameters,
) -> DtlsParseResult<ParsedDtlsParameters> {
    trace!(
        dtls_parameters = %raw_dtls_parameters.0,
        "parsing incoming DTLS parameters"
    );
    let raw_json = raw_dtls_parameters.0.to_string();
    let Some(dtls) = raw_dtls_parameters.0.as_object() else {
        let diagnostic = invalid_input(
            "DTLS parameters payload must be a JSON object",
            String::from("object"),
            raw_dtls_parameters.0.to_string(),
            "$",
            RFC_5763_SECTION_5,
            &raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };

    let Some(role_value) = dtls.get("role") else {
        let diagnostic = invalid_input(
            "DTLS role is missing",
            String::from("auto|client|server"),
            String::from("<missing>"),
            ROLE_PATH,
            RFC_5763_SECTION_5,
            &raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(role_token) = role_value.as_str() else {
        let diagnostic = invalid_input(
            "DTLS role must be a string",
            String::from("auto|client|server"),
            role_value.to_string(),
            ROLE_PATH,
            RFC_5763_SECTION_5,
            &raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let role = parse_role(role_token, &raw_json)?;

    let Some(fingerprints_value) = dtls.get("fingerprints") else {
        let diagnostic = invalid_input(
            "DTLS fingerprints array is missing",
            String::from("non-empty array"),
            String::from("<missing>"),
            FINGERPRINTS_PATH,
            RFC_4572_SECTION_5,
            &raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(fingerprints_array) = fingerprints_value.as_array() else {
        let diagnostic = invalid_input(
            "DTLS fingerprints must be an array",
            String::from("non-empty array"),
            fingerprints_value.to_string(),
            FINGERPRINTS_PATH,
            RFC_4572_SECTION_5,
            &raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    if fingerprints_array.is_empty() {
        let diagnostic = invalid_input(
            "DTLS fingerprints array cannot be empty",
            String::from("at least one fingerprint"),
            String::from("[]"),
            FINGERPRINTS_PATH,
            RFC_4572_SECTION_5,
            &raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }

    let mut fingerprints = Vec::with_capacity(fingerprints_array.len());
    for fingerprint_value in fingerprints_array {
        let fingerprint = parse_fingerprint(fingerprint_value, &raw_json)?;
        fingerprints.push(fingerprint);
    }

    Ok(ParsedDtlsParameters { role, fingerprints })
}

fn parse_role(role_token: &str, raw_json: &str) -> DtlsParseResult<ParsedDtlsRole> {
    let role = match role_token {
        "auto" => ParsedDtlsRole::Auto,
        "client" => ParsedDtlsRole::Client,
        "server" => ParsedDtlsRole::Server,
        _ => {
            let diagnostic = invalid_input(
                "DTLS role is invalid",
                String::from("auto|client|server"),
                role_token.to_owned(),
                ROLE_PATH,
                RFC_5763_SECTION_5,
                raw_json,
            );
            log_diagnostic(&diagnostic);
            return Err(Box::new(diagnostic));
        }
    };
    Ok(role)
}

fn parse_fingerprint(
    fingerprint_value: &serde_json::Value,
    raw_json: &str,
) -> DtlsParseResult<ParsedDtlsFingerprint> {
    let Some(fingerprint) = fingerprint_value.as_object() else {
        let diagnostic = invalid_input(
            "DTLS fingerprint entry must be an object",
            String::from("{algorithm,value}"),
            fingerprint_value.to_string(),
            FINGERPRINTS_PATH,
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };

    let Some(algorithm_value) = fingerprint.get("algorithm") else {
        let diagnostic = invalid_input(
            "DTLS fingerprint algorithm is missing",
            String::from("sha-256"),
            String::from("<missing>"),
            "$.fingerprints[*].algorithm",
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(algorithm_token) = algorithm_value.as_str() else {
        let diagnostic = invalid_input(
            "DTLS fingerprint algorithm must be a string",
            String::from("sha-256"),
            algorithm_value.to_string(),
            "$.fingerprints[*].algorithm",
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let algorithm = parse_fingerprint_algorithm(algorithm_token, raw_json)?;

    let Some(value_value) = fingerprint.get("value") else {
        let diagnostic = invalid_input(
            "DTLS fingerprint value is missing",
            String::from("colon-separated uppercase hex pairs"),
            String::from("<missing>"),
            "$.fingerprints[*].value",
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    let Some(value_token) = value_value.as_str() else {
        let diagnostic = invalid_input(
            "DTLS fingerprint value must be a string",
            String::from("colon-separated uppercase hex pairs"),
            value_value.to_string(),
            "$.fingerprints[*].value",
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    };
    validate_sha256_fingerprint(value_token, raw_json)?;

    Ok(ParsedDtlsFingerprint {
        algorithm,
        value: value_token.to_owned(),
    })
}

fn parse_fingerprint_algorithm(algorithm_token: &str, raw_json: &str) -> DtlsParseResult<String> {
    let normalized = algorithm_token.to_ascii_lowercase();
    if normalized == SUPPORTED_FINGERPRINT_ALGORITHM {
        return Ok(normalized);
    }
    if VALID_BUT_UNSUPPORTED_FINGERPRINT_ALGORITHMS.contains(&normalized.as_str()) {
        let diagnostic = unsupported_feature(
            "DTLS fingerprint algorithm is valid but not supported yet",
            algorithm_token,
            "$.fingerprints[*].algorithm",
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    let diagnostic = invalid_input(
        "DTLS fingerprint algorithm token is invalid",
        String::from("sha-256"),
        algorithm_token.to_owned(),
        "$.fingerprints[*].algorithm",
        RFC_4572_SECTION_5,
        raw_json,
    );
    log_diagnostic(&diagnostic);
    Err(Box::new(diagnostic))
}

fn validate_sha256_fingerprint(value_token: &str, raw_json: &str) -> DtlsParseResult<()> {
    let segments = value_token.split(':').collect::<Vec<_>>();
    if segments.len() != SUPPORTED_SHA256_FINGERPRINT_BYTE_LEN {
        let diagnostic = invalid_input(
            "DTLS fingerprint length is invalid for sha-256",
            String::from("32 colon-separated hexadecimal bytes"),
            value_token.to_owned(),
            "$.fingerprints[*].value",
            RFC_4572_SECTION_5,
            raw_json,
        );
        log_diagnostic(&diagnostic);
        return Err(Box::new(diagnostic));
    }
    for segment in segments {
        let is_hex_byte =
            segment.len() == 2 && segment.chars().all(|char| char.is_ascii_hexdigit());
        if !is_hex_byte {
            let diagnostic = invalid_input(
                "DTLS fingerprint segment is not a hexadecimal byte",
                String::from("two hexadecimal characters"),
                segment.to_owned(),
                "$.fingerprints[*].value",
                RFC_4572_SECTION_5,
                raw_json,
            );
            log_diagnostic(&diagnostic);
            return Err(Box::new(diagnostic));
        }
    }
    Ok(())
}

fn invalid_input(
    summary: &'static str,
    expected: String,
    got: String,
    json_path: &'static str,
    rfc_reference: DtlsRfcReference,
    raw_json: &str,
) -> DtlsParseDiagnostic {
    DtlsParseDiagnostic::InvalidInput {
        summary,
        rfc_reference,
        context: Box::new(DtlsInvalidContext {
            expected,
            got,
            json_path,
            raw_dtls_parameters: raw_json.to_owned(),
        }),
    }
}

fn unsupported_feature(
    summary: &'static str,
    got: &str,
    json_path: &'static str,
    rfc_reference: DtlsRfcReference,
    raw_json: &str,
) -> DtlsParseDiagnostic {
    DtlsParseDiagnostic::UnsupportedFeature {
        summary,
        rfc_reference,
        context: Box::new(DtlsUnsupportedContext {
            got: got.to_owned(),
            json_path,
            raw_dtls_parameters: raw_json.to_owned(),
        }),
    }
}

fn log_diagnostic(diagnostic: &DtlsParseDiagnostic) {
    match diagnostic {
        DtlsParseDiagnostic::InvalidInput {
            summary,
            rfc_reference,
            context,
        } => {
            error!(
                summary,
                expected = context.expected,
                got = context.got,
                json_path = context.json_path,
                rfc_document = rfc_reference.document(),
                rfc_section = rfc_reference.section(),
                "invalid DTLS parameters"
            );
        }
        DtlsParseDiagnostic::UnsupportedFeature {
            summary,
            rfc_reference,
            context,
        } => {
            warn!(
                summary,
                got = context.got,
                json_path = context.json_path,
                rfc_document = rfc_reference.document(),
                rfc_section = rfc_reference.section(),
                "unsupported DTLS feature"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DtlsDiagnosticKind, ParsedDtlsRole, parse_dtls_parameters};
    use crate::signaling::webrtc::DtlsParameters;

    const VALID_SHA256_FINGERPRINT: &str = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99";

    #[test]
    fn parse_dtls_parameters_accepts_valid_sha256_payload() {
        let dtls_parameters = DtlsParameters(json!({
            "role": "client",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": VALID_SHA256_FINGERPRINT,
            }]
        }));
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.role(), ParsedDtlsRole::Client);
        assert_eq!(parsed.fingerprints().len(), 1);
    }

    #[test]
    fn parse_dtls_parameters_rejects_non_object_payload() {
        let dtls_parameters = DtlsParameters(json!(true));
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), DtlsDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "DTLS parameters payload must be a JSON object"
        );
    }

    #[test]
    fn parse_dtls_parameters_rejects_unknown_role() {
        let dtls_parameters = DtlsParameters(json!({
            "role": "passive",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": VALID_SHA256_FINGERPRINT,
            }]
        }));
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), DtlsDiagnosticKind::InvalidInput);
        assert_eq!(diagnostic.summary(), "DTLS role is invalid");
    }

    #[test]
    fn parse_dtls_parameters_marks_sha1_as_unsupported() {
        let dtls_parameters = DtlsParameters(json!({
            "role": "client",
            "fingerprints": [{
                "algorithm": "sha-1",
                "value": "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD",
            }]
        }));
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), DtlsDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "DTLS fingerprint algorithm is valid but not supported yet"
        );
    }

    #[test]
    fn parse_dtls_parameters_rejects_malformed_fingerprint() {
        let dtls_parameters = DtlsParameters(json!({
            "role": "client",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": "AA:BB:CC",
            }]
        }));
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), DtlsDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "DTLS fingerprint length is invalid for sha-256"
        );
    }

    #[test]
    fn parse_dtls_parameters_preserves_replay_context() {
        let dtls_parameters = DtlsParameters(json!({
            "role": "client",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": "ZZ",
            }]
        }));
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert!(diagnostic.replay_context().contains("\"role\":\"client\""));
    }
}
