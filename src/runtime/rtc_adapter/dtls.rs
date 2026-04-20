use super::parse_diagnostic::{AdapterParseDiagnostic, ParseResult};
use serde::Serialize;
use tracing::{error, trace, warn};

use crate::rfc::webrtc as rfc_webrtc;
use o_sfu_router::RfcReference;

const ROLE_PATH: &str = "$.role";
const FINGERPRINTS_PATH: &str = "$.fingerprints";
const SUPPORTED_FINGERPRINT_ALGORITHM: &str = rfc_webrtc::DtlsFingerprintAlgorithm::Sha256.as_str();
const SUPPORTED_SHA256_FINGERPRINT_BYTE_LEN: usize = 32;
const VALID_BUT_UNSUPPORTED_FINGERPRINT_ALGORITHMS: [&str; 4] =
    ["sha-1", "sha-224", "sha-384", "sha-512"];

const DTLS_REPLAY_CONTEXT_HINT: &str = "raw DTLS parameters JSON payload";

const RFC_5763_SECTION_5: RfcReference = RfcReference::new(
    "RFC 5763",
    "5",
    "https://www.rfc-editor.org/rfc/rfc5763#section-5",
);
const RFC_4572_SECTION_5: RfcReference = RfcReference::new(
    "RFC 4572",
    "5",
    "https://www.rfc-editor.org/rfc/rfc4572#section-5",
);

pub(super) type DtlsParseResult<T> = ParseResult<T, DtlsInvalidContext, DtlsUnsupportedContext>;
pub(super) type DtlsParseDiagnostic =
    AdapterParseDiagnostic<DtlsInvalidContext, DtlsUnsupportedContext>;

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

pub(super) type ParsedDtlsRole = rfc_webrtc::DtlsRole;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct RawDtlsFingerprint {
    pub(super) algorithm: String,
    pub(super) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct RawDtlsParameters {
    pub(super) role: String,
    pub(super) fingerprints: Vec<RawDtlsFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedDtlsFingerprint {
    algorithm: rfc_webrtc::DtlsFingerprintAlgorithm,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedDtlsParameters {
    role: ParsedDtlsRole,
    fingerprints: Vec<ParsedDtlsFingerprint>,
}

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
    raw_dtls_parameters: &RawDtlsParameters,
) -> DtlsParseResult<ParsedDtlsParameters> {
    let raw_json =
        serde_json::to_string(raw_dtls_parameters).unwrap_or_else(|_error| String::from("{}"));
    trace!(
        dtls_parameters = %raw_json,
        "parsing incoming DTLS parameters"
    );
    let role = parse_role(raw_dtls_parameters.role.as_str(), &raw_json)?;

    if raw_dtls_parameters.fingerprints.is_empty() {
        let diagnostic = invalid_input(
            "DTLS fingerprints array cannot be empty",
            String::from("at least one fingerprint"),
            String::from("[]"),
            FINGERPRINTS_PATH,
            RFC_4572_SECTION_5,
            &raw_json,
        );
        return Err(boxed_diagnostic(diagnostic));
    }

    let mut fingerprints = Vec::with_capacity(raw_dtls_parameters.fingerprints.len());
    for fingerprint in &raw_dtls_parameters.fingerprints {
        let fingerprint = parse_fingerprint(fingerprint, &raw_json)?;
        fingerprints.push(fingerprint);
    }

    Ok(ParsedDtlsParameters { role, fingerprints })
}

fn parse_role(role_token: &str, raw_json: &str) -> DtlsParseResult<ParsedDtlsRole> {
    rfc_webrtc::DtlsRole::parse(role_token).ok_or_else(|| {
        let diagnostic = invalid_input(
            "DTLS role is invalid",
            String::from("auto|client|server"),
            role_token.to_owned(),
            ROLE_PATH,
            RFC_5763_SECTION_5,
            raw_json,
        );
        boxed_diagnostic(diagnostic)
    })
}

fn parse_fingerprint(
    fingerprint: &RawDtlsFingerprint,
    raw_json: &str,
) -> DtlsParseResult<ParsedDtlsFingerprint> {
    let algorithm = parse_fingerprint_algorithm(fingerprint.algorithm.as_str(), raw_json)?;
    validate_sha256_fingerprint(fingerprint.value.as_str(), raw_json)?;

    Ok(ParsedDtlsFingerprint {
        algorithm,
        value: fingerprint.value.clone(),
    })
}

fn parse_fingerprint_algorithm(
    algorithm_token: &str,
    raw_json: &str,
) -> DtlsParseResult<rfc_webrtc::DtlsFingerprintAlgorithm> {
    let normalized = algorithm_token.to_ascii_lowercase();
    if let Some(algorithm) = rfc_webrtc::DtlsFingerprintAlgorithm::parse(normalized.as_str()) {
        return Ok(algorithm);
    }
    if VALID_BUT_UNSUPPORTED_FINGERPRINT_ALGORITHMS.contains(&normalized.as_str()) {
        let diagnostic = unsupported_feature(
            "DTLS fingerprint algorithm is valid but not supported yet",
            algorithm_token,
            "$.fingerprints[*].algorithm",
            RFC_4572_SECTION_5,
            raw_json,
        );
        return Err(boxed_diagnostic(diagnostic));
    }
    let diagnostic = invalid_input(
        "DTLS fingerprint algorithm token is invalid",
        String::from(SUPPORTED_FINGERPRINT_ALGORITHM),
        algorithm_token.to_owned(),
        "$.fingerprints[*].algorithm",
        RFC_4572_SECTION_5,
        raw_json,
    );
    Err(boxed_diagnostic(diagnostic))
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
        return Err(boxed_diagnostic(diagnostic));
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
            return Err(boxed_diagnostic(diagnostic));
        }
    }
    Ok(())
}

fn invalid_input(
    summary: &'static str,
    expected: String,
    got: String,
    json_path: &'static str,
    rfc_reference: RfcReference,
    raw_json: &str,
) -> DtlsParseDiagnostic {
    DtlsParseDiagnostic::invalid_input(
        summary,
        rfc_reference,
        DTLS_REPLAY_CONTEXT_HINT,
        DtlsInvalidContext {
            expected,
            got,
            json_path,
            raw_dtls_parameters: raw_json.to_owned(),
        },
        raw_json.to_owned(),
    )
}

fn unsupported_feature(
    summary: &'static str,
    got: &str,
    json_path: &'static str,
    rfc_reference: RfcReference,
    raw_json: &str,
) -> DtlsParseDiagnostic {
    DtlsParseDiagnostic::unsupported_feature(
        summary,
        rfc_reference,
        DTLS_REPLAY_CONTEXT_HINT,
        DtlsUnsupportedContext {
            got: got.to_owned(),
            json_path,
            raw_dtls_parameters: raw_json.to_owned(),
        },
        raw_json.to_owned(),
    )
}

fn boxed_diagnostic(diagnostic: DtlsParseDiagnostic) -> Box<DtlsParseDiagnostic> {
    log_diagnostic(&diagnostic);
    Box::new(diagnostic)
}

fn log_diagnostic(diagnostic: &DtlsParseDiagnostic) {
    match diagnostic {
        DtlsParseDiagnostic::InvalidInput { context, .. } => {
            error!(
                summary = diagnostic.summary(),
                expected = context.expected,
                got = context.got,
                json_path = context.json_path,
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                "invalid DTLS parameters"
            );
        }
        DtlsParseDiagnostic::UnsupportedFeature { context, .. } => {
            warn!(
                summary = diagnostic.summary(),
                got = context.got,
                json_path = context.json_path,
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                "unsupported DTLS feature"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_router::ParseDiagnosticKind;

    use super::{ParsedDtlsRole, RawDtlsFingerprint, RawDtlsParameters, parse_dtls_parameters};

    const VALID_SHA256_FINGERPRINT: &str = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99";

    fn sample_dtls_parameters(role: &str, algorithm: &str, value: &str) -> RawDtlsParameters {
        RawDtlsParameters {
            role: role.to_owned(),
            fingerprints: vec![RawDtlsFingerprint {
                algorithm: algorithm.to_owned(),
                value: value.to_owned(),
            }],
        }
    }

    #[test]
    fn parse_dtls_parameters_accepts_valid_sha256_payload() {
        let dtls_parameters = sample_dtls_parameters("client", "sha-256", VALID_SHA256_FINGERPRINT);
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_ok());
        let Some(parsed) = result.ok() else {
            return;
        };
        assert_eq!(parsed.role(), ParsedDtlsRole::Client);
        assert_eq!(parsed.fingerprints().len(), 1);
    }

    #[test]
    fn parse_dtls_parameters_rejects_empty_fingerprints_array() {
        let dtls_parameters = RawDtlsParameters {
            role: String::from("client"),
            fingerprints: vec![],
        };
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "DTLS fingerprints array cannot be empty"
        );
    }

    #[test]
    fn parse_dtls_parameters_rejects_unknown_role() {
        let dtls_parameters =
            sample_dtls_parameters("passive", "sha-256", VALID_SHA256_FINGERPRINT);
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
        assert_eq!(diagnostic.summary(), "DTLS role is invalid");
    }

    #[test]
    fn parse_dtls_parameters_marks_sha1_as_unsupported() {
        let dtls_parameters = sample_dtls_parameters(
            "client",
            "sha-1",
            "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD",
        );
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
        assert_eq!(
            diagnostic.summary(),
            "DTLS fingerprint algorithm is valid but not supported yet"
        );
    }

    #[test]
    fn parse_dtls_parameters_rejects_malformed_fingerprint() {
        let dtls_parameters = sample_dtls_parameters("client", "sha-256", "AA:BB:CC");
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
        assert_eq!(
            diagnostic.summary(),
            "DTLS fingerprint length is invalid for sha-256"
        );
    }

    #[test]
    fn parse_dtls_parameters_preserves_replay_context() {
        let dtls_parameters = sample_dtls_parameters("client", "sha-256", "ZZ");
        let result = parse_dtls_parameters(&dtls_parameters);
        assert!(result.is_err());
        let Some(diagnostic) = result.err() else {
            return;
        };
        assert!(diagnostic.replay_context().contains("\"role\":\"client\""));
    }
}
