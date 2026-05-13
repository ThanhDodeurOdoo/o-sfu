//! RFC references for this module:
//! - JSON Web Token (JWT): <https://www.rfc-editor.org/rfc/rfc7519>
//! - JSON Web Algorithms (JWA): <https://www.rfc-editor.org/rfc/rfc7518>

pub use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

/// JWT `typ` header value.
///
/// Reference: RFC 7519 section 5.1.
pub const TYPE_JWT: &str = "JWT";

/// JWS `alg` header value for HMAC using SHA-256.
///
/// Reference: RFC 7518 section 3.2.
pub const ALGORITHM_HS256: &str = "HS256";

/// Registered JWT claims from RFC 7519 section 4.1.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredJwtClaims {
    /// Expiration time (in seconds since epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
    /// Issued at (in seconds since epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    /// Not before (in seconds since epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    /// Issuer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    /// Subject
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    /// Audience
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<JwtAudience>,
    /// JWT ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jti: Option<String>,
}

/// JWT `aud` claim.
///
/// RFC 7519 allows the audience claim to be either one case-sensitive string
/// or an array of case-sensitive strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JwtAudience {
    Single(String),
    Multiple(Vec<String>),
}

/// JOSE header fields used by `o-sfu`'s JWT handling.
///
/// Reference: RFC 7519 section 5.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtHeader {
    pub alg: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typ: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{JwtAudience, RegisteredJwtClaims};

    #[test]
    fn registered_claims_support_single_audience() -> serde_json::Result<()> {
        let claims = RegisteredJwtClaims {
            aud: Some(JwtAudience::Single("urn:odoo:sfu".to_owned())),
            ..RegisteredJwtClaims::default()
        };
        assert_eq!(
            serde_json::to_value(&claims)?,
            json!({ "aud": "urn:odoo:sfu" })
        );
        Ok(())
    }

    #[test]
    fn registered_claims_support_multiple_audiences() -> serde_json::Result<()> {
        let claims = RegisteredJwtClaims {
            aud: Some(JwtAudience::Multiple(vec![
                "urn:odoo:sfu".to_owned(),
                "urn:odoo:recording".to_owned(),
            ])),
            ..RegisteredJwtClaims::default()
        };
        assert_eq!(
            serde_json::to_value(&claims)?,
            json!({ "aud": ["urn:odoo:sfu", "urn:odoo:recording"] })
        );
        Ok(())
    }
}
