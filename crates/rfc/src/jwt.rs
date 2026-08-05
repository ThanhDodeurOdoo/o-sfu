//! RFC references for this module:
//! - JSON Web Token (JWT): <https://www.rfc-editor.org/rfc/rfc7519>
//! - JSON Web Algorithms (JWA): <https://www.rfc-editor.org/rfc/rfc7518>

use std::{fmt, time::Duration};

pub use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
};

/// JWT `typ` header value.
///
/// Reference: RFC 7519 section 5.1.
pub const TYPE_JWT: &str = "JWT";

/// JWS `alg` header value for HMAC using SHA-256.
///
/// Reference: RFC 7518 section 3.2.
pub const ALGORITHM_HS256: &str = "HS256";

/// minimum HS256 key length from RFC 7518 section 3.2
pub const HS256_MIN_KEY_BYTES: usize = 32;

/// RFC 7519 seconds since the Unix epoch with subsecond precision.
///
/// Both encodings are JSON numbers. Whole seconds serialize through `u64` and
/// fractional values serialize through `f64`.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NumericDate(Duration);

impl From<u64> for NumericDate {
    fn from(seconds: u64) -> Self {
        Self(Duration::from_secs(seconds))
    }
}

impl From<Duration> for NumericDate {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl Serialize for NumericDate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.0.subsec_nanos() == 0 {
            serializer.serialize_u64(self.0.as_secs())
        } else {
            serializer.serialize_f64(self.0.as_secs_f64())
        }
    }
}

impl<'de> Deserialize<'de> for NumericDate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(NumericDateVisitor)
    }
}

struct NumericDateVisitor;

impl Visitor<'_> for NumericDateVisitor {
    type Value = NumericDate;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a nonnegative RFC 7519 NumericDate")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(NumericDate::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Duration::try_from_secs_f64(value)
            .map(NumericDate)
            .map_err(|_error| E::custom("NumericDate must be nonnegative, finite and in range"))
    }
}

/// Registered JWT claims from RFC 7519 section 4.1.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredJwtClaims {
    /// expiration time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<NumericDate>,
    /// issued at time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<NumericDate>,
    /// not before time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<NumericDate>,
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
#[path = "TESTS/jwt.rs"]
mod tests;
