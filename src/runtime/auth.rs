use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE},
};
use hmac::{Hmac, KeyInit, Mac};
use o_sfu_protocol::wire::{UserId, UserPermissions};
pub use o_sfu_rfc::jwt::RegisteredJwtClaims;
use o_sfu_rfc::jwt::{ALGORITHM_HS256, JwtHeader, TYPE_JWT, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

pub const MAX_JWT_TOKEN_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthenticationError {
    #[error("invalid JWT format")]
    InvalidJwtFormat,
    #[error("JWT token exceeds maximum byte length")]
    TokenTooLarge { actual: usize, limit: usize },
    #[error("invalid base64 encoding")]
    InvalidBase64Encoding,
    #[error("invalid JSON payload")]
    InvalidJsonPayload,
    #[error("unsupported JWT algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("invalid JWT signature")]
    InvalidSignature,
    #[error("token expired")]
    TokenExpired,
    #[error("token not valid yet")]
    TokenNotYetValid,
    #[error("token issued in the future")]
    TokenIssuedInFuture,
}

/// Local skew guard for `iat`.
///
/// RFC 7519 defines `iat` as an informational registered claim, so this
/// tolerance remains a runtime hardening policy rather than an RFC-mandated
/// validity rule.
const MAX_IAT_FUTURE_SKEW_SECONDS: u64 = 60;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRoomClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpDisconnectClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(rename = "userIdsByRoom", alias = "sessionIdsByChannel")]
    pub user_ids_by_room: BTreeMap<String, Vec<UserId>>,
}

impl HttpDisconnectClaims {
    pub fn normalize_runtime_user_ids(&mut self) {
        for user_ids in self.user_ids_by_room.values_mut() {
            for user_id in user_ids {
                *user_id = user_id.runtime_normalized();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketConnectClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(rename = "room_id", alias = "sfu_channel_uuid")]
    pub room_id: String,
    #[serde(rename = "user_id", alias = "session_id")]
    pub user_id: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<UserPermissions>,
}

impl WebSocketConnectClaims {
    pub fn normalize_runtime_user_id(&mut self) {
        self.user_id = self.user_id.runtime_normalized();
    }
}

#[must_use]
pub(crate) fn secs_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// # Errors
///
/// Returns an error when the key cannot be decoded or the claims cannot be serialized.
pub fn sign<T>(claims: &T, key_b64: &str) -> Result<String, AuthenticationError>
where
    T: Serialize,
{
    let key = decode_base64(key_b64)?;
    let header = JwtHeader {
        alg: ALGORITHM_HS256.to_owned(),
        typ: Some(TYPE_JWT.to_owned()),
    };
    let header_json =
        serde_json::to_vec(&header).map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    let claims_json =
        serde_json::to_vec(claims).map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json);
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json);
    let signed_data = format!("{header_b64}.{claims_b64}");
    let signature = sign_hs256(signed_data.as_bytes(), &key)?;
    let signature_b64 = URL_SAFE_NO_PAD.encode(signature);
    Ok(format!("{signed_data}.{signature_b64}"))
}

/// # Errors
///
/// Returns an error when the token format, segment encoding, signature, or registered claims are
/// invalid.
///
/// This verifier decodes JWT header, payload, and signature segments with the JOSE base64url
/// alphabet without padding, as required by RFC 7515 / RFC 7519.
pub fn verify<T>(token: &str, key_b64: &str) -> Result<T, AuthenticationError>
where
    T: DeserializeOwned,
{
    validate_token_length(token)?;
    let key = decode_base64(key_b64)?;
    let (header_b64, claims_b64, signature_b64) = split_token(token)?;
    let header_bytes = decode_jwt_segment(header_b64)?;
    let header: JwtHeader = serde_json::from_slice(&header_bytes)
        .map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    if header.alg != ALGORITHM_HS256 {
        return Err(AuthenticationError::UnsupportedAlgorithm(header.alg));
    }
    let actual_signature = decode_jwt_segment(signature_b64)?;
    verify_hs256(
        format!("{header_b64}.{claims_b64}").as_bytes(),
        &key,
        &actual_signature,
    )?;
    let claims_bytes = decode_jwt_segment(claims_b64)?;
    let registered_claims: RegisteredJwtClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    validate_registered_claims(&registered_claims)?;
    serde_json::from_slice(&claims_bytes).map_err(|_error| AuthenticationError::InvalidJsonPayload)
}

/// decode untrusted JWT claims for candidate room selection only
///
/// callers must verify the same token with the selected room key before using
/// the decoded claims as authenticated identity or permission data
pub(crate) fn decode_unverified_claims<T>(token: &str) -> Result<T, AuthenticationError>
where
    T: DeserializeOwned,
{
    validate_token_length(token)?;
    let (_header_b64, claims_b64, _signature_b64) = split_token(token)?;
    let claims_bytes = decode_jwt_segment(claims_b64)?;
    serde_json::from_slice(&claims_bytes).map_err(|_error| AuthenticationError::InvalidJsonPayload)
}

fn validate_token_length(token: &str) -> Result<(), AuthenticationError> {
    if token.len() > MAX_JWT_TOKEN_BYTES {
        return Err(AuthenticationError::TokenTooLarge {
            actual: token.len(),
            limit: MAX_JWT_TOKEN_BYTES,
        });
    }
    Ok(())
}

fn validate_registered_claims(claims: &RegisteredJwtClaims) -> Result<(), AuthenticationError> {
    let now = secs_since_epoch();
    if claims.exp.is_some_and(|exp| exp <= now) {
        return Err(AuthenticationError::TokenExpired);
    }
    if claims.nbf.is_some_and(|nbf| nbf > now) {
        return Err(AuthenticationError::TokenNotYetValid);
    }
    if claims
        .iat
        .is_some_and(|iat| iat > now + MAX_IAT_FUTURE_SKEW_SECONDS)
    {
        return Err(AuthenticationError::TokenIssuedInFuture);
    }
    Ok(())
}

fn sign_hs256(data: &[u8], key: &[u8]) -> Result<Vec<u8>, AuthenticationError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_error| AuthenticationError::InvalidBase64Encoding)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_hs256(data: &[u8], key: &[u8], signature: &[u8]) -> Result<(), AuthenticationError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_error| AuthenticationError::InvalidBase64Encoding)?;
    mac.update(data);
    mac.verify_slice(signature)
        .map_err(|_error| AuthenticationError::InvalidSignature)
}

fn split_token(token: &str) -> Result<(&str, &str, &str), AuthenticationError> {
    let mut parts = token.split('.');
    let (Some(header), Some(claims), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(AuthenticationError::InvalidJwtFormat);
    };
    if header.is_empty() || claims.is_empty() || signature.is_empty() {
        return Err(AuthenticationError::InvalidJwtFormat);
    }
    Ok((header, claims, signature))
}

fn decode_base64(input: &str) -> Result<Vec<u8>, AuthenticationError> {
    let padded = pad_base64(input);
    URL_SAFE
        .decode(padded.as_bytes())
        .or_else(|_error| STANDARD.decode(padded.as_bytes()))
        .map_err(|_error| AuthenticationError::InvalidBase64Encoding)
}

fn decode_jwt_segment(input: &str) -> Result<Vec<u8>, AuthenticationError> {
    URL_SAFE_NO_PAD
        .decode(input.as_bytes())
        .map_err(|_error| AuthenticationError::InvalidBase64Encoding)
}

fn pad_base64(input: &str) -> String {
    let remainder = input.len() % 4;
    if remainder == 0 {
        return input.to_owned();
    }
    format!("{input}{}", "=".repeat(4 - remainder))
}

#[cfg(test)]
#[path = "TESTS/auth.rs"]
mod tests;
