use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use hmac::{Hmac, KeyInit, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

use o_sfu_protocol::shared::{SessionId, SessionPermissions};
use o_sfu_rfc::jwt::{ALGORITHM_HS256, JwtHeader, TYPE_JWT, URL_SAFE_NO_PAD};

use crate::time::secs_since_epoch;

pub use o_sfu_rfc::jwt::{JwtAudience, RegisteredJwtClaims};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthenticationError {
    #[error("invalid JWT format")]
    InvalidJwtFormat,
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
pub struct HttpChannelClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpDisconnectClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(rename = "sessionIdsByChannel")]
    pub session_ids_by_channel: BTreeMap<String, Vec<SessionId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketConnectClaims {
    #[serde(flatten)]
    pub registered: RegisteredJwtClaims,
    #[serde(rename = "sfu_channel_uuid")]
    pub sfu_channel_uuid: String,
    #[serde(rename = "session_id")]
    pub session_id: SessionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<SessionPermissions>,
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
    let key = decode_base64(key_b64)?;
    let (header_b64, claims_b64, signature_b64) = split_token(token)?;
    let header_bytes = decode_jwt_segment(header_b64)?;
    let header: JwtHeader = serde_json::from_slice(&header_bytes)
        .map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    if header.alg != ALGORITHM_HS256 {
        return Err(AuthenticationError::UnsupportedAlgorithm(header.alg));
    }
    let claims_bytes = decode_jwt_segment(claims_b64)?;
    let claims_value: serde_json::Value = serde_json::from_slice(&claims_bytes)
        .map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    let registered_claims: RegisteredJwtClaims = serde_json::from_value(claims_value.clone())
        .map_err(|_error| AuthenticationError::InvalidJsonPayload)?;
    let actual_signature = decode_jwt_segment(signature_b64)?;
    verify_hs256(
        format!("{header_b64}.{claims_b64}").as_bytes(),
        &key,
        &actual_signature,
    )?;
    validate_registered_claims(&registered_claims)?;
    serde_json::from_value(claims_value).map_err(|_error| AuthenticationError::InvalidJsonPayload)
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
mod tests {
    use std::collections::BTreeMap;

    use base64::Engine as _;
    use serde::Serialize;
    use serde_json::json;

    use super::{
        AuthenticationError, HttpChannelClaims, HttpDisconnectClaims, JwtAudience,
        RegisteredJwtClaims, WebSocketConnectClaims, decode_base64, secs_since_epoch, sign,
        sign_hs256, verify,
    };
    use o_sfu_protocol::shared::{SessionId, SessionPermissions};
    use o_sfu_rfc::jwt::{ALGORITHM_HS256, JwtHeader, TYPE_JWT, URL_SAFE_NO_PAD};

    const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

    #[test]
    fn jwt_claims_round_trip() -> serde_json::Result<()> {
        let channel_claims = HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: Some("https://odoo.example.com".to_owned()),
                aud: Some(JwtAudience::Multiple(vec![
                    "urn:odoo:sfu".to_owned(),
                    "urn:odoo:recording".to_owned(),
                ])),
                exp: Some(1_744_000_000),
                ..RegisteredJwtClaims::default()
            },
            key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        };
        let expected_channel_claims = json!({
            "iss": "https://odoo.example.com",
            "aud": ["urn:odoo:sfu", "urn:odoo:recording"],
            "exp": 1_744_000_000,
            "key": "Y2hhbm5lbC1rZXk="
        });
        assert_eq!(
            serde_json::to_value(&channel_claims)?,
            expected_channel_claims
        );
        assert_eq!(
            serde_json::from_value::<HttpChannelClaims>(expected_channel_claims)?,
            channel_claims
        );

        let disconnect_claims = HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            session_ids_by_channel: BTreeMap::from([(
                "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
                vec![
                    SessionId::Integer(7),
                    SessionId::String("guest-3".to_owned()),
                ],
            )]),
        };
        let expected_disconnect_claims = json!({
            "sessionIdsByChannel": {
                "31dcc5dc-4d26-453e-9bca-ab1f5d268303": [7, "guest-3"]
            }
        });
        assert_eq!(
            serde_json::to_value(&disconnect_claims)?,
            expected_disconnect_claims
        );
        assert_eq!(
            serde_json::from_value::<HttpDisconnectClaims>(expected_disconnect_claims)?,
            disconnect_claims
        );

        let websocket_claims = WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            sfu_channel_uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
            session_id: SessionId::Integer(42),
            label: Some("Alice".to_owned()),
            permissions: Some(SessionPermissions {
                transcription: Some(false),
                audio_recording: Some(true),
                video_recording: Some(false),
            }),
        };
        let expected_websocket_claims = json!({
            "sfu_channel_uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
            "session_id": 42,
            "label": "Alice",
            "permissions": {
                "transcription": false,
                "audioRecording": true,
                "videoRecording": false
            }
        });
        assert_eq!(
            serde_json::to_value(&websocket_claims)?,
            expected_websocket_claims
        );
        assert_eq!(
            serde_json::from_value::<WebSocketConnectClaims>(expected_websocket_claims)?,
            websocket_claims
        );

        Ok(())
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: Some("https://odoo.example.com".to_owned()),
                ..RegisteredJwtClaims::default()
            },
            key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        };
        let token = sign(&claims, TEST_AUTH_KEY);
        assert!(token.is_ok());
        let Some(token) = token.ok() else {
            return;
        };
        let verified = verify::<HttpChannelClaims>(&token, TEST_AUTH_KEY);
        assert!(verified.is_ok());
        let Some(verified) = verified.ok() else {
            return;
        };
        assert_eq!(verified, claims);
    }

    #[test]
    fn sign_and_verify_round_trip_with_uuid_like_channel_key() {
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: Some("https://odoo.example.com".to_owned()),
                ..RegisteredJwtClaims::default()
            },
            key: Some("123e4567-e89b-12d3-a456-426614174000".to_owned()),
        };
        let token = sign(&claims, "123e4567-e89b-12d3-a456-426614174000");
        assert!(token.is_ok());
        let Some(token) = token.ok() else {
            return;
        };
        let verified = verify::<HttpChannelClaims>(&token, "123e4567-e89b-12d3-a456-426614174000");
        assert!(verified.is_ok());
        let Some(verified) = verified.ok() else {
            return;
        };
        assert_eq!(verified, claims);
    }

    #[test]
    fn decode_base64_matches_legacy_uuid_like_channel_key_bytes() {
        let decoded = decode_base64("123e4567-e89b-12d3-a456-426614174000");
        assert_eq!(
            decoded.ok(),
            Some(vec![
                0xd7, 0x6d, 0xde, 0xe3, 0x9e, 0xbb, 0xf9, 0xef, 0x3d, 0x6f, 0xed, 0x76, 0x77, 0x7f,
                0x9a, 0xe3, 0x9e, 0xbe, 0xe3, 0x6e, 0xba, 0xd7, 0x8d, 0x7b, 0xe3, 0x4d, 0x34,
            ])
        );
    }

    #[test]
    fn verify_rejects_expired_token() {
        let now = secs_since_epoch();
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims {
                exp: Some(now.saturating_sub(1)),
                ..RegisteredJwtClaims::default()
            },
            key: None,
        };
        let token = sign(&claims, TEST_AUTH_KEY);
        assert!(token.is_ok());
        let Some(token) = token.ok() else {
            return;
        };
        let error = verify::<HttpChannelClaims>(&token, TEST_AUTH_KEY).err();
        assert!(error.is_some());
        let Some(error) = error else {
            return;
        };
        assert_eq!(error, AuthenticationError::TokenExpired);
    }

    #[test]
    fn verify_rejects_token_when_exp_matches_current_second() {
        let now = secs_since_epoch();
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims {
                exp: Some(now),
                ..RegisteredJwtClaims::default()
            },
            key: None,
        };
        let token = sign(&claims, TEST_AUTH_KEY);
        assert!(token.is_ok());
        let Some(token) = token.ok() else {
            return;
        };
        let error = verify::<HttpChannelClaims>(&token, TEST_AUTH_KEY).err();
        assert_eq!(error, Some(AuthenticationError::TokenExpired));
    }

    #[test]
    fn sign_emits_jose_base64url_segments() {
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims::default(),
            key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        };

        let token = sign(&claims, TEST_AUTH_KEY);
        assert!(token.is_ok());
        let Some(token) = token.ok() else {
            return;
        };

        let segments = token.split('.').collect::<Vec<_>>();
        assert_eq!(segments.len(), 3);
        for segment in segments {
            assert!(!segment.contains('='));
            assert!(URL_SAFE_NO_PAD.decode(segment.as_bytes()).is_ok());
        }
    }

    #[test]
    fn verify_accepts_jose_base64url_token_without_typ_header() {
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims::default(),
            key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        };

        let token = sign_token_for_test(&claims, TEST_AUTH_KEY, None, SegmentEncoding::Jose);
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };

        let verified = verify::<HttpChannelClaims>(&token, TEST_AUTH_KEY);
        assert_eq!(verified.ok(), Some(claims));
    }

    #[test]
    fn verify_accepts_jose_base64url_token_with_typ_header() {
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims::default(),
            key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        };

        let token = sign_token_for_test(&claims, TEST_AUTH_KEY, Some("JWT"), SegmentEncoding::Jose);
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };

        let verified = verify::<HttpChannelClaims>(&token, TEST_AUTH_KEY);
        assert_eq!(verified.ok(), Some(claims));
    }

    #[test]
    fn verify_rejects_generated_invalid_token_shapes() {
        for token in [
            "",
            "header",
            "header.claims",
            ".claims.sig",
            "header..sig",
            "header.claims.",
            "a.b.c.d",
        ] {
            let error = verify::<HttpChannelClaims>(token, TEST_AUTH_KEY).err();
            assert_eq!(error, Some(AuthenticationError::InvalidJwtFormat));
        }
    }

    #[derive(Clone, Copy)]
    enum SegmentEncoding {
        Jose,
    }

    fn sign_token_for_test<T: Serialize>(
        claims: &T,
        key_b64: &str,
        typ: Option<&str>,
        segment_encoding: SegmentEncoding,
    ) -> Option<String> {
        let key = decode_base64(key_b64).ok()?;
        let header = JwtHeader {
            alg: ALGORITHM_HS256.to_owned(),
            typ: typ.map(str::to_owned),
        };
        let header_json = serde_json::to_vec(&header).ok()?;
        let claims_json = serde_json::to_vec(claims).ok()?;
        let header_b64 = encode_segment(&header_json, segment_encoding);
        let claims_b64 = encode_segment(&claims_json, segment_encoding);
        let signed_data = format!("{header_b64}.{claims_b64}");
        let signature = sign_hs256(signed_data.as_bytes(), &key).ok()?;
        let signature_b64 = encode_segment(&signature, segment_encoding);
        Some(format!("{signed_data}.{signature_b64}"))
    }

    fn encode_segment(bytes: &[u8], encoding: SegmentEncoding) -> String {
        match encoding {
            SegmentEncoding::Jose => URL_SAFE_NO_PAD.encode(bytes),
        }
    }

    #[test]
    fn sign_uses_rfc_header_constants() {
        let claims = HttpChannelClaims {
            registered: RegisteredJwtClaims::default(),
            key: None,
        };

        let token = sign_token_for_test(
            &claims,
            TEST_AUTH_KEY,
            Some(TYPE_JWT),
            SegmentEncoding::Jose,
        );
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };
        let verified = verify::<HttpChannelClaims>(&token, TEST_AUTH_KEY);
        assert_eq!(verified.ok(), Some(claims));
    }
}
