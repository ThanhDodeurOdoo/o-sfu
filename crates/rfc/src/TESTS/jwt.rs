use serde_json::json;

use super::{JwtAudience, RegisteredJwtClaims};

#[test]
fn registered_claims_accept_fractional_numeric_dates() -> serde_json::Result<()> {
    for (raw, expected) in [
        (r#"{"exp":1744000000}"#, r#"{"exp":1744000000}"#),
        (r#"{"exp":1744000000.5}"#, r#"{"exp":1744000000.5}"#),
        (r#"{"exp":1744000000.0}"#, r#"{"exp":1744000000}"#),
        (r#"{"exp":1.744e9}"#, r#"{"exp":1744000000}"#),
    ] {
        let claims: RegisteredJwtClaims = serde_json::from_str(raw)?;
        assert_eq!(serde_json::to_string(&claims)?, expected);
    }
    Ok(())
}

#[test]
fn registered_claims_reject_invalid_numeric_dates() {
    for raw in [r#"{"iat":-0.5}"#, r#"{"iat":1e20}"#, r#"{"iat":1e9999}"#] {
        assert!(serde_json::from_str::<RegisteredJwtClaims>(raw).is_err());
    }
}

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
