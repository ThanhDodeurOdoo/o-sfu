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
