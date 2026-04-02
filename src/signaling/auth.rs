use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::signaling::shared::{SessionId, SessionPermissions};

/// Registered JWT claims from RFC 7519 section 4.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredJwtClaims {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jti: Option<String>,
}

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims, WebSocketConnectClaims,
    };
    use crate::signaling::shared::{SessionId, SessionPermissions};

    #[test]
    fn jwt_claims_round_trip() -> serde_json::Result<()> {
        let channel_claims = HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: Some("https://odoo.example.com".to_owned()),
                exp: Some(1_744_000_000),
                ..RegisteredJwtClaims::default()
            },
            key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        };
        let expected_channel_claims = json!({
            "iss": "https://odoo.example.com",
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
}
