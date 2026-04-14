use serde::Deserialize;
use serde_json::Value;

use crate::runtime::{
    transport_adapter::{TransportConnectDirection, TransportConnectRequest},
    transport_connect::{
        TransportConnectDtlsFingerprint, TransportConnectDtlsParameters,
        TransportConnectIceParameters,
    },
};

const LEGACY_UPLOAD_TRANSPORT_CONNECT_REQUEST_NAME: &str = "CONNECT_CTS_TRANSPORT";
const LEGACY_DOWNLOAD_TRANSPORT_CONNECT_REQUEST_NAME: &str = "CONNECT_STC_TRANSPORT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LegacyTransportConnectRequest {
    direction: TransportConnectDirection,
    dtls_parameters: TransportConnectDtlsParameters,
    ice_parameters: Option<TransportConnectIceParameters>,
    sdp_offer: Option<String>,
}

impl LegacyTransportConnectRequest {
    #[must_use]
    pub(super) fn decode_wire(message: &Value) -> Option<Result<Self, ()>> {
        let object = message.as_object()?;
        let name = object.get("name")?.as_str()?;
        let direction = match name {
            LEGACY_UPLOAD_TRANSPORT_CONNECT_REQUEST_NAME => TransportConnectDirection::Upload,
            LEGACY_DOWNLOAD_TRANSPORT_CONNECT_REQUEST_NAME => TransportConnectDirection::Download,
            _other => return None,
        };
        let payload = object.get("payload").ok_or(());
        Some(payload.and_then(|payload| Self::decode_payload(direction, payload)))
    }

    fn decode_payload(direction: TransportConnectDirection, payload: &Value) -> Result<Self, ()> {
        let payload = serde_json::from_value::<LegacyTransportConnectPayload>(payload.clone())
            .map_err(|_error| ())?;
        Ok(Self {
            direction,
            dtls_parameters: TransportConnectDtlsParameters {
                role: payload.dtls_parameters.role.clone(),
                fingerprints: payload
                    .dtls_parameters
                    .fingerprints
                    .iter()
                    .map(|fingerprint| TransportConnectDtlsFingerprint {
                        algorithm: fingerprint.algorithm.clone(),
                        value: fingerprint.value.clone(),
                    })
                    .collect(),
            },
            ice_parameters: payload.ice_parameters.as_ref().map(|ice_parameters| {
                TransportConnectIceParameters {
                    username_fragment: ice_parameters
                        .raw
                        .get("usernameFragment")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    password: ice_parameters
                        .raw
                        .get("password")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                }
            }),
            sdp_offer: payload.sdp_offer.clone(),
        })
    }

    #[must_use]
    pub(super) const fn direction(&self) -> TransportConnectDirection {
        self.direction
    }

    #[must_use]
    pub(super) fn transport_connect_request(&self) -> TransportConnectRequest<'_> {
        let request = self.ice_parameters.as_ref().map_or_else(
            || TransportConnectRequest::new(self.direction, &self.dtls_parameters),
            |ice_parameters| {
                TransportConnectRequest::new(self.direction, &self.dtls_parameters)
                    .with_ice_parameters(ice_parameters)
            },
        );
        self.sdp_offer
            .as_deref()
            .map_or(request, |sdp_offer| request.with_sdp_offer(sdp_offer))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyTransportConnectPayload {
    dtls_parameters: LegacyTransportConnectDtlsParameters,
    #[serde(default)]
    ice_parameters: Option<LegacyTransportConnectIceParameters>,
    #[serde(default)]
    sdp_offer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyTransportConnectDtlsParameters {
    role: String,
    fingerprints: Vec<LegacyTransportConnectDtlsFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyTransportConnectDtlsFingerprint {
    algorithm: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyTransportConnectIceParameters {
    #[serde(flatten)]
    raw: serde_json::Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::LegacyTransportConnectRequest;
    use crate::runtime::{
        transport_adapter::TransportConnectDirection,
        transport_connect::{
            TransportConnectDtlsFingerprint, TransportConnectDtlsParameters,
            TransportConnectIceParameters,
        },
    };

    #[test]
    fn upload_request_translation_preserves_connect_parameters() {
        let translated = LegacyTransportConnectRequest::decode_wire(&json!({
            "name": "CONNECT_CTS_TRANSPORT",
            "payload": {
                "dtlsParameters": {
                    "role": "client",
                    "fingerprints": [{
                        "algorithm": "sha-256",
                        "value": "AA:BB"
                    }]
                },
                "iceParameters": {
                    "usernameFragment": "user",
                    "password": "secret"
                },
                "sdpOffer": "v=0\r\ns=offer\r\n"
            }
        }));

        assert!(matches!(translated, Some(Ok(_))));
        let Some(Ok(translated)) = translated else {
            return;
        };
        assert_eq!(translated.direction(), TransportConnectDirection::Upload);
        let request = translated.transport_connect_request();
        assert_eq!(request.direction(), TransportConnectDirection::Upload);
        assert_eq!(
            request.dtls_parameters(),
            &TransportConnectDtlsParameters {
                role: String::from("client"),
                fingerprints: vec![TransportConnectDtlsFingerprint {
                    algorithm: String::from("sha-256"),
                    value: String::from("AA:BB"),
                }],
            }
        );
        assert_eq!(
            request.ice_parameters(),
            Some(&TransportConnectIceParameters {
                username_fragment: Some(String::from("user")),
                password: Some(String::from("secret")),
            })
        );
        assert_eq!(request.sdp_offer(), Some("v=0\r\ns=offer\r\n"));
    }

    #[test]
    fn non_connect_requests_do_not_translate_to_legacy_connect_requests() {
        let translated =
            LegacyTransportConnectRequest::decode_wire(&json!({ "name": "STOP_RECORDING" }));

        assert_eq!(translated, None);
    }

    #[test]
    fn download_request_translation_sets_download_direction() {
        let translated = LegacyTransportConnectRequest::decode_wire(&json!({
            "name": "CONNECT_STC_TRANSPORT",
            "payload": {
                "dtlsParameters": {
                    "role": "client",
                    "fingerprints": []
                }
            }
        }));

        assert!(matches!(translated, Some(Ok(_))));
        let Some(Ok(translated)) = translated else {
            return;
        };
        assert_eq!(translated.direction(), TransportConnectDirection::Download);
        assert_eq!(
            translated.transport_connect_request().direction(),
            TransportConnectDirection::Download
        );
    }

    #[test]
    fn malformed_connect_request_is_rejected() {
        let translated = LegacyTransportConnectRequest::decode_wire(&json!({
            "name": "CONNECT_CTS_TRANSPORT",
            "payload": {
                "dtlsParameters": false
            }
        }));

        assert!(matches!(translated, Some(Err(()))));
    }
}
