use crate::runtime::{
    transport_adapter::{TransportConnectDirection, TransportConnectRequest},
    transport_connect::{
        TransportConnectDtlsFingerprint, TransportConnectDtlsParameters,
        TransportConnectIceParameters,
    },
};
use crate::signaling::current_protocol::{CurrentClientRequest, CurrentTransportConnectPayload};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LegacyTransportConnectRequest {
    direction: TransportConnectDirection,
    dtls_parameters: TransportConnectDtlsParameters,
    ice_parameters: Option<TransportConnectIceParameters>,
    sdp_offer: Option<String>,
}

impl LegacyTransportConnectRequest {
    #[must_use]
    pub(super) fn from_client_request(request: &CurrentClientRequest) -> Option<Self> {
        let (direction, payload) = match request {
            CurrentClientRequest::ConnectUploadTransport(payload) => {
                (TransportConnectDirection::Upload, payload)
            }
            CurrentClientRequest::ConnectDownloadTransport(payload) => {
                (TransportConnectDirection::Download, payload)
            }
            _other => return None,
        };
        Some(Self::new(direction, payload))
    }

    fn new(direction: TransportConnectDirection, payload: &CurrentTransportConnectPayload) -> Self {
        Self {
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
                        .0
                        .get("usernameFragment")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    password: ice_parameters
                        .0
                        .get("password")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                }
            }),
            sdp_offer: payload.sdp_offer.clone(),
        }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::LegacyTransportConnectRequest;
    use crate::{
        runtime::{
            transport_adapter::TransportConnectDirection,
            transport_connect::{
                TransportConnectDtlsFingerprint, TransportConnectDtlsParameters,
                TransportConnectIceParameters,
            },
        },
        signaling::{
            current_protocol::{CurrentClientRequest, CurrentTransportConnectPayload},
            webrtc::{DtlsFingerprint, DtlsParameters, IceParameters},
        },
    };

    #[test]
    fn upload_request_translation_preserves_connect_parameters() {
        let payload = CurrentTransportConnectPayload {
            dtls_parameters: DtlsParameters {
                role: String::from("client"),
                fingerprints: vec![DtlsFingerprint {
                    algorithm: String::from("sha-256"),
                    value: String::from("AA:BB"),
                }],
            },
            ice_parameters: Some(IceParameters(json!({
                "usernameFragment": "user",
                "password": "secret",
            }))),
            sdp_offer: Some(String::from("v=0\r\ns=offer\r\n")),
        };

        let translated = LegacyTransportConnectRequest::from_client_request(
            &CurrentClientRequest::ConnectUploadTransport(payload),
        );

        assert!(translated.is_some());
        let Some(translated) = translated else {
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
        let translated = LegacyTransportConnectRequest::from_client_request(
            &CurrentClientRequest::StopRecording,
        );

        assert_eq!(translated, None);
    }

    #[test]
    fn download_request_translation_sets_download_direction() {
        let translated = LegacyTransportConnectRequest::from_client_request(
            &CurrentClientRequest::ConnectDownloadTransport(CurrentTransportConnectPayload {
                dtls_parameters: DtlsParameters {
                    role: String::from("client"),
                    fingerprints: vec![],
                },
                ice_parameters: None,
                sdp_offer: None,
            }),
        );

        assert!(translated.is_some());
        let Some(translated) = translated else {
            return;
        };
        assert_eq!(translated.direction(), TransportConnectDirection::Download);
        assert_eq!(
            translated.transport_connect_request().direction(),
            TransportConnectDirection::Download
        );
    }
}
