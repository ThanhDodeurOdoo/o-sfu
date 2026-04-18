use serde::Serialize;

/// Direction of a WebRTC transport from the client's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TransportConnectDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TransportConnectDtlsFingerprint {
    pub(crate) algorithm: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TransportConnectDtlsParameters {
    pub(crate) role: String,
    pub(crate) fingerprints: Vec<TransportConnectDtlsFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TransportConnectIceParameters {
    pub(crate) username_fragment: Option<String>,
    pub(crate) password: Option<String>,
}

/// Named request for connecting one transport direction with client auth data.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransportConnectRequest<'a> {
    direction: TransportConnectDirection,
    dtls_parameters: &'a TransportConnectDtlsParameters,
    ice_parameters: Option<&'a TransportConnectIceParameters>,
    sdp_offer: Option<&'a str>,
}

impl<'a> TransportConnectRequest<'a> {
    #[must_use]
    pub(crate) fn new(
        direction: TransportConnectDirection,
        dtls_parameters: &'a TransportConnectDtlsParameters,
    ) -> Self {
        Self {
            direction,
            dtls_parameters,
            ice_parameters: None,
            sdp_offer: None,
        }
    }

    #[must_use]
    pub(crate) fn with_ice_parameters(
        mut self,
        ice_parameters: &'a TransportConnectIceParameters,
    ) -> Self {
        self.ice_parameters = Some(ice_parameters);
        self
    }

    #[must_use]
    pub(crate) fn with_sdp_offer(mut self, sdp_offer: &'a str) -> Self {
        self.sdp_offer = Some(sdp_offer);
        self
    }

    #[must_use]
    pub(crate) const fn direction(self) -> TransportConnectDirection {
        self.direction
    }

    #[must_use]
    pub(crate) const fn dtls_parameters(self) -> &'a TransportConnectDtlsParameters {
        self.dtls_parameters
    }

    #[must_use]
    pub(crate) const fn ice_parameters(self) -> Option<&'a TransportConnectIceParameters> {
        self.ice_parameters
    }

    #[must_use]
    pub(crate) const fn sdp_offer(self) -> Option<&'a str> {
        self.sdp_offer
    }
}
