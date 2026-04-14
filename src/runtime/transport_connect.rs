use serde::Serialize;

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
