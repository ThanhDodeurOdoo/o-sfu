//! Legacy wire wrappers and transport literals used by the current compatibility path.
//!
//! Protocol work should avoid introducing new dependencies on the opaque RTP/ORTC
//! wrappers defined here.

use serde::{Deserialize, Serialize};
use serde_json::Value;

macro_rules! opaque_json_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Value);
    };
}

opaque_json_type!(IceParameters);
opaque_json_type!(PublishOptions);
opaque_json_type!(RtpCapabilities);
opaque_json_type!(RtpParameters);
opaque_json_type!(SctpParameters);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtlsFingerprint {
    pub algorithm: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtlsParameters {
    pub role: String,
    pub fingerprints: Vec<DtlsFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IceCandidate {
    pub foundation: String,
    pub priority: u64,
    pub ip: String,
    pub protocol: String,
    pub port: u64,
    #[serde(rename = "type")]
    pub candidate_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaKind {
    #[serde(rename = "audio")]
    Audio,
    #[serde(rename = "video")]
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportBootstrap {
    pub id: String,
    pub ice_parameters: IceParameters,
    pub ice_candidates: Vec<IceCandidate>,
    pub dtls_parameters: DtlsParameters,
    pub sctp_parameters: SctpParameters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishOptionsByMediaKind {
    pub audio: PublishOptions,
    pub video: PublishOptions,
}
