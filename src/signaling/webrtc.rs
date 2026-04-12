//! Legacy wire wrappers and transport literals used by the current compatibility path.
//!
//! Protocol work should avoid introducing new dependencies on the opaque RTP/ORTC
//! wrappers defined here.

use o_sfu_router::{CodecSetting, RtcpFeedback, RtcpFeedbackKind};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::rfc::webrtc::rtcp_feedback::{kind, parameter};

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

pub(crate) fn serialize_rtcp_feedback(feedback: &RtcpFeedback) -> Value {
    let (feedback_type, parameter) = match feedback.kind() {
        RtcpFeedbackKind::Nack => (kind::NACK, feedback.parameter()),
        RtcpFeedbackKind::NackPli => (kind::NACK, Some(parameter::PLI)),
        RtcpFeedbackKind::CcmFir => (kind::CCM, Some(parameter::FIR)),
        RtcpFeedbackKind::GoogRemb => (kind::GOOG_REMB, None),
        RtcpFeedbackKind::TransportCc => (kind::TRANSPORT_CC, None),
        RtcpFeedbackKind::Other(name) => (name.as_str(), feedback.parameter()),
    };
    let mut feedback_json = Map::new();
    feedback_json.insert("type".to_owned(), json!(feedback_type));
    if let Some(parameter) = parameter {
        feedback_json.insert("parameter".to_owned(), json!(parameter));
    }
    Value::Object(feedback_json)
}

pub(crate) fn serialize_codec_settings<'a>(
    settings: impl Iterator<Item = &'a CodecSetting>,
) -> Value {
    Value::Object(
        settings
            .map(|setting| {
                (
                    setting.key().to_owned(),
                    json!(setting.wire_value().as_ref()),
                )
            })
            .collect(),
    )
}
