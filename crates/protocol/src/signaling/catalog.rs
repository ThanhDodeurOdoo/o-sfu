use o_sfu_rfc::webrtc::MediaKind;
use serde::{Deserialize, Serialize};

use crate::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, PeerSnapshot, RecordingState, StreamType,
    UserId, UserInfo,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct AuthPayload {
    pub jwt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts-bindings", ts(optional))]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct WelcomePayload {
    pub features: AvailableFeatures,
    pub recording: RecordingState,
    pub peers: Vec<PeerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct SessionDescriptionPayload {
    pub sdp: String,
    #[serde(default, rename = "uploadSlots", skip_serializing_if = "Vec::is_empty")]
    pub upload_slots: Vec<NegotiationUploadSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct NegotiationUploadSlot {
    pub mid: String,
    pub kind: MediaKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub codecs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub simulcast_encodings: Vec<NegotiationUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
#[serde(rename_all = "camelCase")]
pub struct NegotiationUploadEncoding {
    pub rid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bitrate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_scale: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_framerate: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub enum UploadLayerPolicyRole {
    Featured,
    Thumbnail,
    DegradedThumbnail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct StreamIntentPayload {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct SubscribePayload {
    /// Wire shape is intentionally flat: `{ sessionId, audio?, camera?, screen? }`.
    /// Adding fields to `DownloadStates` implicitly changes the subscribe payload shape.
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(flatten)]
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct TrackBinding {
    pub mid: String,
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts-bindings", ts(optional))]
    pub source: Option<SourceDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct SourceDescriptor {
    pub source_id: String,
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts-bindings", ts(optional))]
    pub mid: Option<String>,
    pub encodings: Vec<SourceEncodingDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-bindings", ts(optional_fields))]
#[serde(rename_all = "camelCase")]
pub struct SourceEncodingDescriptor {
    pub encoding_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bitrate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_scale: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_framerate: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_role: Option<UploadLayerPolicyRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_temporal_layer_id: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct PeerInfoPayload {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    pub info: UserInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct PeerLeftPayload {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct ClientBroadcastPayload {
    #[cfg_attr(feature = "ts-bindings", ts(type = "unknown"))]
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
#[serde(rename_all = "camelCase")]
pub struct ServerBroadcastPayload {
    pub sender_id: UserId,
    #[cfg_attr(feature = "ts-bindings", ts(type = "unknown"))]
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct RecordingActionResult {
    pub ok: bool,
}
