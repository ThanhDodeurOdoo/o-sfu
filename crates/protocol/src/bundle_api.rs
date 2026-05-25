//! browser bundle compatibility edge for Odoo
//!
//! preserves legacy `SfuClient` methods while native protocol uses typed host
//! commands and wire envelopes
//!
//! import through [`crate::bundle`]

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
    StreamType, UserId, UserInfo,
};

pub const FIRST_BUNDLE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleProtocolStrategy {
    ReuseCurrentWireV1,
}

pub const FIRST_BUNDLE_PROTOCOL_STRATEGY: BundleProtocolStrategy =
    BundleProtocolStrategy::ReuseCurrentWireV1;

pub type BundleIceServer = JsonPayload;

pub type BundleMediaTrack = JsonPayload;

pub type BundleStatsReport = JsonPayload;

/// method call accepted from the Odoo browser bundle edge with aliases like
/// `updateUpload` and `updateDownload`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "arguments", rename_all = "camelCase")]
pub enum BundleMethodCall {
    Connect(BundleConnectCall),
    Disconnect,
    Broadcast(BundleBroadcastCall),
    UpdateInfo(BundleUpdateInfoCall),
    #[serde(rename = "subscribe", alias = "updateDownload")]
    Subscribe(BundleSubscribeCall),
    #[serde(rename = "publish", alias = "updateUpload")]
    Publish(BundlePublishCall),
    GetStats,
    StartRecording(BundleStartRecordingCall),
    StopRecording,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleConnectCall {
    pub url: String,
    #[serde(rename = "jsonWebToken")]
    pub json_web_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<BundleConnectOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleConnectOptions {
    #[serde(rename = "channelUUID", skip_serializing_if = "Option::is_none")]
    pub room_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ice_servers: Option<Vec<BundleIceServer>>,
}

impl BundleConnectOptions {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.room_id.is_none() && self.ice_servers.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleBroadcastCall {
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BundleUpdateInfoCall {
    pub info: UserInfo,
}

impl<'de> Deserialize<'de> for BundleUpdateInfoCall {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct LegacyBundleUpdateInfoCall {
            pub info: UserInfo,
            #[serde(default, rename = "options")]
            pub _options: Option<serde_json::Value>,
        }

        let legacy = LegacyBundleUpdateInfoCall::deserialize(deserializer)?;
        Ok(Self { info: legacy.info })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleSubscribeCall {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlePublishCall {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track: Option<BundleMediaTrack>,
}

#[allow(
    clippy::struct_excessive_bools,
    reason = "The public recording surface intentionally mirrors the three independent toggles exposed by the current bundle."
)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleRecordingOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
}

impl BundleRecordingOptions {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.audio.is_none() && self.video.is_none() && self.transcription.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleStartRecordingCall {
    #[serde(default, skip_serializing_if = "BundleRecordingOptions::is_empty")]
    pub options: BundleRecordingOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_stats: Option<BundleStatsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_stats: Option<BundleStatsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<BundleStatsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera: Option<BundleStatsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screen: Option<BundleStatsReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleConnectionState {
    Disconnected,
    Connecting,
    Authenticated,
    Connected,
    Recovering,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleStateChange {
    pub state: BundleConnectionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BundleUpdateKind {
    #[serde(rename = "track")]
    Track,
    #[serde(rename = "broadcast")]
    Broadcast,
    #[serde(rename = "disconnect")]
    Disconnect,
    #[serde(rename = "info_change")]
    SessionInfoChange,
    #[serde(rename = "channel_info_change")]
    ChannelInfoChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleSessionSnapshot {
    pub available_features: AvailableFeatures,
    pub recording_state: RecordingState,
}

/// odoo/sfu `info_change` bundle payloads are string-keyed objects, so this
/// snapshot shape cannot distinguish `UserId::Integer(7)` from
/// `UserId::String("7")`. Mixed ID kinds remain accepted by the public API,
/// but if two users in the same channel stringify to the same key then the
/// later entry overwrites the earlier one in this snapshot view.
/// we just assume that the API user will not mix integer and string user
/// IDs in the same channel.
pub type BundleSessionInfoSnapshotById = BTreeMap<String, UserInfo>;

#[must_use]
pub fn bundle_session_info_key(user_id: &UserId) -> String {
    match user_id {
        UserId::Integer(value) => value.to_string(),
        UserId::String(value) => value.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct BundleBroadcastUpdate {
    #[serde(rename = "senderId")]
    pub sender_id: UserId,
    #[cfg_attr(feature = "ts-bindings", ts(type = "unknown"))]
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-bindings", derive(ts_rs::TS))]
pub struct BundleDisconnectUpdate {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleTrackUpdate {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    pub track: BundleMediaTrack,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum BundleUpdate {
    #[serde(rename = "track")]
    Track(BundleTrackUpdate),
    #[serde(rename = "broadcast")]
    Broadcast(BundleBroadcastUpdate),
    #[serde(rename = "disconnect")]
    Disconnect(BundleDisconnectUpdate),
    #[serde(rename = "info_change")]
    SessionInfoChange(BundleSessionInfoSnapshotById),
    #[serde(rename = "channel_info_change")]
    ChannelInfoChange(RecordingStateUpdate),
}

impl BundleUpdate {
    #[must_use]
    pub const fn kind(&self) -> BundleUpdateKind {
        match self {
            Self::Track(_) => BundleUpdateKind::Track,
            Self::Broadcast(_) => BundleUpdateKind::Broadcast,
            Self::Disconnect(_) => BundleUpdateKind::Disconnect,
            Self::SessionInfoChange(_) => BundleUpdateKind::SessionInfoChange,
            Self::ChannelInfoChange(_) => BundleUpdateKind::ChannelInfoChange,
        }
    }
}

#[cfg(test)]
mod tests;
