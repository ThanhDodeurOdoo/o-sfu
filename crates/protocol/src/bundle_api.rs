//! rust payloads for odoo browser bundle state and updates
//!
//! callable compatibility methods stay on `SfuClient`
//! host projection emits these DTOs through the public [`crate::bundle`] facade

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    shared::{JsonPayload, RecordingStateUpdate, StreamType, UserId, UserInfo},
    signaling::{SourceDescriptor, TrackBinding},
};

pub type BundleMediaTrack = JsonPayload;

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

/// `info_change` payloads use string keys from [`bundle_session_info_key`]
/// integer and string user ids that stringify to the same key collide, so the
/// later entry wins
pub type BundleSessionInfoSnapshotById = BTreeMap<String, UserInfo>;

#[must_use]
pub fn bundle_session_info_key(user_id: &UserId) -> String {
    user_id.path_segment().into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleBroadcastUpdate {
    #[serde(rename = "senderId")]
    pub sender_id: UserId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
pub struct BundleSourceUpdate {
    pub sources: Vec<SourceDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleRemoteMediaUpdate {
    pub bindings: Vec<TrackBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum BundleUpdate {
    #[serde(rename = "track")]
    Track(BundleTrackUpdate),
    #[serde(rename = "source")]
    Source(BundleSourceUpdate),
    #[serde(rename = "remote_media")]
    RemoteMedia(BundleRemoteMediaUpdate),
    #[serde(rename = "broadcast")]
    Broadcast(BundleBroadcastUpdate),
    #[serde(rename = "disconnect")]
    Disconnect(BundleDisconnectUpdate),
    #[serde(rename = "info_change")]
    SessionInfoChange(BundleSessionInfoSnapshotById),
    #[serde(rename = "channel_info_change")]
    ChannelInfoChange(RecordingStateUpdate),
}

#[cfg(test)]
#[path = "bundle_api/TESTS/mod.rs"]
mod tests;
