use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type JsonPayload = Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SessionId {
    Integer(i64),
    String(String),
}

impl From<i64> for SessionId {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for SessionId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for SessionId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

#[allow(
    clippy::struct_excessive_bools,
    reason = "The startup payload mirrors the current wire contract and therefore exposes four explicit feature flags."
)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AvailableFeatures {
    pub rtc: bool,
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopCode {
    #[serde(rename = "user_request")]
    UserRequest,
    #[serde(rename = "channel_closed")]
    ChannelClosed,
    #[serde(rename = "recording_timeout")]
    RecordingTimeout,
    #[serde(rename = "recording_failed")]
    RecordingFailed,
    #[serde(rename = "disk_space_exhausted")]
    DiskSpaceExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingStateUpdate {
    pub state: RecordingState,
    #[serde(rename = "stopCode", skip_serializing_if = "Option::is_none")]
    pub stop_code: Option<StopCode>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionPermissions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_recording: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_recording: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_talking: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_featured: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_camera_on: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_screen_sharing_on: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_self_muted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_deaf: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_raising_hand: Option<bool>,
}

impl SessionInfo {
    #[must_use]
    pub fn snapshot_defaults() -> Self {
        Self::default().snapshot_complete()
    }

    #[must_use]
    pub fn snapshot_complete(self) -> Self {
        Self {
            is_talking: Some(self.is_talking.unwrap_or(false)),
            is_featured: Some(self.is_featured.unwrap_or(false)),
            is_camera_on: Some(self.is_camera_on.unwrap_or(false)),
            is_screen_sharing_on: Some(self.is_screen_sharing_on.unwrap_or(false)),
            is_self_muted: Some(self.is_self_muted.unwrap_or(false)),
            is_deaf: Some(self.is_deaf.unwrap_or(false)),
            is_raising_hand: Some(self.is_raising_hand.unwrap_or(false)),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadStates {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screen: Option<bool>,
    #[serde(rename = "cameraLayout", skip_serializing_if = "Option::is_none")]
    pub camera_layout: Option<VideoLayoutIntent>,
    #[serde(rename = "screenLayout", skip_serializing_if = "Option::is_none")]
    pub screen_layout: Option<VideoLayoutIntent>,
}

impl DownloadStates {
    /// Iterate over the present (`stream_type`, `active`) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (StreamType, bool)> + '_ {
        [
            self.audio.map(|v| (StreamType::Audio, v)),
            self.camera.map(|v| (StreamType::Camera, v)),
            self.screen.map(|v| (StreamType::Screen, v)),
        ]
        .into_iter()
        .flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoLayoutIntent {
    Featured,
    Pinned,
    VisibleThumbnail,
    Hidden,
    Overflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StreamType {
    #[serde(rename = "audio")]
    Audio,
    #[serde(rename = "camera")]
    Camera,
    #[serde(rename = "screen")]
    Screen,
}
