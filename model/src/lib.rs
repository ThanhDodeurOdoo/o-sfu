use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type JsonPayload = Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UserId {
    Integer(i64),
    String(String),
}

impl From<i64> for UserId {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for UserId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for UserId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

#[allow(
    clippy::struct_excessive_bools,
    reason = "feature flags mirror the compatibility startup surface with explicit optional room capabilities"
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
pub struct UserPermissions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_recording: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_recording: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
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

impl UserInfo {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerSnapshot {
    #[serde(rename = "sessionId")]
    pub user_id: UserId,
    #[serde(default)]
    pub info: UserInfo,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WebSocketCloseCode {
    Clean = 1000,
    Leaving = 1001,
    ProtocolError = 1002,
    Error = 1011,
    AuthFailed = 4001,
    AuthTimeout = 4002,
    Kicked = 4003,
    RoomFull = 4004,
}

impl WebSocketCloseCode {
    #[must_use]
    pub const fn from_u16(value: u16) -> Option<Self> {
        match value {
            1000 => Some(Self::Clean),
            1001 => Some(Self::Leaving),
            1002 => Some(Self::ProtocolError),
            1011 => Some(Self::Error),
            4001 => Some(Self::AuthFailed),
            4002 => Some(Self::AuthTimeout),
            4003 => Some(Self::Kicked),
            4004 => Some(Self::RoomFull),
            _ => None,
        }
    }
}

impl From<WebSocketCloseCode> for u16 {
    fn from(value: WebSocketCloseCode) -> Self {
        match value {
            WebSocketCloseCode::Clean => 1000,
            WebSocketCloseCode::Leaving => 1001,
            WebSocketCloseCode::ProtocolError => 1002,
            WebSocketCloseCode::Error => 1011,
            WebSocketCloseCode::AuthFailed => 4001,
            WebSocketCloseCode::AuthTimeout => 4002,
            WebSocketCloseCode::Kicked => 4003,
            WebSocketCloseCode::RoomFull => 4004,
        }
    }
}
