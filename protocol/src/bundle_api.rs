use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
    SessionId, SessionInfo, StreamType,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "arguments", rename_all = "camelCase")]
pub enum BundleMethodCall {
    Connect(BundleConnectCall),
    Disconnect,
    Broadcast(BundleBroadcastCall),
    UpdateInfo(BundleUpdateInfoCall),
    UpdateDownload(BundleUpdateDownloadCall),
    UpdateUpload(BundleUpdateUploadCall),
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
    pub channel_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ice_servers: Option<Vec<BundleIceServer>>,
}

impl BundleConnectOptions {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.channel_uuid.is_none() && self.ice_servers.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleBroadcastCall {
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleUpdateInfoCall {
    pub info: SessionInfo,
    #[serde(default, skip_serializing_if = "BundleUpdateInfoOptions::is_empty")]
    pub options: BundleUpdateInfoOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleUpdateInfoOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub need_refresh: Option<bool>,
}

impl BundleUpdateInfoOptions {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.need_refresh.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleUpdateDownloadCall {
    pub session_id: SessionId,
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleUpdateUploadCall {
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

pub type BundleSessionInfoSnapshotById = BTreeMap<String, SessionInfo>;

#[must_use]
pub fn bundle_session_info_key(session_id: &SessionId) -> String {
    match session_id {
        SessionId::Integer(value) => value.to_string(),
        SessionId::String(value) => value.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleBroadcastUpdate {
    #[serde(rename = "senderId")]
    pub sender_id: SessionId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleDisconnectUpdate {
    #[serde(rename = "sessionId")]
    pub session_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleTrackUpdate {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    #[serde(rename = "sessionId")]
    pub session_id: SessionId,
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
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::{Value, json};

    use super::{
        BundleBroadcastCall, BundleBroadcastUpdate, BundleConnectCall, BundleConnectOptions,
        BundleConnectionState, BundleMethodCall, BundleProtocolStrategy, BundleRecordingOptions,
        BundleStartRecordingCall, BundleStateChange, BundleUpdate, BundleUpdateDownloadCall,
        BundleUpdateInfoCall, BundleUpdateInfoOptions, BundleUpdateKind, BundleUpdateUploadCall,
        FIRST_BUNDLE_PROTOCOL_STRATEGY, FIRST_BUNDLE_PROTOCOL_VERSION, bundle_session_info_key,
    };
    use crate::shared::{
        DownloadStates, RecordingState, RecordingStateUpdate, SessionId, SessionInfo, StopCode,
        StreamType,
    };

    fn assert_round_trip<T>(value: &T, expected_json: Value) -> serde_json::Result<()>
    where
        T: Debug + DeserializeOwned + PartialEq + Serialize,
    {
        assert_eq!(serde_json::to_value(value)?, expected_json);
        assert_eq!(serde_json::from_value::<T>(expected_json)?, *value);
        Ok(())
    }

    #[test]
    fn first_bundle_protocol_reuses_current_wire_v1() {
        assert_eq!(FIRST_BUNDLE_PROTOCOL_VERSION, 1);
        assert_eq!(
            FIRST_BUNDLE_PROTOCOL_STRATEGY,
            BundleProtocolStrategy::ReuseCurrentWireV1
        );
    }

    #[test]
    fn bundle_connection_states_round_trip() -> serde_json::Result<()> {
        assert_round_trip(&BundleConnectionState::Disconnected, json!("disconnected"))?;
        assert_round_trip(
            &BundleConnectionState::Authenticated,
            json!("authenticated"),
        )?;
        assert_round_trip(&BundleConnectionState::Recovering, json!("recovering"))?;
        assert_round_trip(
            &BundleStateChange {
                state: BundleConnectionState::Closed,
                cause: Some(String::from("full")),
            },
            json!({
                "state": "closed",
                "cause": "full"
            }),
        )
    }

    #[test]
    fn bundle_update_kinds_round_trip() -> serde_json::Result<()> {
        assert_round_trip(&BundleUpdateKind::Track, json!("track"))?;
        assert_round_trip(&BundleUpdateKind::Broadcast, json!("broadcast"))?;
        assert_round_trip(&BundleUpdateKind::Disconnect, json!("disconnect"))?;
        assert_round_trip(&BundleUpdateKind::SessionInfoChange, json!("info_change"))?;
        assert_round_trip(
            &BundleUpdateKind::ChannelInfoChange,
            json!("channel_info_change"),
        )
    }

    #[test]
    fn bundle_connect_and_broadcast_calls_round_trip() -> serde_json::Result<()> {
        let connect = BundleMethodCall::Connect(BundleConnectCall {
            url: "https://sfu.example.com".to_owned(),
            json_web_token: "signed-token".to_owned(),
            options: Some(BundleConnectOptions {
                channel_uuid: Some("31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned()),
                ice_servers: Some(vec![json!({
                    "urls": "stun:stun.example.com"
                })]),
            }),
        });
        assert_round_trip(
            &connect,
            json!({
                "method": "connect",
                "arguments": {
                    "url": "https://sfu.example.com",
                    "jsonWebToken": "signed-token",
                    "options": {
                        "channelUUID": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
                        "iceServers": [{
                            "urls": "stun:stun.example.com"
                        }]
                    }
                }
            }),
        )?;

        let broadcast = BundleMethodCall::Broadcast(BundleBroadcastCall {
            message: json!({ "kind": "wave" }),
        });
        assert_round_trip(
            &broadcast,
            json!({
                "method": "broadcast",
                "arguments": {
                    "message": { "kind": "wave" }
                }
            }),
        )
    }

    #[test]
    fn bundle_state_mutation_calls_round_trip() -> serde_json::Result<()> {
        let update_info = BundleMethodCall::UpdateInfo(BundleUpdateInfoCall {
            info: SessionInfo {
                is_talking: Some(true),
                is_camera_on: Some(false),
                is_screen_sharing_on: None,
                is_self_muted: None,
                is_deaf: None,
                is_raising_hand: Some(true),
            },
            options: BundleUpdateInfoOptions {
                need_refresh: Some(true),
            },
        });
        assert_round_trip(
            &update_info,
            json!({
                "method": "updateInfo",
                "arguments": {
                    "info": {
                        "isTalking": true,
                        "isCameraOn": false,
                        "isRaisingHand": true
                    },
                    "options": {
                        "needRefresh": true
                    }
                }
            }),
        )?;

        let update_download = BundleMethodCall::UpdateDownload(BundleUpdateDownloadCall {
            session_id: SessionId::Integer(7),
            states: DownloadStates {
                audio: Some(false),
                camera: None,
                screen: Some(true),
            },
        });
        assert_round_trip(
            &update_download,
            json!({
                "method": "updateDownload",
                "arguments": {
                    "sessionId": 7,
                    "states": {
                        "audio": false,
                        "screen": true
                    }
                }
            }),
        )?;

        let update_upload = BundleMethodCall::UpdateUpload(BundleUpdateUploadCall {
            stream_type: StreamType::Audio,
            track: Some(json!({
                "id": "microphone-track",
                "kind": "audio"
            })),
        });
        assert_round_trip(
            &update_upload,
            json!({
                "method": "updateUpload",
                "arguments": {
                    "type": "audio",
                    "track": {
                        "id": "microphone-track",
                        "kind": "audio"
                    }
                }
            }),
        )
    }

    #[test]
    fn bundle_recording_and_control_calls_round_trip() -> serde_json::Result<()> {
        let start_recording = BundleMethodCall::StartRecording(BundleStartRecordingCall {
            options: BundleRecordingOptions {
                audio: Some(true),
                video: Some(false),
                transcription: Some(true),
            },
        });
        assert_round_trip(
            &start_recording,
            json!({
                "method": "startRecording",
                "arguments": {
                    "options": {
                        "audio": true,
                        "video": false,
                        "transcription": true
                    }
                }
            }),
        )?;

        assert_round_trip(&BundleMethodCall::GetStats, json!({ "method": "getStats" }))?;
        assert_round_trip(
            &BundleMethodCall::Disconnect,
            json!({ "method": "disconnect" }),
        )?;
        assert_round_trip(
            &BundleMethodCall::StopRecording,
            json!({ "method": "stopRecording" }),
        )
    }

    #[test]
    fn bundle_updates_round_trip() -> serde_json::Result<()> {
        let track_update = BundleUpdate::Track(super::BundleTrackUpdate {
            stream_type: StreamType::Camera,
            session_id: SessionId::Integer(9),
            track: json!({
                "id": "camera-track",
                "kind": "video"
            }),
            active: true,
        });
        assert_eq!(track_update.kind(), BundleUpdateKind::Track);
        assert_round_trip(
            &track_update,
            json!({
                "name": "track",
                "payload": {
                    "type": "camera",
                    "sessionId": 9,
                    "track": {
                        "id": "camera-track",
                        "kind": "video"
                    },
                    "active": true
                }
            }),
        )?;

        let broadcast = BundleUpdate::Broadcast(BundleBroadcastUpdate {
            sender_id: SessionId::String("guest-7".to_owned()),
            message: json!("hello"),
        });
        assert_eq!(broadcast.kind(), BundleUpdateKind::Broadcast);
        assert_round_trip(
            &broadcast,
            json!({
                "name": "broadcast",
                "payload": {
                    "senderId": "guest-7",
                    "message": "hello"
                }
            }),
        )?;

        let session_info = BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&SessionId::Integer(5)),
            SessionInfo {
                is_talking: Some(false),
                is_camera_on: Some(true),
                is_screen_sharing_on: None,
                is_self_muted: None,
                is_deaf: None,
                is_raising_hand: None,
            },
        )]));
        assert_eq!(session_info.kind(), BundleUpdateKind::SessionInfoChange);
        assert_round_trip(
            &session_info,
            json!({
                "name": "info_change",
                "payload": {
                    "5": {
                        "isTalking": false,
                        "isCameraOn": true
                    }
                }
            }),
        )?;

        let channel_info = BundleUpdate::ChannelInfoChange(RecordingStateUpdate {
            state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            stop_code: Some(StopCode::UserRequest),
        });
        assert_eq!(channel_info.kind(), BundleUpdateKind::ChannelInfoChange);
        assert_round_trip(
            &channel_info,
            json!({
                "name": "channel_info_change",
                "payload": {
                    "state": {
                        "recording": false,
                        "audio": false,
                        "transcription": false,
                        "video": false
                    },
                    "stopCode": "user_request"
                }
            }),
        )
    }
}
