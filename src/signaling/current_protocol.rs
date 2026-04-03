//! Current bundle/server wire protocol used by the deployed SFU.
//! This module exists as a typed reference for migration and compatibility work.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::signaling::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
    SessionId, SessionInfo, StreamType,
};
use crate::signaling::webrtc::{
    DtlsParameters, MediaKind, PublishOptionsByMediaKind, RtpCapabilities, RtpParameters,
    TransportBootstrap,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentWebSocketCredentials {
    #[serde(rename = "channelUUID", skip_serializing_if = "Option::is_none")]
    pub channel_uuid: Option<String>,
    pub jwt: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CurrentWebSocketCloseCode {
    Clean = 1000,
    Leaving = 1001,
    Error = 1011,
    AuthenticationFailed = 4106,
    Timeout = 4107,
    Kicked = 4108,
    ChannelFull = 4109,
}

impl From<CurrentWebSocketCloseCode> for u16 {
    fn from(value: CurrentWebSocketCloseCode) -> Self {
        match value {
            CurrentWebSocketCloseCode::Clean => 1000,
            CurrentWebSocketCloseCode::Leaving => 1001,
            CurrentWebSocketCloseCode::Error => 1011,
            CurrentWebSocketCloseCode::AuthenticationFailed => 4106,
            CurrentWebSocketCloseCode::Timeout => 4107,
            CurrentWebSocketCloseCode::Kicked => 4108,
            CurrentWebSocketCloseCode::ChannelFull => 4109,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentWebSocketLifecycleState {
    Accepted,
    AwaitingCredentials,
    CredentialsVerified,
    SessionCreated,
    StartupDataSent,
    MessageLoop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStartupPayload {
    #[serde(rename = "availableFeatures")]
    pub available_features: AvailableFeatures,
    #[serde(rename = "recordingState")]
    pub recording_state: RecordingState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentDownloadStateChangePayload {
    pub session_id: SessionId,
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentSessionInfoUpdatePayload {
    pub info: SessionInfo,
    #[serde(rename = "needRefresh", skip_serializing_if = "Option::is_none")]
    pub need_refresh: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentUploadStateChangePayload {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentTransportConnectPayload {
    #[serde(rename = "dtlsParameters")]
    pub dtls_parameters: DtlsParameters,
    #[serde(rename = "sdpOffer", skip_serializing_if = "Option::is_none")]
    pub sdp_offer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentPublishTrackPayload {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    #[serde(rename = "kind")]
    pub media_kind: MediaKind,
    #[serde(rename = "rtpParameters")]
    pub rtp_parameters: RtpParameters,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStartRecordingPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentBroadcastPayload {
    pub sender_id: SessionId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentSessionDeparturePayload {
    #[serde(rename = "sessionId")]
    pub session_id: SessionId,
}

pub type CurrentSessionInfoSnapshotById = BTreeMap<String, SessionInfo>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentRemoteTrackBootstrapPayload {
    pub id: String,
    #[serde(rename = "kind")]
    pub media_kind: MediaKind,
    #[serde(rename = "producerId")]
    pub source_id: String,
    pub rtp_parameters: RtpParameters,
    pub session_id: SessionId,
    pub active: bool,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentTransportBootstrapPayload {
    #[serde(rename = "capabilities")]
    pub router_capabilities: RtpCapabilities,
    #[serde(rename = "stcConfig")]
    pub download_transport: TransportBootstrap,
    #[serde(rename = "ctsConfig")]
    pub upload_transport: TransportBootstrap,
    #[serde(rename = "producerOptionsByKind")]
    pub publish_options_by_media_kind: PublishOptionsByMediaKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentPublishTrackResponse {
    pub id: String,
}

pub type CurrentRecordingActionResult = bool;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum CurrentClientMessage {
    #[serde(rename = "BROADCAST")]
    Broadcast(JsonPayload),
    #[serde(rename = "CONSUMPTION_CHANGE")]
    UpdateDownloadState(CurrentDownloadStateChangePayload),
    #[serde(rename = "C_INFO_CHANGE")]
    UpdateSessionInfo(CurrentSessionInfoUpdatePayload),
    #[serde(rename = "PRODUCTION_CHANGE")]
    UpdateUploadState(CurrentUploadStateChangePayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum CurrentClientRequest {
    #[serde(rename = "CONNECT_CTS_TRANSPORT")]
    ConnectUploadTransport(CurrentTransportConnectPayload),
    #[serde(rename = "CONNECT_STC_TRANSPORT")]
    ConnectDownloadTransport(CurrentTransportConnectPayload),
    #[serde(rename = "INIT_PRODUCER")]
    PublishTrack(CurrentPublishTrackPayload),
    #[serde(rename = "START_RECORDING")]
    StartRecording(CurrentStartRecordingPayload),
    #[serde(rename = "STOP_RECORDING")]
    StopRecording,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum CurrentServerMessage {
    #[serde(rename = "BROADCAST")]
    Broadcast(CurrentBroadcastPayload),
    #[serde(rename = "SESSION_LEAVE")]
    SessionDeparted(CurrentSessionDeparturePayload),
    #[serde(rename = "S_INFO_CHANGE")]
    SessionInfoChanged(CurrentSessionInfoSnapshotById),
    #[serde(rename = "CH_INFO_CHANGE")]
    ChannelStateChanged(RecordingStateUpdate),
}

#[allow(
    clippy::large_enum_variant,
    reason = "This enum mirrors the current wire request catalog; boxing would add indirection without clarifying the compatibility layer."
)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum CurrentServerRequest {
    #[serde(rename = "INIT_CONSUMER")]
    BootstrapRemoteTrack(CurrentRemoteTrackBootstrapPayload),
    #[serde(rename = "INIT_TRANSPORTS")]
    BootstrapTransports(CurrentTransportBootstrapPayload),
    #[serde(rename = "PING")]
    Ping,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::{Value, json};

    use super::{
        CurrentBroadcastPayload, CurrentClientMessage, CurrentClientRequest,
        CurrentDownloadStateChangePayload, CurrentPublishTrackPayload, CurrentPublishTrackResponse,
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentServerRequest,
        CurrentSessionDeparturePayload, CurrentSessionInfoSnapshotById,
        CurrentSessionInfoUpdatePayload, CurrentStartRecordingPayload, CurrentStartupPayload,
        CurrentTransportBootstrapPayload, CurrentTransportConnectPayload,
        CurrentUploadStateChangePayload, CurrentWebSocketCloseCode, CurrentWebSocketCredentials,
        CurrentWebSocketLifecycleState,
    };
    use crate::signaling::shared::{
        AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, SessionId,
        SessionInfo, SessionPermissions, StopCode, StreamType,
    };
    use crate::signaling::webrtc::{
        DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, MediaKind, PublishOptions,
        PublishOptionsByMediaKind, RtpCapabilities, RtpParameters, SctpParameters,
        TransportBootstrap,
    };

    fn assert_round_trip<T>(value: &T, expected_json: Value) -> serde_json::Result<()>
    where
        T: Debug + DeserializeOwned + PartialEq + Serialize,
    {
        assert_eq!(serde_json::to_value(value)?, expected_json);
        assert_eq!(serde_json::from_value::<T>(expected_json)?, *value);
        Ok(())
    }

    fn sample_transport_bootstrap(id: &str) -> TransportBootstrap {
        TransportBootstrap {
            id: id.to_owned(),
            ice_parameters: IceParameters(json!({
                "usernameFragment": "ufrag",
                "password": "pwd",
                "iceLite": true
            })),
            ice_candidates: vec![IceCandidate {
                foundation: String::from("foundation"),
                priority: 1,
                ip: String::from("203.0.113.10"),
                protocol: String::from("udp"),
                port: 40_000,
                candidate_type: String::from("host"),
            }],
            dtls_parameters: DtlsParameters {
                role: String::from("auto"),
                fingerprints: vec![DtlsFingerprint {
                    algorithm: String::from("sha-256"),
                    value: String::from("AA:BB:CC"),
                }],
            },
            sctp_parameters: SctpParameters(json!({
                "port": 5000,
                "OS": 1024,
                "MIS": 1024,
                "maxMessageSize": 262_144
            })),
        }
    }

    #[test]
    fn current_credentials_round_trip() -> serde_json::Result<()> {
        let credentials = CurrentWebSocketCredentials {
            channel_uuid: Some("31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned()),
            jwt: "signed-token".to_owned(),
        };
        assert_round_trip(
            &credentials,
            json!({
                "channelUUID": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
                "jwt": "signed-token"
            }),
        )
    }

    #[test]
    fn startup_payload_round_trips() -> serde_json::Result<()> {
        let startup = CurrentStartupPayload {
            available_features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: false,
                video_recording: false,
            },
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
        };
        assert_round_trip(
            &startup,
            json!({
                "availableFeatures": {
                    "rtc": true,
                    "transcription": false,
                    "audioRecording": false,
                    "videoRecording": false
                },
                "recordingState": {
                    "recording": false,
                    "audio": false,
                    "transcription": false,
                    "video": false
                }
            }),
        )?;
        assert_eq!(
            u16::from(CurrentWebSocketCloseCode::AuthenticationFailed),
            4106
        );
        assert_eq!(
            CurrentWebSocketLifecycleState::MessageLoop,
            CurrentWebSocketLifecycleState::MessageLoop
        );
        Ok(())
    }

    #[test]
    fn current_client_messages_round_trip() -> serde_json::Result<()> {
        assert_round_trip(
            &CurrentClientMessage::Broadcast(json!({ "nested": [1, 2, 3] })),
            json!({
                "name": "BROADCAST",
                "payload": { "nested": [1, 2, 3] }
            }),
        )?;

        assert_round_trip(
            &CurrentClientMessage::UpdateDownloadState(CurrentDownloadStateChangePayload {
                session_id: SessionId::Integer(3),
                states: DownloadStates {
                    audio: Some(true),
                    camera: Some(false),
                    screen: None,
                },
            }),
            json!({
                "name": "CONSUMPTION_CHANGE",
                "payload": {
                    "sessionId": 3,
                    "states": {
                        "audio": true,
                        "camera": false
                    }
                }
            }),
        )?;

        assert_round_trip(
            &CurrentClientMessage::UpdateSessionInfo(CurrentSessionInfoUpdatePayload {
                info: SessionInfo {
                    is_talking: Some(true),
                    is_camera_on: Some(false),
                    is_screen_sharing_on: Some(false),
                    is_self_muted: Some(true),
                    is_deaf: Some(false),
                    is_raising_hand: Some(false),
                },
                need_refresh: Some(true),
            }),
            json!({
                "name": "C_INFO_CHANGE",
                "payload": {
                    "info": {
                        "isTalking": true,
                        "isCameraOn": false,
                        "isScreenSharingOn": false,
                        "isSelfMuted": true,
                        "isDeaf": false,
                        "isRaisingHand": false
                    },
                    "needRefresh": true
                }
            }),
        )?;

        assert_round_trip(
            &CurrentClientMessage::UpdateUploadState(CurrentUploadStateChangePayload {
                stream_type: StreamType::Camera,
                active: false,
            }),
            json!({
                "name": "PRODUCTION_CHANGE",
                "payload": {
                    "type": "camera",
                    "active": false
                }
            }),
        )
    }

    #[test]
    fn current_client_requests_round_trip() -> serde_json::Result<()> {
        let connect_payload = CurrentTransportConnectPayload {
            dtls_parameters: DtlsParameters {
                role: String::from("client"),
                fingerprints: vec![DtlsFingerprint {
                    algorithm: String::from("sha-256"),
                    value: String::from("AA:BB:CC"),
                }],
            },
            sdp_offer: None,
        };
        assert_round_trip(
            &CurrentClientRequest::ConnectUploadTransport(connect_payload.clone()),
            json!({
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": {
                    "dtlsParameters": {
                        "role": "client",
                        "fingerprints": [{
                            "algorithm": "sha-256",
                            "value": "AA:BB:CC"
                        }]
                    }
                }
            }),
        )?;

        assert_round_trip(
            &CurrentClientRequest::ConnectDownloadTransport(connect_payload),
            json!({
                "name": "CONNECT_STC_TRANSPORT",
                "payload": {
                    "dtlsParameters": {
                        "role": "client",
                        "fingerprints": [{
                            "algorithm": "sha-256",
                            "value": "AA:BB:CC"
                        }]
                    }
                }
            }),
        )?;

        assert_round_trip(
            &CurrentClientRequest::PublishTrack(CurrentPublishTrackPayload {
                stream_type: StreamType::Audio,
                media_kind: MediaKind::Audio,
                rtp_parameters: RtpParameters(json!({
                    "mid": "0",
                    "codecs": []
                })),
            }),
            json!({
                "name": "INIT_PRODUCER",
                "payload": {
                    "type": "audio",
                    "kind": "audio",
                    "rtpParameters": {
                        "mid": "0",
                        "codecs": []
                    }
                }
            }),
        )?;

        assert_round_trip(
            &CurrentClientRequest::StartRecording(CurrentStartRecordingPayload {
                audio: Some(true),
                video: Some(false),
                transcription: Some(true),
            }),
            json!({
                "name": "START_RECORDING",
                "payload": {
                    "audio": true,
                    "video": false,
                    "transcription": true
                }
            }),
        )?;

        assert_round_trip(
            &CurrentClientRequest::StopRecording,
            json!({ "name": "STOP_RECORDING" }),
        )?;

        assert_round_trip(
            &CurrentPublishTrackResponse {
                id: "producer-1".to_owned(),
            },
            json!({ "id": "producer-1" }),
        )
    }

    #[test]
    fn current_transport_connect_payload_round_trip_with_sdp_offer() -> serde_json::Result<()> {
        assert_round_trip(
            &CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
                dtls_parameters: DtlsParameters {
                    role: String::from("client"),
                    fingerprints: vec![DtlsFingerprint {
                        algorithm: String::from("sha-256"),
                        value: String::from("AA:BB:CC"),
                    }],
                },
                sdp_offer: Some(String::from(
                    "v=0\r\ns=-\r\nt=0 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n",
                )),
            }),
            json!({
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": {
                    "dtlsParameters": {
                        "role": "client",
                        "fingerprints": [{
                            "algorithm": "sha-256",
                            "value": "AA:BB:CC"
                        }]
                    },
                    "sdpOffer": "v=0\r\ns=-\r\nt=0 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"
                }
            }),
        )
    }

    #[test]
    fn current_server_messages_round_trip() -> serde_json::Result<()> {
        assert_round_trip(
            &CurrentServerMessage::Broadcast(CurrentBroadcastPayload {
                sender_id: SessionId::String("guest-9".to_owned()),
                message: json!({ "kind": "chat", "body": "hello" }),
            }),
            json!({
                "name": "BROADCAST",
                "payload": {
                    "senderId": "guest-9",
                    "message": {
                        "kind": "chat",
                        "body": "hello"
                    }
                }
            }),
        )?;

        assert_round_trip(
            &CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                session_id: SessionId::Integer(5),
            }),
            json!({
                "name": "SESSION_LEAVE",
                "payload": {
                    "sessionId": 5
                }
            }),
        )?;

        let info_change: CurrentSessionInfoSnapshotById = BTreeMap::from([(
            "5".to_owned(),
            SessionInfo {
                is_talking: Some(true),
                is_camera_on: Some(true),
                ..SessionInfo::default()
            },
        )]);
        assert_round_trip(
            &CurrentServerMessage::SessionInfoChanged(info_change),
            json!({
                "name": "S_INFO_CHANGE",
                "payload": {
                    "5": {
                        "isTalking": true,
                        "isCameraOn": true
                    }
                }
            }),
        )?;

        assert_round_trip(
            &CurrentServerMessage::ChannelStateChanged(RecordingStateUpdate {
                state: RecordingState {
                    recording: Some(true),
                    audio: Some(true),
                    transcription: Some(false),
                    video: Some(true),
                },
                stop_code: Some(StopCode::UserRequest),
            }),
            json!({
                "name": "CH_INFO_CHANGE",
                "payload": {
                    "state": {
                        "recording": true,
                        "audio": true,
                        "transcription": false,
                        "video": true
                    },
                    "stopCode": "user_request"
                }
            }),
        )
    }

    #[test]
    fn current_server_remote_track_bootstrap_round_trip() -> serde_json::Result<()> {
        assert_round_trip(
            &CurrentServerRequest::BootstrapRemoteTrack(CurrentRemoteTrackBootstrapPayload {
                id: "consumer-1".to_owned(),
                media_kind: MediaKind::Video,
                source_id: "producer-9".to_owned(),
                rtp_parameters: RtpParameters(json!({
                    "mid": "1",
                    "codecs": []
                })),
                session_id: SessionId::Integer(17),
                active: true,
                stream_type: StreamType::Screen,
            }),
            json!({
                "name": "INIT_CONSUMER",
                "payload": {
                    "id": "consumer-1",
                    "kind": "video",
                    "producerId": "producer-9",
                    "rtpParameters": {
                        "mid": "1",
                        "codecs": []
                    },
                    "sessionId": 17,
                    "active": true,
                    "type": "screen"
                }
            }),
        )
    }

    #[test]
    fn current_server_transport_bootstrap_round_trip() -> serde_json::Result<()> {
        assert_round_trip(
            &CurrentServerRequest::BootstrapTransports(CurrentTransportBootstrapPayload {
                router_capabilities: RtpCapabilities(json!({
                    "codecs": [],
                    "headerExtensions": []
                })),
                download_transport: sample_transport_bootstrap("stc-1"),
                upload_transport: sample_transport_bootstrap("cts-1"),
                publish_options_by_media_kind: PublishOptionsByMediaKind {
                    audio: PublishOptions(json!({ "stopTracks": false })),
                    video: PublishOptions(json!({ "stopTracks": false, "zeroRtpOnPause": true })),
                },
            }),
            json!({
                "name": "INIT_TRANSPORTS",
                "payload": {
                    "capabilities": {
                        "codecs": [],
                        "headerExtensions": []
                    },
                    "stcConfig": {
                        "id": "stc-1",
                        "iceParameters": {
                            "usernameFragment": "ufrag",
                            "password": "pwd",
                            "iceLite": true
                        },
                        "iceCandidates": [{
                            "foundation": "foundation",
                            "priority": 1,
                            "ip": "203.0.113.10",
                            "protocol": "udp",
                            "port": 40000,
                            "type": "host"
                        }],
                        "dtlsParameters": {
                            "role": "auto",
                            "fingerprints": [{
                                "algorithm": "sha-256",
                                "value": "AA:BB:CC"
                            }]
                        },
                        "sctpParameters": {
                            "port": 5000,
                            "OS": 1024,
                            "MIS": 1024,
                            "maxMessageSize": 262_144
                        }
                    },
                    "ctsConfig": {
                        "id": "cts-1",
                        "iceParameters": {
                            "usernameFragment": "ufrag",
                            "password": "pwd",
                            "iceLite": true
                        },
                        "iceCandidates": [{
                            "foundation": "foundation",
                            "priority": 1,
                            "ip": "203.0.113.10",
                            "protocol": "udp",
                            "port": 40000,
                            "type": "host"
                        }],
                        "dtlsParameters": {
                            "role": "auto",
                            "fingerprints": [{
                                "algorithm": "sha-256",
                                "value": "AA:BB:CC"
                            }]
                        },
                        "sctpParameters": {
                            "port": 5000,
                            "OS": 1024,
                            "MIS": 1024,
                            "maxMessageSize": 262_144
                        }
                    },
                    "producerOptionsByKind": {
                        "audio": {
                            "stopTracks": false
                        },
                        "video": {
                            "stopTracks": false,
                            "zeroRtpOnPause": true
                        }
                    }
                }
            }),
        )?;

        assert_round_trip(&CurrentServerRequest::Ping, json!({ "name": "PING" }))
    }

    #[test]
    fn shared_support_types_round_trip() -> serde_json::Result<()> {
        assert_round_trip(
            &SessionPermissions {
                transcription: Some(true),
                audio_recording: Some(false),
                video_recording: Some(true),
            },
            json!({
                "transcription": true,
                "audioRecording": false,
                "videoRecording": true
            }),
        )
    }
}
