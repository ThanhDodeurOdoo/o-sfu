#![allow(
    clippy::large_enum_variant,
    reason = "the copied legacy protocol fixtures intentionally mirror the historical wire payload layout for differential tests"
)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use o_sfu::signaling::{
    shared::{
        DownloadStates, JsonPayload, RecordingStateUpdate, SessionId, SessionInfo, StreamType,
    },
    webrtc::{
        DtlsParameters, IceParameters, MediaKind, PublishOptionsByMediaKind, RtpCapabilities,
        RtpParameters, TransportBootstrap,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentWebSocketCredentials {
    #[serde(rename = "channelUUID", skip_serializing_if = "Option::is_none")]
    pub channel_uuid: Option<String>,
    pub jwt: String,
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
    #[serde(rename = "iceParameters", skip_serializing_if = "Option::is_none")]
    pub ice_parameters: Option<IceParameters>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", content = "payload")]
pub enum CurrentClientMessage {
    #[serde(rename = "BROADCAST")]
    Broadcast(JsonPayload),
    #[serde(rename = "CONSUMPTION_CHANGE")]
    Subscribe(CurrentDownloadStateChangePayload),
    #[serde(rename = "C_INFO_CHANGE")]
    UpdateSessionInfo(CurrentSessionInfoUpdatePayload),
    #[serde(rename = "PRODUCTION_CHANGE")]
    Publish(CurrentUploadStateChangePayload),
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
