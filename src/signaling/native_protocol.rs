use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
    SessionId, SessionInfo, StreamType,
};

pub type NativeEnvelopeBatch = Vec<NativeEnvelope>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NativeRequestId(String);

impl NativeRequestId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeEnvelope {
    #[serde(rename = "t")]
    pub tag: String,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<NativeRequestId>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub response_to: Option<NativeRequestId>,
}

impl NativeEnvelope {
    fn message(tag: &str, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: None,
            response_to: None,
        }
    }

    fn request(tag: &str, request_id: NativeRequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: Some(request_id),
            response_to: None,
        }
    }

    fn response(tag: &str, response_to: NativeRequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: None,
            response_to: Some(response_to),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum NativeWebSocketCloseCode {
    Clean = 1000,
    Leaving = 1001,
    ProtocolError = 1002,
    Error = 1011,
    AuthFailed = 4001,
    AuthTimeout = 4002,
    Kicked = 4003,
    ChannelFull = 4004,
}

impl From<NativeWebSocketCloseCode> for u16 {
    fn from(value: NativeWebSocketCloseCode) -> Self {
        match value {
            NativeWebSocketCloseCode::Clean => 1000,
            NativeWebSocketCloseCode::Leaving => 1001,
            NativeWebSocketCloseCode::ProtocolError => 1002,
            NativeWebSocketCloseCode::Error => 1011,
            NativeWebSocketCloseCode::AuthFailed => 4001,
            NativeWebSocketCloseCode::AuthTimeout => 4002,
            NativeWebSocketCloseCode::Kicked => 4003,
            NativeWebSocketCloseCode::ChannelFull => 4004,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAuthPayload {
    pub jwt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativePeerSnapshot {
    pub session_id: SessionId,
    #[serde(default)]
    pub info: SessionInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWelcomePayload {
    pub features: AvailableFeatures,
    pub recording: RecordingState,
    pub peers: Vec<NativePeerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSessionDescriptionPayload {
    pub sdp: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeStreamIntentPayload {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSubscribePayload {
    pub session_id: SessionId,
    #[serde(flatten)]
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTrackBinding {
    pub mid: String,
    pub session_id: SessionId,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativePeerInfoPayload {
    pub session_id: SessionId,
    pub info: SessionInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativePeerLeftPayload {
    pub session_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeClientBroadcastPayload {
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeServerBroadcastPayload {
    pub sender_id: SessionId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeRecordingOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeRecordingActionResult {
    pub ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeClientMessage {
    Auth(NativeAuthPayload),
    Publish(NativeStreamIntentPayload),
    Unpublish(NativeStreamIntentPayload),
    Subscribe(NativeSubscribePayload),
    Info(SessionInfo),
    Broadcast(NativeClientBroadcastPayload),
}

impl NativeClientMessage {
    fn tag(&self) -> &'static str {
        match self {
            Self::Auth(_) => "auth",
            Self::Publish(_) => "publish",
            Self::Unpublish(_) => "unpublish",
            Self::Subscribe(_) => "subscribe",
            Self::Info(_) => "info",
            Self::Broadcast(_) => "broadcast",
        }
    }

    fn into_envelope(self) -> Result<NativeEnvelope, serde_json::Error> {
        Ok(NativeEnvelope::message(
            self.tag(),
            Some(match self {
                Self::Auth(payload) => serde_json::to_value(payload)?,
                Self::Publish(payload) | Self::Unpublish(payload) => serde_json::to_value(payload)?,
                Self::Subscribe(payload) => serde_json::to_value(payload)?,
                Self::Info(payload) => serde_json::to_value(payload)?,
                Self::Broadcast(payload) => serde_json::to_value(payload)?,
            }),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeClientRequest {
    StartRecording(NativeRecordingOptions),
    StopRecording,
}

impl NativeClientRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::StartRecording(_) => "startrecording",
            Self::StopRecording => "stoprecording",
        }
    }

    fn into_envelope(
        self,
        request_id: NativeRequestId,
    ) -> Result<NativeEnvelope, serde_json::Error> {
        Ok(NativeEnvelope::request(
            self.tag(),
            request_id,
            match self {
                Self::StartRecording(payload) => Some(serde_json::to_value(payload)?),
                Self::StopRecording => None,
            },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeClientResponse {
    Offer(NativeSessionDescriptionPayload),
    Renegotiate(NativeSessionDescriptionPayload),
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeServerMessage {
    Welcome(NativeWelcomePayload),
    Tracks(Vec<NativeTrackBinding>),
    PeerInfo(NativePeerInfoPayload),
    PeerJoined(NativePeerInfoPayload),
    PeerLeft(NativePeerLeftPayload),
    Broadcast(NativeServerBroadcastPayload),
    RecordingChange(RecordingStateUpdate),
}

impl NativeServerMessage {
    fn tag(&self) -> &'static str {
        match self {
            Self::Welcome(_) => "welcome",
            Self::Tracks(_) => "tracks",
            Self::PeerInfo(_) => "peerinfo",
            Self::PeerJoined(_) => "peerjoined",
            Self::PeerLeft(_) => "peerleft",
            Self::Broadcast(_) => "broadcast",
            Self::RecordingChange(_) => "recordingchange",
        }
    }

    /// Serialize a server push message into the native websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self) -> Result<NativeEnvelope, serde_json::Error> {
        Ok(NativeEnvelope::message(
            self.tag(),
            Some(match self {
                Self::Welcome(payload) => serde_json::to_value(payload)?,
                Self::Tracks(payload) => serde_json::to_value(payload)?,
                Self::PeerInfo(payload) | Self::PeerJoined(payload) => {
                    serde_json::to_value(payload)?
                }
                Self::PeerLeft(payload) => serde_json::to_value(payload)?,
                Self::Broadcast(payload) => serde_json::to_value(payload)?,
                Self::RecordingChange(payload) => serde_json::to_value(payload)?,
            }),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeServerRequest {
    Offer(NativeSessionDescriptionPayload),
    Renegotiate(NativeSessionDescriptionPayload),
    Ping,
}

impl NativeServerRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::Offer(_) => "offer",
            Self::Renegotiate(_) => "renegotiate",
            Self::Ping => "ping",
        }
    }

    /// Serialize a server request into the native websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(
        self,
        request_id: NativeRequestId,
    ) -> Result<NativeEnvelope, serde_json::Error> {
        Ok(NativeEnvelope::request(
            self.tag(),
            request_id,
            match self {
                Self::Offer(payload) | Self::Renegotiate(payload) => {
                    Some(serde_json::to_value(payload)?)
                }
                Self::Ping => None,
            },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeServerResponse {
    StartRecording(NativeRecordingActionResult),
    StopRecording(NativeRecordingActionResult),
}

impl NativeServerResponse {
    fn tag(&self) -> &'static str {
        match self {
            Self::StartRecording(_) => "startrecording",
            Self::StopRecording(_) => "stoprecording",
        }
    }

    /// Serialize a server response into the native websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(
        self,
        response_to: NativeRequestId,
    ) -> Result<NativeEnvelope, serde_json::Error> {
        Ok(NativeEnvelope::response(
            self.tag(),
            response_to,
            Some(match self {
                Self::StartRecording(payload) | Self::StopRecording(payload) => {
                    serde_json::to_value(payload)?
                }
            }),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeClientEnvelope {
    Message(NativeClientMessage),
    Request {
        request_id: NativeRequestId,
        request: NativeClientRequest,
    },
    Response {
        response_to: NativeRequestId,
        response: NativeClientResponse,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeEnvelopeDecodeError {
    InvalidRoutingMetadata,
    UnknownTag(String),
    InvalidPayload(String),
    UnexpectedPayload(String),
}

impl NativeClientEnvelope {
    /// Serialize a typed client-side envelope into the native websocket shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self) -> Result<NativeEnvelope, serde_json::Error> {
        match self {
            Self::Message(message) => message.into_envelope(),
            Self::Request {
                request_id,
                request,
            } => request.into_envelope(request_id),
            Self::Response {
                response_to,
                response,
            } => match response {
                NativeClientResponse::Offer(payload) => Ok(NativeEnvelope::response(
                    "offer",
                    response_to,
                    Some(serde_json::to_value(payload)?),
                )),
                NativeClientResponse::Renegotiate(payload) => Ok(NativeEnvelope::response(
                    "renegotiate",
                    response_to,
                    Some(serde_json::to_value(payload)?),
                )),
                NativeClientResponse::Ping => {
                    Ok(NativeEnvelope::response("ping", response_to, None))
                }
            },
        }
    }

    /// Decode a raw websocket envelope into the typed native client contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the routing metadata is invalid, the tag is unknown,
    /// or the payload does not match the declared message shape.
    pub fn decode(envelope: NativeEnvelope) -> Result<Self, NativeEnvelopeDecodeError> {
        match (envelope.request_id, envelope.response_to) {
            (Some(_), Some(_)) => Err(NativeEnvelopeDecodeError::InvalidRoutingMetadata),
            (None, Some(response_to)) => {
                decode_client_response(response_to, &envelope.tag, envelope.payload)
            }
            (Some(request_id), None) => {
                decode_client_request(request_id, &envelope.tag, envelope.payload)
            }
            (None, None) => decode_client_message(&envelope.tag, envelope.payload),
        }
    }
}

fn decode_client_message(
    tag: &str,
    payload: Option<Value>,
) -> Result<NativeClientEnvelope, NativeEnvelopeDecodeError> {
    match tag {
        "auth" => Ok(NativeClientEnvelope::Message(NativeClientMessage::Auth(
            parse_payload(tag, payload)?,
        ))),
        "publish" => Ok(NativeClientEnvelope::Message(NativeClientMessage::Publish(
            parse_payload(tag, payload)?,
        ))),
        "unpublish" => Ok(NativeClientEnvelope::Message(
            NativeClientMessage::Unpublish(parse_payload(tag, payload)?),
        )),
        "subscribe" => Ok(NativeClientEnvelope::Message(
            NativeClientMessage::Subscribe(parse_payload(tag, payload)?),
        )),
        "info" => Ok(NativeClientEnvelope::Message(NativeClientMessage::Info(
            parse_payload(tag, payload)?,
        ))),
        "broadcast" => Ok(NativeClientEnvelope::Message(
            NativeClientMessage::Broadcast(parse_payload(tag, payload)?),
        )),
        _ => Err(NativeEnvelopeDecodeError::UnknownTag(tag.to_owned())),
    }
}

fn decode_client_request(
    request_id: NativeRequestId,
    tag: &str,
    payload: Option<Value>,
) -> Result<NativeClientEnvelope, NativeEnvelopeDecodeError> {
    match tag {
        "startrecording" => Ok(NativeClientEnvelope::Request {
            request_id,
            request: NativeClientRequest::StartRecording(parse_payload(tag, payload)?),
        }),
        "stoprecording" => {
            ensure_empty_payload(tag, payload.as_ref())?;
            Ok(NativeClientEnvelope::Request {
                request_id,
                request: NativeClientRequest::StopRecording,
            })
        }
        _ => Err(NativeEnvelopeDecodeError::UnknownTag(tag.to_owned())),
    }
}

fn decode_client_response(
    response_to: NativeRequestId,
    tag: &str,
    payload: Option<Value>,
) -> Result<NativeClientEnvelope, NativeEnvelopeDecodeError> {
    let response = match tag {
        "offer" => NativeClientResponse::Offer(parse_payload(tag, payload)?),
        "renegotiate" => NativeClientResponse::Renegotiate(parse_payload(tag, payload)?),
        "ping" => {
            ensure_empty_payload(tag, payload.as_ref())?;
            NativeClientResponse::Ping
        }
        _ => return Err(NativeEnvelopeDecodeError::UnknownTag(tag.to_owned())),
    };
    Ok(NativeClientEnvelope::Response {
        response_to,
        response,
    })
}

fn parse_payload<T: DeserializeOwned>(
    tag: &str,
    payload: Option<Value>,
) -> Result<T, NativeEnvelopeDecodeError> {
    serde_json::from_value(
        payload.ok_or_else(|| NativeEnvelopeDecodeError::InvalidPayload(tag.to_owned()))?,
    )
    .map_err(|_error| NativeEnvelopeDecodeError::InvalidPayload(tag.to_owned()))
}

fn ensure_empty_payload(
    tag: &str,
    payload: Option<&Value>,
) -> Result<(), NativeEnvelopeDecodeError> {
    if payload.is_some() {
        return Err(NativeEnvelopeDecodeError::UnexpectedPayload(tag.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        NativeAuthPayload, NativeClientEnvelope, NativeClientMessage, NativeClientRequest,
        NativeClientResponse, NativeEnvelope, NativeEnvelopeDecodeError, NativePeerInfoPayload,
        NativePeerLeftPayload, NativePeerSnapshot, NativeRecordingActionResult,
        NativeRecordingOptions, NativeRequestId, NativeServerBroadcastPayload, NativeServerMessage,
        NativeServerRequest, NativeServerResponse, NativeSessionDescriptionPayload,
        NativeStreamIntentPayload, NativeSubscribePayload, NativeTrackBinding,
        NativeWebSocketCloseCode, NativeWelcomePayload,
    };
    use crate::signaling::shared::{
        AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, SessionId,
        SessionInfo, StopCode, StreamType,
    };

    #[test]
    fn native_close_codes_follow_phase_nine_contract() {
        assert_eq!(u16::from(NativeWebSocketCloseCode::AuthFailed), 4001);
        assert_eq!(u16::from(NativeWebSocketCloseCode::AuthTimeout), 4002);
        assert_eq!(u16::from(NativeWebSocketCloseCode::Kicked), 4003);
        assert_eq!(u16::from(NativeWebSocketCloseCode::ChannelFull), 4004);
    }

    #[test]
    fn native_client_auth_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
        let envelope =
            NativeClientEnvelope::Message(NativeClientMessage::Auth(NativeAuthPayload {
                jwt: String::from("jwt-token"),
                channel: Some(String::from("channel-1")),
            }))
            .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&envelope)?,
            json!({
                "t": "auth",
                "p": {
                    "jwt": "jwt-token",
                    "channel": "channel-1",
                },
            })
        );
        Ok(())
    }

    #[test]
    fn native_start_recording_request_decodes_with_request_id() {
        let decoded = NativeClientEnvelope::decode(NativeEnvelope {
            tag: String::from("startrecording"),
            payload: Some(json!({
                "audio": true,
                "video": false,
            })),
            request_id: Some(NativeRequestId::new("3")),
            response_to: None,
        });

        assert_eq!(
            decoded,
            Ok(NativeClientEnvelope::Request {
                request_id: NativeRequestId::new("3"),
                request: NativeClientRequest::StartRecording(NativeRecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: None,
                }),
            })
        );
    }

    #[test]
    fn native_offer_response_decodes_with_response_id() {
        let decoded = NativeClientEnvelope::decode(NativeEnvelope {
            tag: String::from("offer"),
            payload: Some(json!({
                "sdp": "v=0\r\n",
            })),
            request_id: None,
            response_to: Some(NativeRequestId::new("1")),
        });

        assert_eq!(
            decoded,
            Ok(NativeClientEnvelope::Response {
                response_to: NativeRequestId::new("1"),
                response: NativeClientResponse::Offer(NativeSessionDescriptionPayload {
                    sdp: String::from("v=0\r\n"),
                }),
            })
        );
    }

    #[test]
    fn native_subscribe_message_decodes_flat_download_state_shape() {
        let decoded = NativeClientEnvelope::decode(NativeEnvelope {
            tag: String::from("subscribe"),
            payload: Some(json!({
                "sessionId": 7,
                "audio": true,
                "camera": false,
            })),
            request_id: None,
            response_to: None,
        });

        assert_eq!(
            decoded,
            Ok(NativeClientEnvelope::Message(
                NativeClientMessage::Subscribe(NativeSubscribePayload {
                    session_id: SessionId::Integer(7),
                    states: DownloadStates {
                        audio: Some(true),
                        camera: Some(false),
                        screen: None,
                    },
                })
            ))
        );
    }

    #[test]
    fn native_decode_rejects_envelopes_with_both_request_and_response_ids() {
        let decoded = NativeClientEnvelope::decode(NativeEnvelope {
            tag: String::from("ping"),
            payload: None,
            request_id: Some(NativeRequestId::new("1")),
            response_to: Some(NativeRequestId::new("2")),
        });

        assert_eq!(
            decoded,
            Err(NativeEnvelopeDecodeError::InvalidRoutingMetadata)
        );
    }

    #[test]
    fn native_welcome_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
        let welcome = NativeServerMessage::Welcome(NativeWelcomePayload {
            features: AvailableFeatures {
                rtc: true,
                transcription: false,
                audio_recording: true,
                video_recording: false,
            },
            recording: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            peers: vec![NativePeerSnapshot {
                session_id: SessionId::String(String::from("alice")),
                info: SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
            }],
        })
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&welcome)?,
            json!({
                "t": "welcome",
                "p": {
                    "features": {
                        "rtc": true,
                        "transcription": false,
                        "audioRecording": true,
                        "videoRecording": false,
                    },
                    "recording": {
                        "recording": false,
                        "audio": false,
                        "transcription": false,
                        "video": false,
                    },
                    "peers": [{
                        "sessionId": "alice",
                        "info": {
                            "isTalking": true,
                        },
                    }],
                },
            })
        );
        Ok(())
    }

    #[test]
    fn native_publish_message_uses_stream_type_field() -> serde_json::Result<()> {
        let envelope = NativeClientMessage::Publish(NativeStreamIntentPayload {
            stream_type: StreamType::Screen,
        })
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&envelope)?,
            json!({
                "t": "publish",
                "p": {
                    "type": "screen",
                },
            })
        );
        Ok(())
    }

    #[test]
    fn native_server_track_and_peer_messages_round_trip_to_wire_envelopes() -> serde_json::Result<()>
    {
        let track_update = NativeServerMessage::Tracks(vec![NativeTrackBinding {
            mid: String::from("0"),
            session_id: SessionId::Integer(5),
            stream_type: StreamType::Camera,
            active: true,
        }])
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&track_update)?,
            json!({
                "t": "tracks",
                "p": [{
                    "mid": "0",
                    "sessionId": 5,
                    "type": "camera",
                    "active": true,
                }],
            })
        );

        let peer_joined = NativeServerMessage::PeerJoined(NativePeerInfoPayload {
            session_id: SessionId::Integer(9),
            info: SessionInfo {
                is_camera_on: Some(true),
                ..SessionInfo::default()
            },
        })
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&peer_joined)?,
            json!({
                "t": "peerjoined",
                "p": {
                    "sessionId": 9,
                    "info": {
                        "isCameraOn": true,
                    },
                },
            })
        );

        let peer_left = NativeServerMessage::PeerLeft(NativePeerLeftPayload {
            session_id: SessionId::Integer(9),
        })
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&peer_left)?,
            json!({
                "t": "peerleft",
                "p": {
                    "sessionId": 9,
                },
            })
        );
        Ok(())
    }

    #[test]
    fn native_server_broadcast_and_recording_messages_round_trip_to_wire_envelopes()
    -> serde_json::Result<()> {
        let broadcast = NativeServerMessage::Broadcast(NativeServerBroadcastPayload {
            sender_id: SessionId::String(String::from("bob")),
            message: json!({"text": "hello"}),
        })
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&broadcast)?,
            json!({
                "t": "broadcast",
                "p": {
                    "senderId": "bob",
                    "message": {
                        "text": "hello",
                    },
                },
            })
        );

        let recording_change = NativeServerMessage::RecordingChange(RecordingStateUpdate {
            state: RecordingState {
                recording: Some(true),
                audio: Some(true),
                transcription: Some(false),
                video: Some(true),
            },
            stop_code: Some(StopCode::UserRequest),
        })
        .into_envelope()?;
        assert_eq!(
            serde_json::to_value(&recording_change)?,
            json!({
                "t": "recordingchange",
                "p": {
                    "state": {
                        "recording": true,
                        "audio": true,
                        "transcription": false,
                        "video": true,
                    },
                    "stopCode": "user_request",
                },
            })
        );
        Ok(())
    }

    #[test]
    fn native_server_requests_and_responses_round_trip_to_wire_envelopes() -> serde_json::Result<()>
    {
        let offer = NativeServerRequest::Offer(NativeSessionDescriptionPayload {
            sdp: String::from("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"),
        })
        .into_envelope(NativeRequestId::new("1"))?;
        assert_eq!(
            serde_json::to_value(&offer)?,
            json!({
                "t": "offer",
                "q": "1",
                "p": {
                    "sdp": "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n",
                },
            })
        );

        let ping = NativeServerRequest::Ping.into_envelope(NativeRequestId::new("4"))?;
        assert_eq!(
            serde_json::to_value(&ping)?,
            json!({
                "t": "ping",
                "q": "4",
            })
        );

        let start_recording =
            NativeServerResponse::StartRecording(NativeRecordingActionResult { ok: true })
                .into_envelope(NativeRequestId::new("3"))?;
        assert_eq!(
            serde_json::to_value(&start_recording)?,
            json!({
                "t": "startrecording",
                "r": "3",
                "p": {
                    "ok": true,
                },
            })
        );
        Ok(())
    }
}
