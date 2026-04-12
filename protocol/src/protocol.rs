use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::shared::{
    AvailableFeatures, DownloadStates, JsonPayload, RecordingState, RecordingStateUpdate,
    SessionId, SessionInfo, StreamType,
};

pub type EnvelopeBatch = Vec<Envelope>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(String);

impl RequestId {
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
pub struct Envelope {
    #[serde(rename = "t")]
    pub tag: String,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub response_to: Option<RequestId>,
}

impl Envelope {
    fn message(tag: &str, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: None,
            response_to: None,
        }
    }

    fn request(tag: &str, request_id: RequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: Some(request_id),
            response_to: None,
        }
    }

    fn response(tag: &str, response_to: RequestId, payload: Option<Value>) -> Self {
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
pub enum WebSocketCloseCode {
    Clean = 1000,
    Leaving = 1001,
    ProtocolError = 1002,
    Error = 1011,
    AuthFailed = 4001,
    AuthTimeout = 4002,
    Kicked = 4003,
    ChannelFull = 4004,
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
            WebSocketCloseCode::ChannelFull => 4004,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthPayload {
    pub jwt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSnapshot {
    pub session_id: SessionId,
    #[serde(default)]
    pub info: SessionInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WelcomePayload {
    pub features: AvailableFeatures,
    pub recording: RecordingState,
    pub peers: Vec<PeerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptionPayload {
    pub sdp: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamIntentPayload {
    #[serde(rename = "type")]
    pub stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribePayload {
    /// Wire shape is intentionally flat: `{ sessionId, audio?, camera?, screen? }`.
    /// Adding fields to `DownloadStates` implicitly changes the subscribe payload shape.
    pub session_id: SessionId,
    #[serde(flatten)]
    pub states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackBinding {
    pub mid: String,
    pub session_id: SessionId,
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfoPayload {
    pub session_id: SessionId,
    pub info: SessionInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerLeftPayload {
    pub session_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientBroadcastPayload {
    pub message: JsonPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerBroadcastPayload {
    pub sender_id: SessionId,
    pub message: JsonPayload,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingActionResult {
    pub ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Auth(AuthPayload),
    Publish(StreamIntentPayload),
    Unpublish(StreamIntentPayload),
    Subscribe(SubscribePayload),
    Info(SessionInfo),
    Broadcast(ClientBroadcastPayload),
}

impl ClientMessage {
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

    fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::message(
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
pub enum ClientRequest {
    StartRecording(RecordingOptions),
    StopRecording,
}

impl ClientRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::StartRecording(_) => "startrecording",
            Self::StopRecording => "stoprecording",
        }
    }

    fn into_envelope(self, request_id: RequestId) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::request(
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
pub enum ClientResponse {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    Welcome(WelcomePayload),
    Tracks(Vec<TrackBinding>),
    PeerInfo(PeerInfoPayload),
    PeerJoined(PeerInfoPayload),
    PeerLeft(PeerLeftPayload),
    Broadcast(ServerBroadcastPayload),
    RecordingChange(RecordingStateUpdate),
}

impl ServerMessage {
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

    /// Serialize a server push message into the protocol websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::message(
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
pub enum ServerRequest {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
    Ping,
}

impl ServerRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::Offer(_) => "offer",
            Self::Renegotiate(_) => "renegotiate",
            Self::Ping => "ping",
        }
    }

    /// Serialize a server request into the protocol websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self, request_id: RequestId) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::request(
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
pub enum ServerResponse {
    StartRecording(RecordingActionResult),
    StopRecording(RecordingActionResult),
}

impl ServerResponse {
    fn tag(&self) -> &'static str {
        match self {
            Self::StartRecording(_) => "startrecording",
            Self::StopRecording(_) => "stoprecording",
        }
    }

    /// Serialize a server response into the protocol websocket envelope shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self, response_to: RequestId) -> Result<Envelope, serde_json::Error> {
        Ok(Envelope::response(
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
pub enum ClientEnvelope {
    Message(ClientMessage),
    Request {
        request_id: RequestId,
        request: ClientRequest,
    },
    Response {
        response_to: RequestId,
        response: ClientResponse,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerEnvelope {
    Message(ServerMessage),
    Request {
        request_id: RequestId,
        request: ServerRequest,
    },
    Response {
        response_to: RequestId,
        response: ServerResponse,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeDecodeError {
    InvalidRoutingMetadata,
    UnknownTag(String),
    InvalidPayload(String),
    UnexpectedPayload(String),
}

impl ClientEnvelope {
    /// Serialize a typed client-side envelope into the protocol websocket shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
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
                ClientResponse::Offer(payload) => Ok(Envelope::response(
                    "offer",
                    response_to,
                    Some(serde_json::to_value(payload)?),
                )),
                ClientResponse::Renegotiate(payload) => Ok(Envelope::response(
                    "renegotiate",
                    response_to,
                    Some(serde_json::to_value(payload)?),
                )),
                ClientResponse::Ping => Ok(Envelope::response("ping", response_to, None)),
            },
        }
    }

    /// Decode a raw websocket envelope into the typed native client contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the routing metadata is invalid, the tag is unknown,
    /// or the payload does not match the declared message shape.
    pub fn decode(envelope: Envelope) -> Result<Self, EnvelopeDecodeError> {
        match (envelope.request_id, envelope.response_to) {
            (Some(_), Some(_)) => Err(EnvelopeDecodeError::InvalidRoutingMetadata),
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

impl ServerEnvelope {
    /// Serialize a typed server-side envelope into the protocol websocket shape.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized to JSON.
    pub fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
        match self {
            Self::Message(message) => message.into_envelope(),
            Self::Request {
                request_id,
                request,
            } => request.into_envelope(request_id),
            Self::Response {
                response_to,
                response,
            } => response.into_envelope(response_to),
        }
    }

    /// Decode a raw websocket envelope into the typed native server contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the routing metadata is invalid, the tag is unknown,
    /// or the payload does not match the declared message shape.
    pub fn decode(envelope: Envelope) -> Result<Self, EnvelopeDecodeError> {
        match (envelope.request_id, envelope.response_to) {
            (Some(_), Some(_)) => Err(EnvelopeDecodeError::InvalidRoutingMetadata),
            (None, Some(response_to)) => {
                decode_server_response(response_to, &envelope.tag, envelope.payload)
            }
            (Some(request_id), None) => {
                decode_server_request(request_id, &envelope.tag, envelope.payload)
            }
            (None, None) => decode_server_message(&envelope.tag, envelope.payload),
        }
    }
}

fn decode_client_message(
    tag: &str,
    payload: Option<Value>,
) -> Result<ClientEnvelope, EnvelopeDecodeError> {
    match tag {
        "auth" => Ok(ClientEnvelope::Message(ClientMessage::Auth(parse_payload(
            tag, payload,
        )?))),
        "publish" => Ok(ClientEnvelope::Message(ClientMessage::Publish(
            parse_payload(tag, payload)?,
        ))),
        "unpublish" => Ok(ClientEnvelope::Message(ClientMessage::Unpublish(
            parse_payload(tag, payload)?,
        ))),
        "subscribe" => Ok(ClientEnvelope::Message(ClientMessage::Subscribe(
            parse_payload(tag, payload)?,
        ))),
        "info" => Ok(ClientEnvelope::Message(ClientMessage::Info(parse_payload(
            tag, payload,
        )?))),
        "broadcast" => Ok(ClientEnvelope::Message(ClientMessage::Broadcast(
            parse_payload(tag, payload)?,
        ))),
        _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
    }
}

fn decode_client_request(
    request_id: RequestId,
    tag: &str,
    payload: Option<Value>,
) -> Result<ClientEnvelope, EnvelopeDecodeError> {
    match tag {
        "startrecording" => Ok(ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StartRecording(parse_payload(tag, payload)?),
        }),
        "stoprecording" => {
            ensure_empty_payload(tag, payload.as_ref())?;
            Ok(ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StopRecording,
            })
        }
        _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
    }
}

fn decode_client_response(
    response_to: RequestId,
    tag: &str,
    payload: Option<Value>,
) -> Result<ClientEnvelope, EnvelopeDecodeError> {
    let response = match tag {
        "offer" => ClientResponse::Offer(parse_payload(tag, payload)?),
        "renegotiate" => ClientResponse::Renegotiate(parse_payload(tag, payload)?),
        "ping" => {
            ensure_empty_payload(tag, payload.as_ref())?;
            ClientResponse::Ping
        }
        _ => return Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
    };
    Ok(ClientEnvelope::Response {
        response_to,
        response,
    })
}

fn decode_server_message(
    tag: &str,
    payload: Option<Value>,
) -> Result<ServerEnvelope, EnvelopeDecodeError> {
    match tag {
        "welcome" => Ok(ServerEnvelope::Message(ServerMessage::Welcome(
            parse_payload(tag, payload)?,
        ))),
        "tracks" => Ok(ServerEnvelope::Message(ServerMessage::Tracks(
            parse_payload(tag, payload)?,
        ))),
        "peerinfo" => Ok(ServerEnvelope::Message(ServerMessage::PeerInfo(
            parse_payload(tag, payload)?,
        ))),
        "peerjoined" => Ok(ServerEnvelope::Message(ServerMessage::PeerJoined(
            parse_payload(tag, payload)?,
        ))),
        "peerleft" => Ok(ServerEnvelope::Message(ServerMessage::PeerLeft(
            parse_payload(tag, payload)?,
        ))),
        "broadcast" => Ok(ServerEnvelope::Message(ServerMessage::Broadcast(
            parse_payload(tag, payload)?,
        ))),
        "recordingchange" => Ok(ServerEnvelope::Message(ServerMessage::RecordingChange(
            parse_payload(tag, payload)?,
        ))),
        _ => Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
    }
}

fn decode_server_request(
    request_id: RequestId,
    tag: &str,
    payload: Option<Value>,
) -> Result<ServerEnvelope, EnvelopeDecodeError> {
    let request = match tag {
        "offer" => ServerRequest::Offer(parse_payload(tag, payload)?),
        "renegotiate" => ServerRequest::Renegotiate(parse_payload(tag, payload)?),
        "ping" => {
            ensure_empty_payload(tag, payload.as_ref())?;
            ServerRequest::Ping
        }
        _ => return Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
    };
    Ok(ServerEnvelope::Request {
        request_id,
        request,
    })
}

fn decode_server_response(
    response_to: RequestId,
    tag: &str,
    payload: Option<Value>,
) -> Result<ServerEnvelope, EnvelopeDecodeError> {
    let response = match tag {
        "startrecording" => ServerResponse::StartRecording(parse_payload(tag, payload)?),
        "stoprecording" => ServerResponse::StopRecording(parse_payload(tag, payload)?),
        _ => return Err(EnvelopeDecodeError::UnknownTag(tag.to_owned())),
    };
    Ok(ServerEnvelope::Response {
        response_to,
        response,
    })
}

fn parse_payload<T: DeserializeOwned>(
    tag: &str,
    payload: Option<Value>,
) -> Result<T, EnvelopeDecodeError> {
    serde_json::from_value(
        payload.ok_or_else(|| EnvelopeDecodeError::InvalidPayload(tag.to_owned()))?,
    )
    .map_err(|_error| EnvelopeDecodeError::InvalidPayload(tag.to_owned()))
}

fn ensure_empty_payload(tag: &str, payload: Option<&Value>) -> Result<(), EnvelopeDecodeError> {
    if payload.is_some() {
        return Err(EnvelopeDecodeError::UnexpectedPayload(tag.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        AuthPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse, Envelope,
        EnvelopeDecodeError, PeerInfoPayload, PeerLeftPayload, PeerSnapshot, RecordingActionResult,
        RecordingOptions, RequestId, ServerBroadcastPayload, ServerEnvelope, ServerMessage,
        ServerRequest, ServerResponse, SessionDescriptionPayload, StreamIntentPayload,
        SubscribePayload, TrackBinding, WebSocketCloseCode, WelcomePayload,
    };
    use crate::shared::{
        AvailableFeatures, DownloadStates, RecordingState, RecordingStateUpdate, SessionId,
        SessionInfo, StopCode, StreamType,
    };

    #[test]
    fn protocol_close_codes_follow_phase_nine_contract() {
        assert_eq!(u16::from(WebSocketCloseCode::AuthFailed), 4001);
        assert_eq!(u16::from(WebSocketCloseCode::AuthTimeout), 4002);
        assert_eq!(u16::from(WebSocketCloseCode::Kicked), 4003);
        assert_eq!(u16::from(WebSocketCloseCode::ChannelFull), 4004);
    }

    #[test]
    fn protocol_client_auth_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
        let envelope = ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
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
    fn protocol_start_recording_request_decodes_with_request_id() {
        let decoded = ClientEnvelope::decode(Envelope {
            tag: String::from("startrecording"),
            payload: Some(json!({
                "audio": true,
                "video": false,
            })),
            request_id: Some(RequestId::new("3")),
            response_to: None,
        });

        assert_eq!(
            decoded,
            Ok(ClientEnvelope::Request {
                request_id: RequestId::new("3"),
                request: ClientRequest::StartRecording(RecordingOptions {
                    audio: Some(true),
                    video: Some(false),
                    transcription: None,
                }),
            })
        );
    }

    #[test]
    fn protocol_offer_response_decodes_with_response_id() {
        let decoded = ClientEnvelope::decode(Envelope {
            tag: String::from("offer"),
            payload: Some(json!({
                "sdp": "v=0\r\n",
            })),
            request_id: None,
            response_to: Some(RequestId::new("1")),
        });

        assert_eq!(
            decoded,
            Ok(ClientEnvelope::Response {
                response_to: RequestId::new("1"),
                response: ClientResponse::Offer(SessionDescriptionPayload {
                    sdp: String::from("v=0\r\n"),
                }),
            })
        );
    }

    #[test]
    fn protocol_subscribe_message_decodes_flat_download_state_shape() {
        let decoded = ClientEnvelope::decode(Envelope {
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
            Ok(ClientEnvelope::Message(ClientMessage::Subscribe(
                SubscribePayload {
                    session_id: SessionId::Integer(7),
                    states: DownloadStates {
                        audio: Some(true),
                        camera: Some(false),
                        screen: None,
                    },
                }
            )))
        );
    }

    #[test]
    fn protocol_decode_rejects_envelopes_with_both_request_and_response_ids() {
        let decoded = ClientEnvelope::decode(Envelope {
            tag: String::from("ping"),
            payload: None,
            request_id: Some(RequestId::new("1")),
            response_to: Some(RequestId::new("2")),
        });

        assert_eq!(decoded, Err(EnvelopeDecodeError::InvalidRoutingMetadata));
    }

    #[test]
    fn protocol_welcome_message_round_trips_to_wire_envelope() -> serde_json::Result<()> {
        let welcome = ServerMessage::Welcome(WelcomePayload {
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
            peers: vec![PeerSnapshot {
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
    fn protocol_publish_message_uses_stream_type_field() -> serde_json::Result<()> {
        let envelope = ClientMessage::Publish(StreamIntentPayload {
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
    fn protocol_server_track_and_peer_messages_round_trip_to_wire_envelopes()
    -> serde_json::Result<()> {
        let track_update = ServerMessage::Tracks(vec![TrackBinding {
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

        let peer_joined = ServerMessage::PeerJoined(PeerInfoPayload {
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

        let peer_left = ServerMessage::PeerLeft(PeerLeftPayload {
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
    fn protocol_server_welcome_message_decodes_without_routing_metadata() {
        let decoded = ServerEnvelope::decode(Envelope {
            tag: String::from("welcome"),
            payload: Some(json!({
                "features": {
                    "rtc": true,
                    "transcription": false,
                    "audioRecording": true,
                    "videoRecording": false,
                },
                "recording": {
                    "recording": true,
                },
                "peers": [{
                    "sessionId": 7,
                    "info": {
                        "isTalking": false,
                    },
                }],
            })),
            request_id: None,
            response_to: None,
        });

        assert_eq!(
            decoded,
            Ok(ServerEnvelope::Message(ServerMessage::Welcome(
                WelcomePayload {
                    features: AvailableFeatures {
                        rtc: true,
                        transcription: false,
                        audio_recording: true,
                        video_recording: false,
                    },
                    recording: RecordingState {
                        recording: Some(true),
                        audio: None,
                        transcription: None,
                        video: None,
                    },
                    peers: vec![PeerSnapshot {
                        session_id: SessionId::Integer(7),
                        info: SessionInfo {
                            is_talking: Some(false),
                            ..SessionInfo::default()
                        },
                    }],
                }
            )))
        );
    }

    #[test]
    fn protocol_server_offer_request_round_trips_through_server_envelope() -> serde_json::Result<()>
    {
        let envelope = ServerEnvelope::Request {
            request_id: RequestId::new("offer-1"),
            request: ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0\r\n"),
            }),
        }
        .into_envelope()?;

        assert_eq!(
            ServerEnvelope::decode(envelope),
            Ok(ServerEnvelope::Request {
                request_id: RequestId::new("offer-1"),
                request: ServerRequest::Offer(SessionDescriptionPayload {
                    sdp: String::from("v=0\r\n"),
                }),
            })
        );
        Ok(())
    }

    #[test]
    fn protocol_server_stop_recording_response_round_trips_through_server_envelope()
    -> serde_json::Result<()> {
        let envelope = ServerEnvelope::Response {
            response_to: RequestId::new("recording-1"),
            response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
        }
        .into_envelope()?;

        assert_eq!(
            ServerEnvelope::decode(envelope),
            Ok(ServerEnvelope::Response {
                response_to: RequestId::new("recording-1"),
                response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
            })
        );
        Ok(())
    }

    #[test]
    fn protocol_server_broadcast_and_recording_messages_round_trip_to_wire_envelopes()
    -> serde_json::Result<()> {
        let broadcast = ServerMessage::Broadcast(ServerBroadcastPayload {
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

        let recording_change = ServerMessage::RecordingChange(RecordingStateUpdate {
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
    fn protocol_server_requests_and_responses_round_trip_to_wire_envelopes()
    -> serde_json::Result<()> {
        let offer = ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"),
        })
        .into_envelope(RequestId::new("1"))?;
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

        let ping = ServerRequest::Ping.into_envelope(RequestId::new("4"))?;
        assert_eq!(
            serde_json::to_value(&ping)?,
            json!({
                "t": "ping",
                "q": "4",
            })
        );

        let start_recording = ServerResponse::StartRecording(RecordingActionResult { ok: true })
            .into_envelope(RequestId::new("3"))?;
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
