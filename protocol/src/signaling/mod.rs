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
mod tests;
