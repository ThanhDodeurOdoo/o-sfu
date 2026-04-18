use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    ClientMessage, ClientRequest, ClientResponse, Envelope, RequestId, ServerMessage,
    ServerRequest, ServerResponse,
};

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
