use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    ClientMessage, ClientRequest, ClientResponse, Envelope, RequestId, ServerMessage,
    ServerRequest, ServerResponse, envelope::EnvelopeRoute, tags,
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
                    tags::OFFER,
                    response_to,
                    Some(serde_json::to_value(payload)?),
                )),
                ClientResponse::Renegotiate(payload) => Ok(Envelope::response(
                    tags::RENEGOTIATE,
                    response_to,
                    Some(serde_json::to_value(payload)?),
                )),
            },
        }
    }

    /// Decode a raw websocket envelope into the typed native client contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the tag is unknown or the payload does not match
    /// the declared message shape.
    pub fn decode(envelope: Envelope) -> Result<Self, EnvelopeDecodeError> {
        let (tag, payload, route) = envelope.into_parts();
        match route {
            EnvelopeRoute::Message => decode_client_message(&tag, payload),
            EnvelopeRoute::Request(request_id) => decode_client_request(request_id, &tag, payload),
            EnvelopeRoute::Response(response_to) => {
                decode_client_response(response_to, &tag, payload)
            }
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
    /// Returns an error when the tag is unknown or the payload does not match
    /// the declared message shape.
    pub fn decode(envelope: Envelope) -> Result<Self, EnvelopeDecodeError> {
        let (tag, payload, route) = envelope.into_parts();
        match route {
            EnvelopeRoute::Message => decode_server_message(&tag, payload),
            EnvelopeRoute::Request(request_id) => decode_server_request(request_id, &tag, payload),
            EnvelopeRoute::Response(response_to) => {
                decode_server_response(response_to, &tag, payload)
            }
        }
    }
}

fn decode_client_message(
    tag: &str,
    payload: Option<Value>,
) -> Result<ClientEnvelope, EnvelopeDecodeError> {
    match tag {
        tags::AUTH => Ok(ClientEnvelope::Message(ClientMessage::Auth(parse_payload(
            tag, payload,
        )?))),
        tags::PUBLISH => Ok(ClientEnvelope::Message(ClientMessage::Publish(
            parse_payload(tag, payload)?,
        ))),
        tags::UNPUBLISH => Ok(ClientEnvelope::Message(ClientMessage::Unpublish(
            parse_payload(tag, payload)?,
        ))),
        tags::SUBSCRIBE => Ok(ClientEnvelope::Message(ClientMessage::Subscribe(
            parse_payload(tag, payload)?,
        ))),
        tags::INFO => Ok(ClientEnvelope::Message(ClientMessage::Info(parse_payload(
            tag, payload,
        )?))),
        tags::BROADCAST => Ok(ClientEnvelope::Message(ClientMessage::Broadcast(
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
        tags::START_RECORDING => Ok(ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StartRecording(parse_payload(tag, payload)?),
        }),
        tags::STOP_RECORDING => {
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
        tags::OFFER => ClientResponse::Offer(parse_payload(tag, payload)?),
        tags::RENEGOTIATE => ClientResponse::Renegotiate(parse_payload(tag, payload)?),
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
        tags::WELCOME => Ok(ServerEnvelope::Message(ServerMessage::Welcome(
            parse_payload(tag, payload)?,
        ))),
        tags::TRACKS => Ok(ServerEnvelope::Message(ServerMessage::Tracks(
            parse_payload(tag, payload)?,
        ))),
        tags::PEER_INFO => Ok(ServerEnvelope::Message(ServerMessage::PeerInfo(
            parse_payload(tag, payload)?,
        ))),
        tags::PEER_JOINED => Ok(ServerEnvelope::Message(ServerMessage::PeerJoined(
            parse_payload(tag, payload)?,
        ))),
        tags::PEER_LEFT => Ok(ServerEnvelope::Message(ServerMessage::PeerLeft(
            parse_payload(tag, payload)?,
        ))),
        tags::BROADCAST => Ok(ServerEnvelope::Message(ServerMessage::Broadcast(
            parse_payload(tag, payload)?,
        ))),
        tags::RECORDING_CHANGE => Ok(ServerEnvelope::Message(ServerMessage::RecordingChange(
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
        tags::OFFER => ServerRequest::Offer(parse_payload(tag, payload)?),
        tags::RENEGOTIATE => ServerRequest::Renegotiate(parse_payload(tag, payload)?),
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
        tags::START_RECORDING => ServerResponse::StartRecording(parse_payload(tag, payload)?),
        tags::STOP_RECORDING => ServerResponse::StopRecording(parse_payload(tag, payload)?),
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
