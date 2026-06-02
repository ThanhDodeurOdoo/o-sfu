use super::{
    ClientMessage, ClientRequest, ClientResponse, Envelope, EnvelopeDecodeError, RequestId,
    ServerMessage, ServerRequest, ServerResponse, envelope::EnvelopeRoute,
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

impl ClientEnvelope {
    /// encode one typed client-side envelope into the websocket wire shape
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

    /// decode a raw websocket envelope into the typed native client contract
    ///
    /// # Errors
    ///
    /// returns an error when the tag is unknown or the payload does not match
    /// the declared message shape
    pub fn decode(envelope: Envelope) -> Result<Self, EnvelopeDecodeError> {
        let (tag, payload, route) = envelope.into_parts();
        match route {
            EnvelopeRoute::Message => {
                ClientMessage::decode(&tag, payload).map(ClientEnvelope::Message)
            }
            EnvelopeRoute::Request(request_id) => {
                ClientRequest::decode(&tag, payload).map(|request| Self::Request {
                    request_id,
                    request,
                })
            }
            EnvelopeRoute::Response(response_to) => {
                ClientResponse::decode(&tag, payload).map(|response| Self::Response {
                    response_to,
                    response,
                })
            }
        }
    }
}

impl ServerEnvelope {
    /// encode one typed server-side envelope into the websocket wire shape
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

    /// decode a raw websocket envelope into the typed native server contract
    ///
    /// # Errors
    ///
    /// returns an error when the tag is unknown or the payload does not match
    /// the declared message shape
    pub fn decode(envelope: Envelope) -> Result<Self, EnvelopeDecodeError> {
        let (tag, payload, route) = envelope.into_parts();
        match route {
            EnvelopeRoute::Message => {
                ServerMessage::decode(&tag, payload).map(ServerEnvelope::Message)
            }
            EnvelopeRoute::Request(request_id) => {
                ServerRequest::decode(&tag, payload).map(|request| Self::Request {
                    request_id,
                    request,
                })
            }
            EnvelopeRoute::Response(response_to) => {
                ServerResponse::decode(&tag, payload).map(|response| Self::Response {
                    response_to,
                    response,
                })
            }
        }
    }
}
