use super::{
    ClientBroadcastPayload, Envelope, RecordingOptions, RequestId, SessionDescriptionPayload,
    StreamIntentPayload, SubscribePayload,
};
use crate::shared::SessionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Auth(super::AuthPayload),
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

    pub(crate) fn into_envelope(self) -> Result<Envelope, serde_json::Error> {
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

    pub(crate) fn into_envelope(
        self,
        request_id: RequestId,
    ) -> Result<Envelope, serde_json::Error> {
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
pub enum ServerRequest {
    Offer(SessionDescriptionPayload),
    Renegotiate(SessionDescriptionPayload),
}

impl ServerRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::Offer(_) => "offer",
            Self::Renegotiate(_) => "renegotiate",
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
            Some(match self {
                Self::Offer(payload) | Self::Renegotiate(payload) => serde_json::to_value(payload)?,
            }),
        ))
    }
}
