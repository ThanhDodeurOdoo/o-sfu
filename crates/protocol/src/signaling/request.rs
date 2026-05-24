#[cfg(feature = "ts-bindings")]
use super::EnvelopeSpec;
use super::{
    ClientBroadcastPayload, Envelope, RecordingOptions, RequestId, SessionDescriptionPayload,
    StreamIntentPayload, SubscribePayload, tags,
};
use crate::shared::UserInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Auth(super::AuthPayload),
    Publish(StreamIntentPayload),
    Unpublish(StreamIntentPayload),
    Subscribe(SubscribePayload),
    Info(UserInfo),
    Broadcast(ClientBroadcastPayload),
}

#[cfg(feature = "ts-bindings")]
pub(crate) const CLIENT_MESSAGE_ENVELOPES: &[EnvelopeSpec] = &[
    EnvelopeSpec::message(tags::AUTH, "AuthPayload"),
    EnvelopeSpec::message(tags::PUBLISH, "StreamIntentPayload"),
    EnvelopeSpec::message(tags::UNPUBLISH, "StreamIntentPayload"),
    EnvelopeSpec::message(tags::SUBSCRIBE, "SubscribePayload"),
    EnvelopeSpec::message(tags::INFO, "UserInfo"),
    EnvelopeSpec::message(tags::BROADCAST, "ClientBroadcastPayload"),
];

impl ClientMessage {
    fn tag(&self) -> &'static str {
        match self {
            Self::Auth(_) => tags::AUTH,
            Self::Publish(_) => tags::PUBLISH,
            Self::Unpublish(_) => tags::UNPUBLISH,
            Self::Subscribe(_) => tags::SUBSCRIBE,
            Self::Info(_) => tags::INFO,
            Self::Broadcast(_) => tags::BROADCAST,
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

#[cfg(feature = "ts-bindings")]
pub(crate) const CLIENT_REQUEST_ENVELOPES: &[EnvelopeSpec] = &[
    EnvelopeSpec::request(tags::START_RECORDING, Some("RecordingOptions")),
    EnvelopeSpec::request(tags::STOP_RECORDING, None),
];

impl ClientRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::StartRecording(_) => tags::START_RECORDING,
            Self::StopRecording => tags::STOP_RECORDING,
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

#[cfg(feature = "ts-bindings")]
pub(crate) const SERVER_REQUEST_ENVELOPES: &[EnvelopeSpec] = &[
    EnvelopeSpec::request(tags::OFFER, Some("SessionDescriptionPayload")),
    EnvelopeSpec::request(tags::RENEGOTIATE, Some("SessionDescriptionPayload")),
];

impl ServerRequest {
    fn tag(&self) -> &'static str {
        match self {
            Self::Offer(_) => tags::OFFER,
            Self::Renegotiate(_) => tags::RENEGOTIATE,
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
