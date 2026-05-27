//! Native signaling protocol surface and wire codec.

mod catalog;
mod codec;
mod envelope;
mod request;
mod response;
mod tags;
#[cfg(test)]
mod tests;

pub use catalog::{
    AuthPayload, ClientBroadcastPayload, NegotiationUploadEncoding, NegotiationUploadSlot,
    PeerInfoPayload, PeerLeftPayload, RecordingActionResult, ServerBroadcastPayload,
    SessionDescriptionPayload, SourceDescriptor, SourceEncodingDescriptor, StreamIntentPayload,
    SubscribePayload, TrackBinding, UploadLayerPolicyRole, WelcomePayload,
};
pub use o_sfu_model::{PeerSnapshot, RecordingOptions, WebSocketCloseCode};

pub use self::{
    codec::{ClientEnvelope, EnvelopeDecodeError, ServerEnvelope},
    envelope::{
        Envelope, EnvelopeBatch, EnvelopeBatchDecodeError, RequestId, decode_envelope_batch,
    },
    request::{ClientMessage, ClientRequest, ServerRequest},
    response::{ClientResponse, ServerMessage, ServerResponse},
};
#[cfg(feature = "ts-bindings")]
pub(crate) use self::{
    request::{CLIENT_MESSAGE_ENVELOPES, CLIENT_REQUEST_ENVELOPES, SERVER_REQUEST_ENVELOPES},
    response::{CLIENT_RESPONSE_ENVELOPES, SERVER_MESSAGE_ENVELOPES, SERVER_RESPONSE_ENVELOPES},
    tags::WIRE_TAGS,
};

#[cfg(feature = "ts-bindings")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeKind {
    Message,
    Request,
    Response,
}

#[cfg(feature = "ts-bindings")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EnvelopeSpec {
    pub kind: EnvelopeKind,
    pub tag: &'static str,
    pub payload: Option<&'static str>,
}

#[cfg(feature = "ts-bindings")]
impl EnvelopeSpec {
    pub(crate) const fn message(tag: &'static str, payload: &'static str) -> Self {
        Self {
            kind: EnvelopeKind::Message,
            tag,
            payload: Some(payload),
        }
    }

    pub(crate) const fn request(tag: &'static str, payload: Option<&'static str>) -> Self {
        Self {
            kind: EnvelopeKind::Request,
            tag,
            payload,
        }
    }

    pub(crate) const fn response(tag: &'static str, payload: &'static str) -> Self {
        Self {
            kind: EnvelopeKind::Response,
            tag,
            payload: Some(payload),
        }
    }
}
