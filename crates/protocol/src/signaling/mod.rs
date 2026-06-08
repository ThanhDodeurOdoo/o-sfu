//! Native signaling protocol surface and wire codec.

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod catalog;
mod codec;
mod envelope;
mod wire_catalog;

pub use catalog::{
    AuthPayload, ClientBroadcastPayload, NegotiationUploadEncoding, NegotiationUploadSlot,
    PeerInfoPayload, PeerLeftPayload, RecordingActionResult, ServerBroadcastPayload,
    SessionDescriptionPayload, SourceDescriptor, SourceEncodingDescriptor, StreamIntentPayload,
    SubscribePayload, TrackBinding, UploadLayerPolicyRole, WelcomePayload,
};
pub use o_sfu_model::{PeerSnapshot, RecordingOptions, WebSocketCloseCode};

pub(crate) use self::wire_catalog::{EnvelopeKind, EnvelopeSpec};
pub use self::{
    codec::{ClientEnvelope, ServerEnvelope},
    envelope::{
        Envelope, EnvelopeBatch, EnvelopeBatchDecodeError, RequestId, decode_envelope_batch,
    },
    wire_catalog::{
        ClientMessage, ClientRequest, ClientResponse, EnvelopeDecodeError, ServerMessage,
        ServerRequest, ServerResponse,
    },
};
