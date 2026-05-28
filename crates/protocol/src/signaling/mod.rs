//! Native signaling protocol surface and wire codec.

mod catalog;
mod codec;
mod envelope;
#[cfg(test)]
mod tests;
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
