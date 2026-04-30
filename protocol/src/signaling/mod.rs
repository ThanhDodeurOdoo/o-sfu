//! Native signaling protocol surface and wire codec.

mod catalog;
mod codec;
mod envelope;
mod request;
mod response;
#[cfg(test)]
mod tests;

pub use catalog::{
    AuthPayload, ClientBroadcastPayload, NegotiationUploadEncoding, NegotiationUploadSlot,
    PeerInfoPayload, PeerLeftPayload, RecordingActionResult, ServerBroadcastPayload,
    SessionDescriptionPayload, SourceDescriptor, SourceEncodingDescriptor, StreamIntentPayload,
    SubscribePayload, TrackBinding, UploadLayerPolicyRole, WelcomePayload,
};
pub use codec::{ClientEnvelope, EnvelopeDecodeError, ServerEnvelope};
pub use envelope::{Envelope, EnvelopeBatch, RequestId};
pub use o_sfu_model::{PeerSnapshot, RecordingOptions, WebSocketCloseCode};
pub use request::{ClientMessage, ClientRequest, ServerRequest};
pub use response::{ClientResponse, ServerMessage, ServerResponse};
