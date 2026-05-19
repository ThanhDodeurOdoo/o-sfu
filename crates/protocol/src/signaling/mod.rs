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
    envelope::{Envelope, EnvelopeBatch, RequestId},
    request::{ClientMessage, ClientRequest, ServerRequest},
    response::{ClientResponse, ServerMessage, ServerResponse},
};
