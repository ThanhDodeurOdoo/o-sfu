//! Native signaling protocol surface and wire codec.

mod catalog;
mod close_code;
mod codec;
mod envelope;
mod request;
mod response;
#[cfg(test)]
mod tests;

pub use catalog::{
    AuthPayload, ClientBroadcastPayload, PeerInfoPayload, PeerLeftPayload, PeerSnapshot,
    RecordingActionResult, RecordingOptions, ServerBroadcastPayload, SessionDescriptionPayload,
    SourceDescriptor, SourceEncodingDescriptor, StreamIntentPayload, SubscribePayload,
    TrackBinding, WelcomePayload,
};
pub use close_code::WebSocketCloseCode;
pub use codec::{ClientEnvelope, EnvelopeDecodeError, ServerEnvelope};
pub use envelope::{Envelope, EnvelopeBatch, RequestId};
pub use request::{ClientMessage, ClientRequest, ServerRequest};
pub use response::{ClientResponse, ServerMessage, ServerResponse};
