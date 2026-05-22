//! The modules in this tree define the pure routing state machine, the typed
//! RTP/domain models used at its boundary, and narrow test-support helpers.
mod consumer;
mod consumer_capability;
#[cfg(any(test, feature = "test-support"))]
mod diagnostic;
mod error;
mod ids;
mod observer;
mod producer;
mod proof_storage;
mod relation_index;
mod route_state;
mod router;
mod rtp;
mod rtp_negotiation;
mod session;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
#[cfg(test)]
mod tests;
mod transport;

pub use o_sfu_rfc::webrtc::MediaKind;

#[cfg(any(test, feature = "test-support"))]
pub use self::diagnostic::{
    ParseDiagnostic, ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference,
};
pub use self::{
    consumer::Consumer,
    consumer_capability::ConsumerCapability,
    error::RouterError,
    ids::{ConsumerId, ProducerId, RouterId, SessionId, TransportId},
    observer::{NoopRouterObserver, RouterEvent, RouterObserver},
    producer::Producer,
    route_state::{ConsumerRouteState, ProducerRouteState},
    router::Router,
    rtp::{
        CodecSetting, HeaderExtension, HeaderExtensionId, HeaderExtensionUri, MediaCapabilities,
        MediaCodec, MediaCodecCapability, MediaFormat, MediaStream, Mid, PayloadType, Rid,
        RtcpFeedback, RtcpFeedbackKind, Ssrc, StreamBinding,
    },
    rtp_negotiation::{
        RtpNegotiationError, can_consume, derive_consumable_rtp_parameters,
        negotiate_consumer_rtp_parameters,
    },
    session::{Session, SessionState},
    transport::{Transport, TransportDirection},
};
