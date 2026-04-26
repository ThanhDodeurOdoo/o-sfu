//! The modules in this tree define the pure routing state machine, the typed
//! RTP/domain models used at its boundary, and some helpers (for test/proofs)
mod consumer;
mod consumer_capability;
mod diagnostic;
mod error;
mod ids;
mod media;
mod observer;
mod producer;
mod router;
mod rtp;
mod rtp_negotiation;
mod session;
#[cfg(test)]
mod tests;
mod transport;

pub use self::{
    consumer::Consumer,
    consumer_capability::ConsumerCapability,
    diagnostic::{ParseDiagnostic, ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference},
    error::RouterError,
    ids::{ConsumerId, ProducerId, RouterId, SessionId, TransportId},
    media::MediaKind,
    observer::{NoopRouterObserver, RouterEvent, RouterObserver},
    producer::Producer,
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
    session::{Session, SessionPermissionFlags, SessionPermissions, SessionState},
    transport::{Transport, TransportDirection},
};
