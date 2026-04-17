// TODO: needs documentation:
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
pub(crate) mod verification;

pub use self::consumer::Consumer;
pub use self::consumer_capability::ConsumerCapability;
pub use self::diagnostic::{
    ParseDiagnostic, ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference,
};
pub use self::error::RouterError;
pub use self::ids::{ConsumerId, ProducerId, RouterId, SessionId, TransportId};
pub use self::media::{MediaKind, StreamType};
pub use self::observer::{NoopRouterObserver, RouterEvent, RouterObserver};
pub use self::producer::Producer;
pub use self::router::Router;
pub use self::rtp::{
    CodecSetting, HeaderExtension, HeaderExtensionId, HeaderExtensionUri, MediaCapabilities,
    MediaCodec, MediaCodecCapability, MediaFormat, MediaStream, Mid, PayloadType, Rid,
    RtcpFeedback, RtcpFeedbackKind, Ssrc, StreamBinding,
};
pub use self::rtp_negotiation::{
    RtpNegotiationError, can_consume, derive_consumable_rtp_parameters,
    negotiate_consumer_rtp_parameters,
};
pub use self::session::{Session, SessionPermissionFlags, SessionPermissions, SessionState};
pub use self::transport::{Transport, TransportDirection};
