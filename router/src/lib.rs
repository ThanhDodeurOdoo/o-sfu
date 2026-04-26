//! Pure router-core domain types for `o-sfu`.
//!
//! This crate keep all the routing logic pure, independant from async runtime
mod model;
#[cfg(any(test, feature = "test-support"))]
pub mod test_sample;

pub use self::model::{
    CodecSetting, Consumer, ConsumerCapability, ConsumerId, HeaderExtension, HeaderExtensionId,
    HeaderExtensionUri, MediaCapabilities, MediaCodec, MediaCodecCapability, MediaFormat,
    MediaKind, MediaStream, Mid, NoopRouterObserver, ParseDiagnostic, ParseDiagnosticKind,
    ParseDiagnosticSpec, PayloadType, Producer, ProducerId, RfcReference, Rid, Router, RouterError,
    RouterEvent, RouterId, RouterObserver, RtcpFeedback, RtcpFeedbackKind, RtpNegotiationError,
    Session, SessionId, SessionState, Ssrc, StreamBinding, Transport, TransportDirection,
    TransportId, can_consume, derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};
