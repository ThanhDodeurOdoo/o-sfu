//! Pure router-core domain types for `o-sfu`.
//!
//! This crate keep all the routing logic pure, independant from async runtime
mod model;

pub use self::model::{
    CodecSetting, Consumer, ConsumerCapability, ConsumerId, HeaderExtension, HeaderExtensionId,
    HeaderExtensionUri, MediaCapabilities, MediaCodec, MediaCodecCapability, MediaFormat,
    MediaKind, MediaStream, Mid, NoopRouterObserver, ParseDiagnostic, ParseDiagnosticKind,
    ParseDiagnosticSpec, PayloadType, Producer, ProducerId, RfcReference, Rid, Router, RouterError,
    RouterEvent, RouterId, RouterObserver, RtcpFeedback, RtcpFeedbackKind, RtpNegotiationError,
    Session, SessionId, SessionPermissionFlags, SessionPermissions, SessionState, Ssrc,
    StreamBinding, StreamType, Transport, TransportDirection, TransportId, can_consume,
    derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};
