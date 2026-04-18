//! Pure router-core domain types for `o-sfu`.
//!
//! This crate keep session, transport, producer, consumer, and RTP negotiation
//! state independant from async runtime, sockets, or WebRTC bindings. The
//! router is the proof-friendly core that channel and trnasport layers build on.
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
pub use self::model::{
    HeaderExtension as RtpHeaderExtension, MediaCapabilities as RtpCapabilities,
    MediaCodecCapability as RtpCodecCapability, MediaFormat as RtpCodecParameters,
    MediaStream as RtpParameters, StreamBinding as RtpEncoding,
};
pub use o_sfu_rfc as rfc;
