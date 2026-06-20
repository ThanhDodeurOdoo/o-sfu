//! Pure router-core domain types for `o-sfu`.
//!
//! This crate keep all the routing logic pure, independant from async runtime
mod model;
#[cfg(any(test, feature = "test-support", kani))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;

pub use self::model::{
    CodecSetting, ConnectionId, ConsumerCapability, ConsumerId, ConsumerRouteState, ConsumerSpec,
    HeaderExtension, HeaderExtensionId, HeaderExtensionUri, MediaCapabilities, MediaCodec,
    MediaCodecCapability, MediaFormat, MediaKind, MediaStream, MediaWorkerId, Mid,
    NoopRouterObserver, PayloadType, ProducerId, ProducerRouteState, ProducerSpec,
    ReceiveTransportHandle, Rid, RoutedConsumerId, RoutedProducerId, Router, RouterError,
    RouterEvent, RouterId, RouterObserver, RouterPlacement, RouterPlacements,
    RouterPlacementsError, RoutingCommitReceipt, RoutingError, RoutingPlacementCommit,
    RoutingPlacementSnapshot, RoutingRepairReport, RoutingTopology, RtcpFeedback, RtcpFeedbackKind,
    RtpNegotiationError, SendTransportHandle, Session, SessionHandle, SessionId, SessionState,
    Ssrc, StreamBinding, TransportDirection, TransportId, can_consume,
    derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};
#[cfg(any(test, feature = "test-support"))]
pub use self::model::{ParseDiagnostic, ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference};
