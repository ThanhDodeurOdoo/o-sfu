//! The modules in this tree define the pure routing state machine, the typed
//! RTP/domain models used at its boundary, and narrow test-support helpers.
#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod consumer;
mod consumer_capability;
#[cfg(any(test, feature = "test-support"))]
mod diagnostic;
mod error;
mod ids;
mod mutation;
mod observer;
mod producer;
mod proof_storage;
mod relation_index;
mod route_state;
mod router;
mod rtp;
mod rtp_negotiation;
mod session;
#[cfg(any(test, feature = "test-support", kani))]
#[path = "TESTS/test_support.rs"]
pub mod test_support;
mod topology;
mod transport;

pub use o_sfu_rfc::webrtc::MediaKind;

#[cfg(any(test, feature = "test-support"))]
pub use self::diagnostic::{
    ParseDiagnostic, ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference,
};
use self::{consumer::Consumer, producer::Producer, transport::Transport};
pub use self::{
    consumer_capability::ConsumerCapability,
    error::RouterError,
    ids::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId, SessionId, TransportId},
    mutation::{
        ConsumerSpec, ProducerSpec, ReceiveTransportHandle, SendTransportHandle, SessionHandle,
    },
    observer::{NoopRouterObserver, RouterEvent, RouterObserver},
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
    topology::{
        RoutedConsumerId, RoutedProducerId, RouterPlacement, RouterPlacements,
        RouterPlacementsError, RoutingCommitReceipt, RoutingError, RoutingPlacementSnapshot,
        RoutingRepairReport, RoutingTopology,
    },
    transport::TransportDirection,
};
