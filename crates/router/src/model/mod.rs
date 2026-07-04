//! The modules in this tree define the pure routing state machine, the typed
//! RTP/domain models used at its boundary, and narrow test-support helpers.
#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod consumer;
pub(crate) mod consumer_capability;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod diagnostic;
pub(crate) mod error;
pub(crate) mod ids;
pub(crate) mod mutation;
mod producer;
mod proof_storage;
mod relation_index;
pub(crate) mod route_state;
mod router;
pub(crate) mod rtp;
pub(crate) mod rtp_negotiation;
pub(crate) mod session;
#[cfg(any(test, feature = "test-support", kani))]
#[path = "TESTS/test_support.rs"]
pub mod test_support;
pub(crate) mod topology;
pub(crate) mod transport;

pub use o_sfu_rfc::webrtc::MediaKind;

use self::{consumer::Consumer, producer::Producer, transport::Transport};
pub use self::{
    consumer_capability::ConsumerCapability,
    error::RouterError,
    ids::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId, SessionId, TransportId},
    mutation::{
        ConsumerSpec, ProducerSpec, ReceiveTransportHandle, SendTransportHandle, SessionHandle,
    },
    route_state::{ConsumerRouteState, ProducerRouteState},
    router::Router,
    rtp::{
        CodecSetting, HeaderExtension, HeaderExtensionUri, MediaCapabilities, MediaCodec,
        MediaCodecCapability, MediaFormat, MediaStream, PayloadType, RtcpFeedback,
        RtcpFeedbackKind,
    },
    session::Session,
    transport::TransportDirection,
};
