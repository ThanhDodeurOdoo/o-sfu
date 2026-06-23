//! pure SFU router for sessions, transports, producers and consumers
//!
//! `o-sfu-router` is the stateful routing model below `o-sfu-core`
//! it contain the state of topology for sessions, transports, producers and
//! consumers
//! accepted mutations update that topology before the runtime creates transport
//! side effects
//!
//! ```text
//! signaling or SDP edge
//!     -> rtp::MediaStream and rtp::MediaCapabilities
//!     -> negotiation facade
//!     -> Router or topology::RoutingTopology
//!     -> core transport effects
//! ```
//!
//! the crate is synchronous and sans-io
//! it stores typed ids, RTP media shapes, route state and reverse indexes
//! it does not open sockets, parse SDP, run worker tasks or send protocol
//! messages
//!
//! ## single-router state
//!
//! [`Router`] owns the pure state for one router instance:
//!
//! ```text
//! Session
//!   +- receive Transport -> Producer
//!   +- send Transport    -> Consumer -> Producer
//! ```
//!
//! callers mutate a [`Router`] through short-lived handles:
//!
//! ```text
//! Router -> SessionHandle -> ReceiveTransportHandle -> Producer
//! Router -> SessionHandle -> SendTransportHandle    -> Consumer
//! ```
//!
//! the handle chain encodes transport direction before producer or consumer
//! attachment
//! reverse indexes stay inside the router so teardown and producer route-state
//! propagation can be updated in the same mutation that changes the primary
//! entity
//!
//! ## topology role
//!
//! [`topology::RoutingTopology`] composes one or more [`Router`] instances into
//! the room-level placement model used by the core
//! it commits a user connection to a router placement, creates the upload and
//! download transport pair for that user and routes consumers on the producer's
//! source router
//!
//! ```text
//! committed placement -> home Router
//! routed producer     -> producer's home Router
//! routed consumer     -> producer's source Router
//! cross-router route  -> receiver shadow on the source Router
//! ```
//!
//! a rejected placement leaves the topology unchanged
//! receiver shadows are derived cleanup state and exist only while routed
//! consumer edges still need that receiver on a producer source router
//!
//! ## negotiation role
//!
//! sdp and WebRTC offer-answer stay outside this crate
//! [`rtp`] defines the typed RTP values that cross the boundary
//! [`negotiation::derive_consumable_rtp_parameters`] maps producer parameters
//! onto router capabilities
//! [`negotiation::negotiate_consumer_rtp_parameters`] maps the consumable stream
//! onto receiver capabilities
//! after that the mutation flow passes the final [`state::ConsumerCapability`]
//! into [`state::ConsumerSpec`] so the state machine only decides whether the
//! already-negotiated route may be attached
//!
//! ## example
//!
//! ```
//! use o_sfu_router::{
//!     Router,
//!     ids::{ConsumerId, ProducerId, RouterId, SessionId, TransportId},
//!     rtp::MediaKind,
//!     state::{ConsumerCapability, ConsumerSpec, ProducerSpec, Session},
//! };
//!
//! # fn main() -> Result<(), o_sfu_router::RouterError> {
//! let mut router = Router::new(RouterId(1));
//! let publisher = SessionId(10);
//! let subscriber = SessionId(20);
//! let receive_transport = TransportId(30);
//! let send_transport = TransportId(40);
//! let producer = ProducerId(50);
//! let consumer = ConsumerId(60);
//!
//! router.join(Session::new(publisher))?;
//! router.join(Session::new(subscriber))?;
//! router
//!     .session(publisher)?
//!     .open_receive_transport(receive_transport)?
//!     .publish(ProducerSpec::new(producer, MediaKind::Audio))?;
//! router
//!     .session(subscriber)?
//!     .open_send_transport(send_transport)?
//!     .consume(ConsumerSpec::new(
//!         consumer,
//!         producer,
//!         ConsumerCapability::Compatible,
//!     ))?;
//! # Ok(())
//! # }
//! ```

mod model;
#[cfg(any(test, feature = "test-support", kani))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;

/// typed router, session, transport, producer and consumer identifiers
pub mod ids {
    pub use crate::model::ids::*;
}

/// typed RTP values used at the router boundary
///
/// the router receives already structured [`crate::rtp::MediaStream`] and
/// [`crate::rtp::MediaCapabilities`] values
/// sdp parsing, m-line validation and offer-answer sequencing stay at the
/// signaling edge
pub mod rtp {
    pub use crate::model::{MediaKind, rtp::*};
}

/// producer, router and consumer RTP negotiation facade
///
/// callers use this module when they need pure RTP capability matching without
/// constructing a full router mutation flow
/// the output is the typed media shape that later drives router state and
/// transport setup
pub mod negotiation {
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::model::diagnostic::*;
    pub use crate::model::rtp_negotiation::*;
}

/// multi-router placement and receiver-shadow topology helpers
///
/// [`crate::topology::RoutingTopology`] tracks committed user placement, routed
/// producers, routed consumers and the receiver shadows required for
/// cross-router routes
/// it is the pure topology authority used before core installs runtime effects
pub mod topology {
    pub use crate::model::topology::*;
}

/// single-router state records, mutation handles and error types
///
/// this module is the main mutation surface once callers have chosen ids,
/// media kinds and negotiated capability results
/// use `Router::join`, `Router::session` and the typed transport handles to
/// preserve router invariants in one transition
pub mod state {
    pub use crate::model::{
        consumer_capability::ConsumerCapability,
        error::RouterError,
        mutation::{
            ConsumerSpec, ProducerSpec, ReceiveTransportHandle, SendTransportHandle, SessionHandle,
        },
        observer::{NoopRouterObserver, RouterEvent, RouterObserver},
        route_state::{ConsumerRouteState, ProducerRouteState},
        session::{Session, SessionState},
        transport::TransportDirection,
    };
}

pub use ids::{ConnectionId, MediaWorkerId, RouterId};
pub use model::Router;
pub use rtp::MediaKind;
pub use state::RouterError;
