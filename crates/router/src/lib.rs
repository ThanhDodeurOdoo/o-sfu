mod model;
#[cfg(any(test, feature = "test-support", kani))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;

pub mod ids {
    pub use crate::model::ids::*;
}

pub mod rtp {
    pub use crate::model::{MediaKind, rtp::*};
}

pub mod negotiation {
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::model::diagnostic::*;
    pub use crate::model::rtp_negotiation::*;
}

pub mod topology {
    pub use crate::model::topology::*;
}

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
