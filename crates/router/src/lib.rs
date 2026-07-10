//! pure room router for placement and routed media lifetimes
//!
//! [`Router`] is the sole stateful facade. it owns the exact user-to-connection
//! placement relation and a private graph for each attached router
//!
//! ```text
//! Router
//!   +- committed user -> connection -> home router
//!   +- local router
//!      +- home session -> producer -> dependent consumers
//!      +- foreign session -> consumers
//! ```
//!
//! foreign sessions are the receiver shadows required by cross-router routes
//! they disappear with their final consumer. RTP negotiation remains a separate
//! pure facade because it does not mutate routing state

mod model;
#[cfg(kani)]
#[path = "../../../tests/proofs/src/router/proofs.rs"]
mod proofs;
#[cfg(any(test, feature = "test-support"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;

/// typed router and media identifiers
pub mod ids {
    pub use crate::model::ids::*;
}

/// typed RTP values used at the router boundary
pub mod rtp {
    pub use crate::model::{MediaKind, rtp::*};
}

/// producer and consumer RTP negotiation
pub mod negotiation {
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::model::diagnostic::*;
    pub use crate::model::rtp_negotiation::*;
}

/// placement and routed media identifiers used by [`Router`]
pub mod topology {
    pub use crate::model::topology::{
        PlacementSnapshot, RoutedConsumerId, RoutedProducerId, RouterPlacement, RouterPlacements,
        RouterPlacementsError,
    };
}

pub use ids::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId};
pub use model::{MediaKind, Router, RouterError};
