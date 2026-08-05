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
//! they disappear with their final consumer. Replacing a receiver connection
//! removes its shadows without removing another user's producers
//!
//! # Examples
//!
//! ```
//! # use o_sfu_model::UserId;
//! # use o_sfu_router::{
//! #     ConnectionId, ConsumerId, MediaWorkerId, ProducerId, Router, RouterError, RouterId,
//! #     rtp::MediaCapabilities,
//! #     topology::{RouterPlacement, RouterPlacements},
//! # };
//!
//! # fn main() -> Result<(), RouterError> {
//! # fn placement(router: u64, worker: usize) -> RouterPlacement {
//! #     RouterPlacement {
//! #         router: RouterId(router),
//! #         media_worker: MediaWorkerId::from_raw(worker),
//! #     }
//! # }
//! let source = placement(1, 0);
//! let first_receiver = placement(2, 1);
//! let next_receiver = placement(3, 2);
//! let placements = RouterPlacements::new(source, vec![first_receiver, next_receiver]);
//! let mut router = Router::with_placements(placements, MediaCapabilities::default());
//! let publisher = UserId::from(1_i64);
//! let receiver = UserId::from(2_i64);
//! let publisher_connection = ConnectionId::from_raw(10);
//! let first_receiver_connection = ConnectionId::from_raw(20);
//! router.commit_session_placement(&publisher, publisher_connection, source)?;
//! router.commit_session_placement(&receiver, first_receiver_connection, first_receiver)?;
//! let producer = router.add_producer(&publisher, ProducerId(30))?;
//! let stale = router.add_consumer(&receiver, ConsumerId(40), producer)?;
//!
//! assert_eq!(stale.router_id(), producer.router_id());
//! assert_eq!(stale.connection_id(), first_receiver_connection);
//!
//! let next_receiver_connection = ConnectionId::from_raw(21);
//! router.commit_session_placement(&receiver, next_receiver_connection, next_receiver)?;
//! assert_eq!(
//!     router.remove_consumer(stale),
//!     Err(RouterError::MissingConsumer(stale)),
//! );
//!
//! let current = router.add_consumer(&receiver, ConsumerId(41), producer)?;
//! assert_eq!(current.router_id(), producer.router_id());
//! assert_eq!(current.connection_id(), next_receiver_connection);
//! # Ok(())
//! # }
//! ```

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
///
/// Producer parameters are first normalized against router capabilities. The
/// consumer is then negotiated from that router-visible stream
///
/// # Examples
///
/// ```
/// # use o_sfu_router::{
/// #     MediaKind,
/// #     negotiation::{
/// #         RtpNegotiationError, derive_consumable_rtp_parameters,
/// #         negotiate_consumer_rtp_parameters,
/// #     },
/// #     rtp::{
/// #         MediaCapabilities, MediaCodecCapability, MediaFormat, MediaStream, PayloadType,
/// #     },
/// # };
/// # fn main() -> Result<(), RtpNegotiationError> {
/// let producer = MediaStream::new(
///     vec![
///         MediaFormat::new(MediaKind::Video, "rtx", PayloadType::new(97), 90_000)
///             .with_parameter("apt", "96"),
///         MediaFormat::new(MediaKind::Video, "VP8", PayloadType::new(96), 90_000),
///     ],
///     vec![],
///     vec![],
/// );
/// let router = MediaCapabilities::new(
///     vec![
///         MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
///             .with_payload_type(PayloadType::new(100)),
///         MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
///             .with_payload_type(PayloadType::new(101))
///             .with_parameter("apt", "100"),
///     ],
///     vec![],
/// );
/// let consumer = MediaCapabilities::new(
///     vec![
///         MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000),
///         MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
///             .with_parameter("apt", "100"),
///     ],
///     vec![],
/// );
///
/// let consumable = derive_consumable_rtp_parameters(&producer, &router)?;
/// let negotiated = negotiate_consumer_rtp_parameters(&consumable, &consumer)?;
///
/// assert_eq!(
///     consumable
///         .formats()
///         .map(|format| (
///             format.codec_name(),
///             format.payload_type(),
///             format.rtx_associated_payload_type(),
///         ))
///         .collect::<Vec<_>>(),
///     vec![("VP8", 100, None), ("rtx", 101, Some(100))],
/// );
/// assert_eq!(negotiated, consumable);
/// # Ok(())
/// # }
/// ```
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
