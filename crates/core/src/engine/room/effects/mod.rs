//! Shared post-lock effect executor for room transitions.
//!
//! Transition modules own their choreography. This module keeps only the common
//! batching primitive used to apply transport cleanup, relay changes,
//! diagnostics and outbound sends after room-state locks have been released.

mod batch;
pub(super) use batch::{MediaCountDelta, RoomEffectBatch, RoomEffectContext, TransportUserCleanup};
