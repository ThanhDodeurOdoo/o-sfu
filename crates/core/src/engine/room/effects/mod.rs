//! Shared post-lock commit executor for room transitions.

mod batch;
pub(super) use batch::{RoomCommit, RoomEffectContext};
