//! Shared post-lock commit executor for room transitions.

mod batch;
mod consumer_setup;

pub(super) use batch::{RoomCommit, RoomEffectContext};
