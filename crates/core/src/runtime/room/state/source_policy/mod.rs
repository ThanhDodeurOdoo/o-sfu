//! Pure room-owned source policy.
//!
//! The source-selection path is split into a one-way pipeline:
//! immutable input from `RoomState` and transport observations, layout
//! intent, pure budget planning, semantic route actions, selector-to-packet-gate
//! projection, and stale-update commits after async effects finish.
//!
//! The packet loop consumes only transport packet gates. It never sees receiver
//! bandwidth, active-speaker layout, source descriptors, or route-pause reasons.

mod action;
mod audio;
mod commit;
mod video;

pub(in crate::runtime::room) use action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate};
