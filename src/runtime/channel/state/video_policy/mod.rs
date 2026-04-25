//! Pure room-owned video source policy.
//!
//! The source-selection path is deliberately split into a one-way pipeline:
//! immutable input from `ChannelState` and transport observations, layout
//! intent, pure budget planning, semantic route actions, selector-to-packet-gate
//! projection, and stale-update commits after async effects finish.
//!
//! The packet loop consumes only transport packet gates. It never sees receiver
//! bandwidth, active-speaker layout, source descriptors, or route-pause reasons.

mod action;
mod budget;
mod commit;
mod input;
mod layout;
mod projection;

pub(in crate::runtime::channel) use action::{
    ConsumerPacketSelectionUpdate, FeaturedSessionUpdate,
};
