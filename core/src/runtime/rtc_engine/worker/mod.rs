//! Worker-local RTC command handling for one shard
//!
//! The packet loop owns the mutable [`RtcBootstrapState`](super::state::RtcBootstrapState)
//! for a shard and calls into this module whenever a control-plane command needs
//! to mutate it. That keeps async facade code out of the state-transition layer
//! while preserving one serialized owner for user, negotiation, media, and
//! teardown state.
//!
//! What it does:
//! - dispatch worker mailbox commands into focused mutation modules
//! - keep offer/answer, media registry, and route-control changes serialized on
//!   the packet-loop task
//! - expose the small helpers that packet-loop code reuses directly, such as
//!   keyframe requests for already-resolved sources

mod dispatcher;
mod media;
mod negotiation;
mod publication;
mod session;

pub use dispatcher::{WorkerCommandContext, handle_worker_command};
pub(super) use media::{
    drain_due_rid_keyframe_refreshes, observe_source_rid_readiness, request_keyframe_for_source,
};
