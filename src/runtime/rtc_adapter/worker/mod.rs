//! Worker-local RTC command handling for one shard
//!
//! The packet loop owns the mutable [`RtcBootstrapState`](super::state::RtcBootstrapState)
//! for a shard and calls into this module whenever a control-plane command needs
//! to mutate it. That keeps async facade code out of the state-transition layer
//! while preserving one serialized owner for session, negotiation, media, and
//! teardown state.
//!
//! What it does:
//! - dispatch worker mailbox commands into focused mutation modules
//! - keep offer/answer, media registry, and route-control changes serialized on
//!   the packet-loop task
//! - expose the small helpers that packet-loop code reuses directly, such as
//!   keyframe requests for already-resolved sources

#[cfg(test)]
mod debug;
mod dispatcher;
mod media;
mod negotiation;
mod publication;
mod session;

pub(crate) use dispatcher::WorkerCommandContext;
#[cfg(test)]
pub(crate) use dispatcher::handle_debug_worker_command;
pub(crate) use dispatcher::handle_worker_command;
pub(crate) use media::request_keyframe_for_source;
