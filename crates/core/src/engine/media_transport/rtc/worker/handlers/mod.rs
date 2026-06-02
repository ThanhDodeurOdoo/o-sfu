//! Worker-local RTC command handling for one worker
//!
//! The packet loop owns the mutable [`PacketLoopState`](super::super::state::PacketLoopState)
//! for a worker and calls into this module whenever a control-plane command needs
//! to mutate it. That keeps async mailbox code out of the state-transition layer
//! while preserving one serialized owner for user, negotiation, media, and
//! teardown state.
//!
//! What it does:
//! - dispatch worker mailbox commands into focused mutation modules
//! - keep offer/answer, media registry, and route-control changes serialized on
//!   the packet-loop task
//! - expose the small helpers that packet-loop code reuses directly, such as
//!   keyframe requests for already-resolved sources

mod bwe;
mod dispatcher;
mod media;
mod negotiation;
mod publication;
mod recv_stream;
mod session;

pub(in crate::engine::media_transport::rtc) use dispatcher::{
    WorkerCommandContext, handle_worker_command,
};
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use media::worker_set_consumer_pkt_gates_for_bench;
pub(in crate::engine::media_transport::rtc) use media::{
    KeyframeRequestMode, KeyframeRequestTarget, apply_src_rid_ready, drain_due_rid_kf_refreshes,
    request_kf_for_target,
};
