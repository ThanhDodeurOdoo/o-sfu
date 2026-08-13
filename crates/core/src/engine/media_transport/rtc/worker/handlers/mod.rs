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

pub use dispatcher::{WorkerCommandContext, handle_worker_command};
#[cfg(feature = "internal-benchmarks")]
pub use media::apply_media_control_batch;
pub use media::{
    KeyframeRequestMode, KeyframeRequestTarget, apply_src_decoder_ready, request_kf_for_target,
};
pub(in crate::engine::media_transport::rtc) use media::{consumer_payload_type, guarded_pkt_gate};
