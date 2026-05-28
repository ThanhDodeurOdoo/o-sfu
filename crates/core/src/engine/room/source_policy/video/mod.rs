//! Receiver video policy.
//!
//! Video policy owns receiver-specific route ranking, layout intent, selected
//! encoding projection and bandwidth solving. Shared source-policy actions live
//! one level up because audio admission also emits route-activity updates.

mod budget;
mod input;
mod layout;
mod projection;

pub(in crate::engine::room) use layout::VideoAdmissionRank;
