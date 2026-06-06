//! Receiver video policy.
//!
//! Video policy owns receiver-specific route ranking, layout intent, selected
//! encoding projection and bandwidth solving. Shared source-policy actions live
//! one level up because audio admission also emits route-activity updates.

mod adaptation;
mod admission;
mod budget;
#[cfg(test)]
mod fixtures;
mod hysteresis;
mod input;
mod layout;
mod planner;
mod projection;

pub(in crate::engine::room) use layout::VideoAdmissionRank;
