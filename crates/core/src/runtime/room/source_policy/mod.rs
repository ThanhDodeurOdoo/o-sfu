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
mod active_speaker;
mod audio;
mod commit;
mod effects;
mod sync;
mod video;

pub(in crate::runtime::room) use self::{
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
    active_speaker::rank_active_speaker_sources,
    effects::SourcePolicyEffectPlan,
    sync::SourcePolicyEvent,
    video::VideoAdmissionRank,
};
