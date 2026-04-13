//! Pure channel-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `presence` owns client-driven session presence state.
//! `session_info_projection` owns compatibility projection from channel-owned state.
//! `shared` owns the in-memory state and bookkeeping types.
//! `membership` owns session lifecycle, presence fan-out, and negotiation readiness.
//! `media` owns producer/consumer bootstrap and routing-side media bookkeeping.

mod ids;
mod media;
mod membership;
mod presence;
mod session_info_projection;
mod shared;

pub(in crate::runtime::channel) use self::media::{
    ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, PendingPublishedTrack,
};
pub(in crate::runtime::channel) use self::shared::{ChannelState, TransportMediaRemoval};
