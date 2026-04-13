//! Pure channel-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `shared` owns the in-memory state and bookkeeping types.
//! `membership` owns session lifecycle, info fan-out, and negotiation readiness.
//! `media` owns producer/consumer bootstrap and routing-side media bookkeeping.

mod ids;
mod media;
mod membership;
mod shared;

pub(in crate::runtime::channel) use self::media::{
    ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget,
};
pub(in crate::runtime::channel) use self::shared::{ChannelState, TransportMediaRemoval};
