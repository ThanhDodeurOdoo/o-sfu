//! Pure channel-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `fanout` owns outbound fan-out preparation from state snapshots.
//! `layout` owns server-driven session layout state.
//! `presence` owns client-driven session presence state.
//! `recording` owns channel recording-control state updates and fan-out.
//! `session_info_projection` owns outward-facing session and peer projection.
//! `source_packet_policy` owns room-level source-layer selection planning.
//! `shared` owns the in-memory state and bookkeeping types.
//! `membership` owns session lifecycle, presence fan-out, and negotiation readiness.
//! `media` owns producer/consumer bootstrap and routing-side media bookkeeping.
//! `test_support` owns read-only state inspectors used only by tests.

mod diagnostics;
mod fanout;
mod ids;
mod layout;
mod media;
mod membership;
mod presence;
mod recording;
mod session_info_projection;
mod shared;
mod source_packet_policy;
#[cfg(test)]
mod test_support;

pub(crate) use self::media::ConsumerRouteState;
pub(crate) use self::media::RemoteTrackBootstrap;
pub(in crate::runtime::channel) use self::media::{
    ConsumerBootstrapOrigin, ConsumerRouteUpdate, PendingConsumerBootstrap,
    PendingConsumerBootstrapTarget, PreparedConsumerBootstrap, PreparedPublishedTrack,
    ValidatedPublishDescriptor,
};
pub(in crate::runtime::channel) use self::membership::LifecycleEffects;
pub(in crate::runtime::channel) use self::membership::{
    DisconnectSessionsOutcome, JoinSessionOutcome, LeaveSessionOutcome,
};
pub(in crate::runtime::channel) use self::shared::{ChannelState, TransportMediaRemoval};
pub(in crate::runtime::channel) use self::source_packet_policy::{
    FeaturedSessionUpdate, SourcePacketSelectionUpdate,
};
