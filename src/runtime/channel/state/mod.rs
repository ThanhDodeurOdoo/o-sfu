//! Pure channel-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `fanout` owns outbound fan-out preparation from state snapshots.
//! `layout` owns server-driven session layout state.
//! `presence` owns client-driven session presence state.
//! `recording` owns channel recording-control state updates and fan-out.
//! `session_info_projection` owns outward-facing session and peer projection.
//! `video_policy` owns room-level source selection input, planning, projection, and commit.
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
#[cfg(test)]
mod test_support;
mod video_policy;

pub(crate) use self::media::{ConsumerRouteState, RemoteTrackBootstrap};
pub(in crate::runtime::channel) use self::{
    media::{
        ConsumerBootstrapOrigin, ConsumerRouteUpdate, PendingConsumerBootstrap,
        PendingConsumerBootstrapTarget, PlannedConsumerBootstrap, PlannedSubscriptionChange,
        PreparedConsumerBootstrap, ValidatedPublishDescriptor,
    },
    membership::{
        DisconnectSessionsOutcome, JoinSessionOutcome, LeaveSessionOutcome, LifecycleEffects,
    },
    shared::{ChannelState, TransportMediaRemoval},
    video_policy::{ConsumerPacketSelectionUpdate, FeaturedSessionUpdate},
};
