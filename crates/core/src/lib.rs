//! Media-core facade for the `o-sfu` server.
//!
//! Stable callers use [`prelude`] for ordinary media work and [`server`] only
//! when they need concrete room, transport, diagnostics, metrics or recording
//! integration.
//!
//! The crate root keeps only fundamental value types: [`Bitrate`],
//! [`ConnectionId`] and [`RoomInstanceId`]. The private engine tree stays
//! hidden so new exposed types must fit [`prelude`] or [`server`] first.
use std::fmt::{self, Display, Formatter};

mod engine;
mod options;
pub mod prelude;
mod room;
pub mod server;
mod sfu;

pub(crate) use options::{
    AudioCodecPreference, CodecPreferences, CoreOptions, LocalSpilloverPolicy, MediaCodecFlags,
    RoomMediaLimits, RoomSpilloverMode, RoomWorkerPolicy, RtcPortRange, RuntimeFeatureFlags,
    SessionBitrateLimits, VideoBitrateLimits, VideoCodecPreference,
};
pub(crate) use room::{
    MediaSessionIdentity, PublicationActivity, PublicationActivityOutcome, PublishStageOutcome,
    RollbackStagedPublishOutcome, SessionNegotiationOutcome, SubscriptionUpdateOutcome,
    TransportEffectOutcome, UnpublishOutcome, UserInfoRefresh,
};

/// Media bitrate stored as bits per second (not bytes per second).
///
/// This type is the core-domain value for transport caps, pressure snapshots,
/// source metadata and media-policy budgets. Convert to raw bps only at
/// environment, wire, telemetry and backend-library boundaries. Packet byte
/// counters, RTP payload lengths and buffer sizes should stay as byte counts so
/// the unit difference remains visible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bitrate(u64);

impl Bitrate {
    #[must_use]
    pub const fn from_bps(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn from_kbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_mbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_bps(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    #[must_use]
    pub const fn divided_by(self, divisor: u64) -> Self {
        match self.0.checked_div(divisor) {
            Some(value) => Self(value),
            None => Self::zero(),
        }
    }
}

/// unique identifier for a user's transport connection within the server process
///
/// this separates the ephemeral transport lifecycle from the persistent logical
/// user identity. a single user might create multiple connections over time due to
/// network drops or handovers. this identifier ensures media operations only apply
/// to the specific transport they were negotiated against
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    #[must_use]
    pub fn allocate(next_connection_id: &mut u64) -> Self {
        let connection_id = Self(*next_connection_id);
        *next_connection_id = next_connection_id.saturating_add(1);
        connection_id
    }

    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for ConnectionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// unique identifier for a running room instance within the server process
///
/// this separates a specific runtime allocation from the overarching application
/// room identity. it helps telemetry, logging, and underlying components distinguish
/// between consecutive lifecycles of the same room if it is torn down and recreated
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoomInstanceId(u64);

impl RoomInstanceId {
    #[must_use]
    pub fn allocate(next_room_instance_id: &mut u64) -> Self {
        let room_instance_id = Self(*next_room_instance_id);
        *next_room_instance_id = next_room_instance_id.saturating_add(1);
        room_instance_id
    }

    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for RoomInstanceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
