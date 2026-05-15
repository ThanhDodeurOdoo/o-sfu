//! Media-core facade for the `o-sfu` server.
//!
//! The supported front door is intentionally small:
//!
//! - configuration values such as [`CoreOptions`], [`MediaOptions`],
//!   [`RoutingOptions`], [`CodecOptions`], [`ObservabilityOptions`],
//!   [`RtcPortRange`], [`SessionBitrateLimits`], [`VideoBitrateLimits`],
//!   [`MediaCodecFlags`], [`CodecPreferences`], and [`RuntimeFeatureFlags`].
//! - [`SfuCore`] and its borrow-based [`MediaSession`] handle, used by the
//!   server application to express endpoint health checks, offer/answer
//!   negotiation, publication, subscription, and cleanup intent. `SfuCore`
//!   constructs sessions, while media operations live on `MediaSession`.
//! - [`NegotiationOffer`], [`UploadSlot`], and [`UploadEncoding`], the
//!   transport-neutral negotiation vocabulary exposed by the core front door.
//! - [`MediaSessionContext`], the room-owned identity bundle carried by
//!   [`MediaSession`].
//! - semantic media intent and outcome types such as [`PublicationActivity`],
//!   [`PublishStageOutcome`], [`UnpublishOutcome`] and [`UserInfoRefresh`] for
//!   caller-facing control decisions.
//! - [`MediaTransport`] as the runtime media transport facade, with
//!   [`RtcTransport`] and [`RtcTransportBuilder`] kept as RTC construction
//!   handles below that facade.
//! - server-integration DTOs and facades under [`server`], including
//!   diagnostics, metrics, room orchestration, recording taps, source
//!   descriptors, and current transport construction seams.
//!
//! The implementation-heavy runtime tree is private. Integration tests and
//! server code use [`server`] for in-repository integration and crate-root
//! re-exports for the stable media-core front door. New public items should
//! first fit the front door above or the explicit server-integration namespace.
//! Otherwise they need an architecture note explaining why they are
//! intentionally exposed.
//!
//! # Server-facing example
//!
//! ```rust,no_run
//! use o_sfu_core::{CoreOptions, MediaTransport, SfuCore};
//! use o_sfu_core::server::room::Room;
//! use o_sfu_core::server::session::UserId;
//! use o_sfu_core::ConnectionId;
//!
//! async fn create_offer(
//!     core: &SfuCore,
//!     room: &Room,
//!     user_id: &UserId,
//!     connection_id: ConnectionId,
//! ) -> Result<(), o_sfu_core::SfuCoreError> {
//!     let session = core.session(room, user_id, connection_id);
//!     let (offer, capabilities) = session.create_initial_offer().await?;
//!     let browser_answer_sdp = exchange_offer_with_browser(offer.sdp).await?;
//!     session
//!         .apply_initial_answer(&browser_answer_sdp, &capabilities)
//!         .await?;
//!     Ok(())
//! }
//!
//! async fn exchange_offer_with_browser(
//!     _offer_sdp: String,
//! ) -> Result<String, o_sfu_core::SfuCoreError> {
//!     Ok(String::from("v=0\r\n"))
//! }
//!
//! fn build_core(options: CoreOptions, transport: MediaTransport) -> SfuCore {
//!     SfuCore::new(options, transport)
//! }
//! ```
//!
//! `o-sfu-core` keeps backend selection behind [`MediaTransport`], while the
//! session facade targets the runtime [`server::room::Room`] implementation.
//! Normal server application code should use [`SfuCore`] and should not name
//! concrete RTC workers or fake transport variants.
use std::fmt::{self, Display, Formatter};

mod options;
mod room;
mod runtime;
pub mod server;
mod sfu;

pub use options::{
    AudioCodecPreference, CodecOptions, CodecPreferences, CoreOptions, LocalSpilloverPolicy,
    LocalSpilloverPolicyError, LocalSpilloverPolicyParts, MediaCodecFlags, MediaOptions,
    ObservabilityOptions, RoomSpilloverMode, RoomWorkerPolicy, RoutingOptions, RtcPortRange,
    RuntimeFeatureFlags, SessionBitrateLimits, VideoBitrateLimits, VideoCodecPreference,
};
pub use room::{
    MediaSessionContext, PublicationActivity, PublicationActivityOutcome, PublishStageOutcome,
    RollbackStagedPublishOutcome, SessionNegotiationOutcome, SubscriptionUpdateOutcome,
    TransportEffectOutcome, UnpublishOutcome, UserInfoRefresh,
};
pub use runtime::{
    media_transport::{MediaTransport, RtcTransport, RtcTransportBuildError, RtcTransportBuilder},
    source_model::{
        ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
        SourceLayoutPolicy, SourcePolicy, SourcePublishIntent, SourceRoomPolicySelector,
        SourceSubscriptionIntent, UploadLayerPolicyRole, UserStreamId,
    },
};
pub use sfu::{
    MediaEndpointHealth, MediaSession, NegotiationOffer, OfferedMediaCapabilities, SfuCore,
    SfuCoreError, UploadEncoding, UploadSlot,
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
