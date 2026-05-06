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
//! - [`MediaTransport`] and [`RtcTransport`] as the runtime media transport
//!   boundary over the concrete RTC engine.
//! - the transport concern traits in [`transport`], especially
//!   [`TransportFacade`] when a caller needs one backend with negotiation,
//!   media, and observability capabilities, and the narrower port traits when a
//!   caller only needs one concern.
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
//! use o_sfu_core::{CoreOptions, MediaCore, RtcTransport};
//! use o_sfu_core::server::room::Room;
//! use o_sfu_core::server::session::UserId;
//! use o_sfu_core::ConnectionId;
//!
//! async fn create_offer(
//!     core: &MediaCore,
//!     room: &Room,
//!     user_id: &UserId,
//!     connection_id: ConnectionId,
//! ) -> Result<(), o_sfu_core::SfuCoreError> {
//!     let session = core.session(room, user_id, connection_id);
//!     let (offer, capabilities) = session.create_initial_offer().await?;
//!     session.apply_initial_answer(&offer.sdp, &capabilities).await?;
//!     Ok(())
//! }
//!
//! fn build_core(options: CoreOptions, transport: RtcTransport) -> MediaCore {
//!     MediaCore::new(options, o_sfu_core::MediaTransport::from_rtc_transport(transport))
//! }
//! ```
//!
//! `o-sfu-core` keeps the core transport backend generic for tests and future
//! adapters, but the session facade targets the runtime
//! [`server::room::Room`] implementation. Normal server application code should
//! use [`MediaCore`] and should not become generic over transport backends
//! just because the core can be.
mod ids;
mod options;
mod room;
mod runtime;
pub mod server;
mod sfu;
pub mod transport;

pub use ids::{ConnectionId, RoomInstanceId};
pub use options::{
    AudioCodecPreference, CodecOptions, CodecPreferences, CoreOptions, LocalSpilloverPolicy,
    MediaCodecFlags, MediaOptions, ObservabilityOptions, RoomShardingPolicy, RoomSpilloverMode,
    RoutingOptions, RtcPortRange, RuntimeFeatureFlags, SessionBitrateLimits, VideoBitrateLimits,
    VideoCodecPreference,
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
pub use transport::TransportFacade;

/// Production media-core facade used by the server runtime.
///
/// This alias fixes [`SfuCore`] to the cfg-selected [`MediaTransport`] backend.
/// Normal server application code should depend on this type instead of being
/// generic over transport backends.
pub type MediaCore = SfuCore<MediaTransport>;
