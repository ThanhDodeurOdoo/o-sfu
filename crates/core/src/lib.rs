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
//! - the transport concern traits in [`transport`]. Use the narrow port trait
//!   for the concern a caller needs.
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
//! use o_sfu_core::{CoreOptions, MediaCore, MediaTransport};
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
//! fn build_core(options: CoreOptions, transport: MediaTransport) -> MediaCore {
//!     MediaCore::new(options, transport)
//! }
//! ```
//!
//! `o-sfu-core` keeps backend selection behind [`MediaTransport`], while the
//! session facade targets the runtime [`server::room::Room`] implementation.
//! Normal server application code should use [`MediaCore`] and should not name
//! concrete RTC workers or fake transport variants.
mod bitrate;
mod ids;
mod options;
mod room;
mod runtime;
pub mod server;
mod sfu;
pub mod transport;

pub use bitrate::Bitrate;
pub use ids::{ConnectionId, RoomInstanceId};
pub use options::{
    AudioCodecPreference, CodecOptions, CodecPreferences, CoreOptions, LocalSpilloverPolicy,
    LocalSpilloverPolicyError, LocalSpilloverPolicyParts, MediaCodecFlags, MediaOptions,
    ObservabilityOptions, RoomShardingPolicy, RoomSpilloverMode, RoutingOptions, RtcPortRange,
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

/// Production media-core facade used by the server runtime.
///
/// Normal server application code should depend on this type instead of naming
/// transport construction details.
pub type MediaCore = SfuCore;
