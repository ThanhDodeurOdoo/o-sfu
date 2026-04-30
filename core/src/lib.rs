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
//! - [`MediaRoom`] and [`MediaSessionContext`], the room bridge implemented by
//!   the runtime room engine.
//! - semantic media intent and outcome types such as [`PublicationActivity`],
//!   [`PublishStageOutcome`], [`UnpublishOutcome`] and [`UserInfoRefresh`] for
//!   caller-facing control decisions.
//! - [`MediaTransport`], [`RtcTransport`], and [`RuntimeTransportAdapter`] as the
//!   runtime media transport boundary over the concrete RTC adapter.
//! - the transport concern traits in [`transport`], especially
//!   [`TransportFacade`] when a caller needs one backend with negotiation,
//!   media, and observability capabilities, and the narrower port traits when a
//!   caller only needs one concern.
//! - server-integration DTOs and facades under [`server`], including
//!   diagnostics, metrics, room orchestration, recording taps, source
//!   descriptors, and current transport construction seams.
//!
//! The broad [`runtime`] module is still public because integration tests and
//! unfinished migration work consume it directly. Production server code should
//! use [`server`] for in-repository integration and crate-root re-exports for
//! the stable media-core front door. The older media-signal vocabulary has moved
//! under [`unstable::signals`] because it is not the current server
//! orchestration path. New public items should first fit the front door above or
//! the explicit server-integration namespace. Otherwise they need an
//! architecture note explaining why they are intentionally exposed.
//!
//! # Server-facing example
//!
//! ```rust,no_run
//! use o_sfu_core::{CoreOptions, RuntimeSfuCore, RtcTransport};
//! use o_sfu_core::server::room::Room;
//! use o_sfu_core::server::session::UserId;
//! use o_sfu_core::ConnectionId;
//!
//! async fn create_offer(
//!     core: &RuntimeSfuCore,
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
//! fn build_core(options: CoreOptions, transport: RtcTransport) -> RuntimeSfuCore {
//!     RuntimeSfuCore::new(options, o_sfu_core::MediaTransport::from_rtc_transport(transport))
//! }
//! ```
//!
//! `o-sfu-core` keeps the core transport backend generic for tests and future
//! adapters, but normal server application code should use [`RuntimeSfuCore`]
//! and should not become generic over transport backends just because the core
//! can be.
#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    reason = "The physical core split temporarily exposes former server-internal media APIs; the next room-media migration will narrow and document the stable public surface."
)]
#![allow(
    dead_code,
    reason = "The testing-transport feature exposes cross-crate test seams used by selected server tests; not every helper is constructed in every all-features target."
)]

mod ids;
mod options;
mod room;
pub mod runtime;
pub mod server;
mod sfu;
pub mod transport;

mod signals;

pub mod unstable {
    //! Unstable media-core surfaces with no compatibility guarantee yet.
    //!
    //! These names remain available for experiments and migration work, but
    //! they are not part of the supported `o-sfu-core` front door.

    pub mod signals {
        pub use crate::signals::*;
    }
}

pub use ids::{ConnectionId, RoomInstanceId};
pub use options::{
    AudioCodecPreference, CodecOptions, CodecPreferences, CoreOptions, MediaCodecFlags,
    MediaOptions, ObservabilityOptions, RoomShardingPolicy, RoomSpilloverMode, RoutingOptions,
    RtcPortRange, RuntimeFeatureFlags, SessionBitrateLimits, VideoBitrateLimits,
    VideoCodecPreference,
};
pub use room::{
    MediaRoom, MediaSessionContext, PublicationActivity, PublicationActivityOutcome,
    PublishStageOutcome, RollbackStagedPublishOutcome, SessionNegotiationOutcome,
    SubscriptionUpdateOutcome, TransportEffectOutcome, UnpublishOutcome, UserInfoRefresh,
};
pub use runtime::{
    source_model::UploadLayerPolicyRole,
    transport_adapter::{
        MediaTransport, RtcTransport, RtcTransportBuildError, RtcTransportBuilder,
        RuntimeTransportAdapter,
    },
};
pub use sfu::{
    MediaEndpointHealth, MediaSession, NegotiationOffer, OfferedMediaCapabilities, SfuCore,
    SfuCoreError, UploadEncoding, UploadSlot,
};
#[allow(
    deprecated,
    reason = "The aliases are intentionally re-exported for one migration cycle."
)]
pub use sfu::{MediaNegotiationOffer, MediaUploadEncoding, MediaUploadSlot};
pub use transport::TransportFacade;

/// Production media-core facade used by the server runtime.
///
/// This alias fixes [`SfuCore`] to the cfg-selected [`MediaTransport`] backend.
/// Normal server application code should depend on this type instead of being
/// generic over transport backends.
pub type RuntimeSfuCore = SfuCore<MediaTransport>;
