//! Media-core facade for the `o-sfu` server.
//!
//! The supported front door is intentionally small:
//!
//! - configuration values such as [`CoreOptions`], [`MediaOptions`],
//!   [`RoutingOptions`], [`CodecOptions`], [`ObservabilityOptions`],
//!   [`RtcPortRange`], [`SessionBitrateLimits`], [`MediaCodecFlags`], and
//!   [`RuntimeFeatureFlags`];
//! - [`SfuCore`], the media facade used by the server application to express
//!   endpoint health checks, offer/answer negotiation, publication, subscription,
//!   and cleanup intent;
//! - [`MediaRoom`], the room bridge implemented by the runtime room engine;
//! - [`RuntimeTransportAdapter`], the production transport backend facade over
//!   the concrete RTC adapter;
//! - the transport concern traits in [`transport`], especially
//!   [`TransportFacade`] when a caller needs one backend with negotiation,
//!   media, and observability capabilities, and the narrower port traits when a
//!   caller only needs one concern;
//! - diagnostics and metrics DTOs under [`runtime::diagnostics`] and
//!   [`runtime::metrics`], which remain part of the current server integration
//!   contract.
//!
//! The broad [`runtime`] module and the standalone [`signals`] vocabulary are
//! still public because the server crate and integration tests consume them
//! directly. Treat those modules as transitional internals unless a type is
//! re-exported at the crate root or explicitly documented by its module as a
//! supported extension point. New public items should first fit the front door
//! above; otherwise they need an architecture note explaining why they are
//! intentionally exposed.
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
mod sfu;
pub mod signals;
pub mod transport;

pub use ids::{ConnectionId, RoomInstanceId};
pub use options::{
    CodecOptions, CoreOptions, MediaCodecFlags, MediaOptions, ObservabilityOptions, RoutingOptions,
    RtcPortRange, RuntimeFeatureFlags, SessionBitrateLimits,
};
pub use room::MediaRoom;
pub use runtime::transport_adapter::RuntimeTransportAdapter;
pub use sfu::{
    MediaEndpointHealth, MediaNegotiationOffer, MediaUploadEncoding, MediaUploadSlot,
    OfferedMediaCapabilities, SfuCore, SfuCoreError,
};
pub use transport::TransportFacade;

pub type RuntimeSfuCore = SfuCore<RuntimeTransportAdapter>;
