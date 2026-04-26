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

pub type RuntimeSfuCore = SfuCore<RuntimeTransportAdapter>;
