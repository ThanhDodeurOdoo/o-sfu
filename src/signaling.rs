pub mod auth;
pub mod bundle_api;
pub mod client_batch;
pub mod http;
#[doc(hidden)]
#[allow(
    dead_code,
    reason = "ORTC JSON compatibility helpers are still retained for the bundle-facing migration notes and remaining differential coverage while native signaling becomes the only production path"
)]
pub mod ortc_mapper;
pub mod protocol;
pub mod shared;
#[doc(hidden)]
#[allow(
    dead_code,
    reason = "some codec and feedback serializers remain shared support for the compatibility conversion layer until the next signaling cleanup removes that layer entirely"
)]
pub mod webrtc;

pub const DEFAULT_AUTHENTICATION_TIMEOUT_MS: u64 = 10_000;
