pub mod auth;
pub mod bundle_api;
pub mod current_bus;
pub mod current_protocol;
pub mod http;
pub mod ortc_mapper;
pub mod shared;
pub mod webrtc;

pub const CURRENT_WIRE_PROTOCOL_VERSION: u16 = 1;
pub const DEFAULT_AUTHENTICATION_TIMEOUT_MS: u64 = 10_000;
