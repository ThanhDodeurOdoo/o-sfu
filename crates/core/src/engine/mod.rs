//! Private media engine implementation tree.
//!
//! This tree owns the room engine, media transport facade, RTC worker implementation,
//! recording hooks, diagnostics projections, metrics bridge and source policy
//! machinery used below the public `o-sfu-core` front door.
//!
//! The media engine has to coordinate state that is too concrete for the pure
//! router and too low level for the server shell: room membership, publish or
//! subscribe transactions, transport cleanup, packet sinks, relay setup, packet-
//! loop observations and room media policy. Keeping those pieces together
//! lets them share media-engine state without making the server crate import
//! RTC workers or room-state internals.

pub mod diagnostics;
pub mod h264_payloads;
mod hot_path;
pub mod media_transport;
pub mod metrics;
pub mod packet_sink_registry;
pub mod recording;
pub mod room;
pub mod router_events;
pub mod source_model;
pub mod sync;

pub use o_sfu_model::{
    AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState,
    RecordingStateUpdate, StopCode, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
    WebSocketCloseCode,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use source_model::test_support::TestSourceKind;

pub use crate::{ConnectionId, MediaWorkerId, RoomInstanceId};
