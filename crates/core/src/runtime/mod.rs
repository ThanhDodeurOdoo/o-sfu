//! Private media runtime implementation tree.
//!
//! This is the core crate's media engine internals. It is different from the
//! server process `Runtime` in `o-sfu/src/runtime/mod.rs`, which owns process boot,
//! HTTP and WebSocket serving plus task lifetime. This tree owns the concrete
//! room engine, media transport facade, RTC worker implementation, recording
//! hooks, diagnostics projections, metrics bridge and source policy machinery
//! used below the public `o-sfu-core` front door.
//!
//! The tree is useful because the media engine has to coordinate state that is
//! too concrete for the pure router and too low level for the server shell:
//! room membership, publish or subscribe transactions, transport cleanup,
//! packet sinks, relay setup, packet-loop observations and room-owned media
//! policy. Keeping those pieces together lets them share concrete runtime state
//! without making the server crate import RTC workers or room-state internals.
//!
//! Callers outside the core crate should not import this module. Stable media
//! operations go through [`crate::SfuCore`] and [`crate::MediaSession`]. Server
//! integration and transport extension points go through [`crate::server`]. A
//! type defined here becomes public only when it is re-exported through one of
//! those supported facades.

pub mod diagnostics;
mod hot_path;
pub mod media_transport;
pub mod metrics;
pub mod packet_sink_registry;
pub mod recording;
pub mod room;
pub(in crate::runtime) mod router_events;
pub mod rtc_engine;
pub mod source_model;

pub use o_sfu_model::{
    AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState,
    RecordingStateUpdate, StopCode, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
    WebSocketCloseCode,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use source_model::test_support::TestSourceKind;

pub use crate::{ConnectionId, RoomInstanceId};
