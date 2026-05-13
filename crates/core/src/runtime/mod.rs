//! Transitional runtime integration namespace.
//!
//! This module remains public while the server crate and integration tests
//! migrate to explicit supported paths. Its submodules expose runtime, room,
//! recording, diagnostics, metrics, media transport, and RTC engine details
//! that are not automatically part of the stable `o-sfu-core` front door. New
//! public consumers should prefer crate-root re-exports or module paths
//! documented in the API surface policy.
//!
//! A public item under this module is stable only when its owning module says
//! so explicitly. Otherwise it is server-integration or transitional API and
//! may move behind narrower re-exports during the cleanup sequence.

pub mod diagnostics;
pub mod media_transport;
pub mod metrics;
pub mod packet_sink_registry;
pub mod recording;
pub mod room;
pub(in crate::runtime) mod router_events;
pub mod rtc_engine;
pub mod session_types;
pub mod source_model;
pub mod telemetry;

pub use session_types::{
    AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState,
    RecordingStateUpdate, StopCode, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
    WebSocketCloseCode,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use source_model::test_support::TestSourceKind;

pub use crate::{ConnectionId, RoomInstanceId};
