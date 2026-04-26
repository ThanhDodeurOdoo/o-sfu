//! Diagnostics is the operator-facing (devops, infra,...) read and event boundary for the runtime.
//!
//! It exists so `/internal/diagnostics/...` can expose live room and user
//! state without reaching into packet-loop internals or mixing storage,
//! serialization, and query assembly in one place.
//!
//! sub parts:
//! - `store` keeps the bounded recent-event history and the instance-id to
//!   room-uuid mapping used by transport-side event observation.
//! - `types` defines the serialzied views returned to operators and the small
//!   transport-health conversion helpers shared by diagnostics emitters.
//! - `queries` allow live responses from `RoomManager`,
//!   `ObservabilityPort`, and `DiagnosticsStore`.
//!
//! Callers generally use this boundary in two ways:
//! - runtime and transport code record notable lifecycle events into
//!   `DiagnosticsStore`
//! - HTTP diagnostics routes ask `queries` for summary, room, or user
//!   views when an operator requests them

mod queries;
pub(crate) mod types {
    pub(crate) use o_sfu_core::runtime::diagnostics::types::*;
}

pub(crate) use o_sfu_core::runtime::diagnostics::{DiagnosticsEventData, DiagnosticsStore};
pub(crate) use queries::{
    room_detail_response, rooms_response, summary_response, user_detail_response,
};
pub(crate) use types::{
    DiagnosticsActiveSpeaker, DiagnosticsIncomingBitrate, DiagnosticsMediaKind,
    DiagnosticsPublication, DiagnosticsQualitySummary, DiagnosticsRouteState, DiagnosticsSource,
    DiagnosticsSourceEncoding, DiagnosticsSourceSelection, DiagnosticsSubscription,
    DiagnosticsTemporalLayerMetadata, DiagnosticsTemporalLayerSelection, DiagnosticsUserLookup,
    DiagnosticsUserTransport, DiagnosticsUserView, DiagnosticsVideoLayoutRole,
    DiagnosticsVideoRoutePriority,
};
