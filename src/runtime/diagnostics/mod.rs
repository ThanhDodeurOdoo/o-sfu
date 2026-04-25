//! Diagnostics is the operator-facing (devops, infra,...) read and event boundary for the runtime.
//!
//! It exists so `/internal/diagnostics/...` can expose live channel and session
//! state without reaching into packet-loop internals or mixing storage,
//! serialization, and query assembly in one place.
//!
//! sub parts:
//! - `store` keeps the bounded recent-event history and the instance-id to
//!   channel-uuid mapping used by transport-side event observation.
//! - `types` defines the serialzied views returned to operators and the small
//!   transport-health conversion helpers shared by diagnostics emitters.
//! - `queries` allow live responses from `ChannelManager`,
//!   `ObservabilityPort`, and `DiagnosticsStore`.
//!
//! Callers generally use this boundary in two ways:
//! - runtime and transport code record notable lifecycle events into
//!   `DiagnosticsStore`
//! - HTTP diagnostics routes ask `queries` for summary, channel, or session
//!   views when an operator requests them

mod queries;
mod store;
pub(crate) mod types;

pub(crate) use queries::{
    channel_detail_response, channels_response, session_detail_response, summary_response,
};
pub(crate) use store::{DiagnosticsEventData, DiagnosticsStore};
pub(crate) use types::{
    DiagnosticsIncomingBitrate, DiagnosticsMediaKind, DiagnosticsPublication,
    DiagnosticsQualitySummary, DiagnosticsRouteState, DiagnosticsSessionLookup,
    DiagnosticsSessionTransport, DiagnosticsSessionView, DiagnosticsSource,
    DiagnosticsSourceEncoding, DiagnosticsSourceSelection, DiagnosticsSubscription,
    health_json_value, maybe_health_json_value,
};
