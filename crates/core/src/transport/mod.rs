//! backend-neutral media transport vocabulary
//!
//! in this crate, "transport" names the ids, DTOs, outcomes and wakeup signals
//! that cross the media backend boundary
//!
//! the room layer decides who is in a room, who published a source, who
//! subscribed to it, which source layers are allowed by policy and when a
//! participant should be cleaned up. those decisions are domain state. they
//! should not depend on whether media is executed by the real RTC engine, a
//! deterministic test transport or a later backend
//!
//! [`crate::runtime::media_transport::MediaTransport`] owns the side-effecting
//! operations that make that room policy real on a backend. this module owns the
//! stable values those operations exchange with room, server, diagnostics and
//! RTC code
//!
//! this directory exists as its own boundary because this vocabulary is shared
//! by both sides:
//!
//! - room and server code use these values to describe media work
//! - runtime backends use the same values while performing that work
//! - deterministic tests use the same contracts without importing RTC worker
//!   internals
//!
//! keeping this vocabulary outside `runtime/media_transport` matters because the
//! runtime media transport is the backend selection and adapter layer. it is
//! allowed to know about the active backend. this module is not. it names the
//! stable data contract that survives backend swaps
//!
//! ```text
//! room or server intent
//!        |
//!        v
//! crate::transport dto values
//!        |
//!        v
//! runtime::media_transport::MediaTransport
//!        |
//!        +--> real RTC workers
//!        |
//!        +--> deterministic test transport
//! ```
//!
//! `types` owns the ids, snapshots, outcomes and error values that cross the
//! boundary. a [`TransportMediaId`] is backend-local execution state, not a room
//! source id. a [`TransportSessionKey`] is the transport identity for one
//! room-scoped user connection, not a user id by itself
//!
//! `source_policy` owns the coalesced wakeup bridge used when transport
//! observations change source-policy inputs. the room still recomputes policy
//! from current state. the bridge only says which room instances need another
//! policy pass
//!
//! this root re-exports the boundary so callers can import one
//! `crate::transport` vocabulary instead of depending on the private file split

mod source_policy;
mod types;

pub use source_policy::{
    SourcePolicyDirtyState, SourcePolicySignal, SourcePolicyUpdateSubscription,
};
pub use types::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
    ConsumerPacketGateUpdate, ProducerActivity, ReceiverBandwidthSnapshot, SessionOffer,
    SessionUploadEncoding, SessionUploadSlot, SourcePacketGate, SourcePacketOperatingPoint,
    TransportAdapterError, TransportBitrateSnapshot, TransportMediaId,
    TransportPlacementPressureSnapshot, TransportRelayRouteAction, TransportRelayRouteEffect,
    TransportResult, TransportSessionHealth, TransportSessionKey, TransportWorkerPressureSnapshot,
};
