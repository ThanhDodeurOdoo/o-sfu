//! backend-neutral media transport contract
//!
//! in this crate, "transport" means the boundary that turns room-owned media
//! intent into work performed by a concrete media backend
//!
//! the room layer decides who is in a room, who published a source, who
//! subscribed to it, which source layers are allowed by policy and when a
//! participant should be cleaned up. those decisions are domain state. they
//! should not depend on whether media is executed by the real RTC engine, a
//! deterministic test transport or a later backend
//!
//! the transport layer owns the execution vocabulary below that room state. it
//! creates and applies offers, allocates backend media ids, connects producer
//! and consumer routes, toggles packet delivery, exposes best-effort transport
//! observations and releases backend resources. those operations are not room
//! policy. they are the controlled side effects needed to make room policy real
//! on a media backend
//!
//! this directory exists as its own boundary because those contracts are shared
//! by both sides:
//!
//! - room and server code use these traits and values to ask for media work
//! - runtime backends implement these traits to perform that work
//! - deterministic tests use the same contracts without importing RTC worker
//!   internals
//!
//! keeping this vocabulary outside `runtime/media_transport` matters because
//! `runtime/media_transport` is the backend selection and adapter layer. it is
//! allowed to know about the active backend. this module is not. it names the
//! stable contract that survives backend swaps
//!
//! ```text
//! room or server intent
//!        |
//!        v
//! crate::transport ports and dto values
//!        |
//!        v
//! runtime::media_transport backend adapter
//!        |
//!        +--> real RTC workers
//!        |
//!        +--> deterministic test transport
//! ```
//!
//! the split also keeps call sites honest. a function that only closes a user
//! should depend on [`SessionPort`], not on a full media transport handle. a
//! function that only needs live bitrate or active-speaker snapshots should
//! depend on [`ObservabilityPort`]. this keeps orchestration code explicit about
//! which side effects it is allowed to perform
//!
//! `ports` owns the concern-oriented traits such as [`NegotiationPort`],
//! [`MediaPort`], [`SessionPort`], [`ObservabilityPort`] and
//! [`SourcePolicyPort`]
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

mod ports;
mod source_policy;
mod types;

pub use ports::{
    ConsumerActivity, MediaPort, NegotiationPort, ObservabilityPort, ProducerActivity, SessionPort,
    SourcePolicyPort,
};
pub use source_policy::{
    SourcePolicyDirtyState, SourcePolicySignal, SourcePolicyUpdateSubscription,
};
pub use types::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerPacketGateUpdate,
    ReceiverBandwidthSnapshot, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
    SourcePacketGate, SourcePacketOperatingPoint, TransportAdapterError, TransportBitrateSnapshot,
    TransportMediaId, TransportPlacementPressureSnapshot, TransportRelayRouteAction,
    TransportRelayRouteEffect, TransportResult, TransportSessionHealth, TransportSessionKey,
    TransportWorkerPressureSnapshot,
};
