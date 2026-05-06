//! Runtime media transport boundary used by core orchestration.
//!
//! This module owns the service surface that room, server, and [`crate::SfuCore`]
//! code use when they need media work to happen outside the pure router. It is
//! intentionally named after the capability it provides rather than the backend
//! that currently implements it: callers ask the media transport to create SDP
//! offers, apply answers, publish or consume media, close sessions, read
//! diagnostics snapshots, and subscribe to source-policy wakeups.
//!
//! The boundary has three responsibilities:
//!
//! - expose the opaque [`MediaTransport`] handle and the narrow construction
//!   inputs needed by the server runtime;
//! - implement the transport port traits from [`crate::transport`] so higher
//!   layers express intent without knowing about RTC workers, Str0m state, UDP
//!   sockets, worker-local relay routing, or deterministic test backends;
//! - select the active backend for the build. Production builds wrap the real
//!   RTC engine through [`RtcTransport`], while test builds can also select a
//!   deterministic fake transport without putting fake-only behavior on the
//!   production path.
//!
//! Code above this module should depend on [`MediaTransport`] or one of the
//! concern-oriented port traits. Code below this module, especially the RTC
//! engine, may deal with worker-local state machines and packet-loop details.
//! Keeping that split explicit prevents room and signaling orchestration from
//! growing knowledge of the concrete WebRTC implementation.

mod config;
mod runtime_adapter;
mod shard_set;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "test_support/transport_backend.rs"]
mod transport_backend;
#[cfg(any(test, feature = "testing-transport"))]
use transport_backend::MediaTransportBackend;
#[cfg(not(any(test, feature = "testing-transport")))]
pub(super) type MediaTransportBackend = runtime_adapter::RtcTransport;

pub use config::{MediaTransportDeps, RtcTransportConfig, RtcTransportShardSetConfig};
pub use runtime_adapter::{
    MediaTransport, RtcTransport, RtcTransportBuildError, RtcTransportBuilder,
};

pub use crate::transport::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerActivity,
    ConsumerPacketGateUpdate, MediaPort, ObservabilityPort, ProducerActivity,
    ReceiverBandwidthSnapshot, SessionOffer, SessionPort, SessionUploadEncoding, SessionUploadSlot,
    SourcePacketGate, SourcePacketOperatingPoint, SourcePolicySignal, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportResult, TransportSessionKey,
};

#[cfg(test)]
mod tests;
