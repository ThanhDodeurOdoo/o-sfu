//! Runtime media transport implementation namespace.
//!
//! This module exposes the opaque [`MediaTransport`] boundary plus the narrow
//! construction types needed by the server runtime. Production builds select
//! the RTC backend module. Test builds select a backend module that can hold
//! both real RTC and deterministic fake transport without mixing fake-only code
//! into the production boundary.

#[cfg(any(test, feature = "testing-transport"))]
mod fake;

mod config;
mod runtime_adapter;
mod shard_set;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
#[cfg(not(any(test, feature = "testing-transport")))]
mod transport_backend;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "transport_backend_test.rs"]
mod transport_backend;

pub use config::{
    MediaTransportDeps, RtcTransportAdapterConfig, RtcTransportAdapterDeps,
    RtcTransportAdapterShardSetConfig,
};
pub use runtime_adapter::{
    MediaTransport, RtcTransport, RtcTransportBuildError, RtcTransportBuilder,
    RuntimeTransportAdapter,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use transport_backend::TestTransport;

pub use crate::{
    SessionBitrateLimits,
    transport::{
        ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
        ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerActivity,
        ConsumerPacketGateUpdate, MediaPort, NegotiationPort, ObservabilityPort, ProducerActivity,
        ReceiverBandwidthSnapshot, SessionOffer, SessionPort, SessionUploadEncoding,
        SessionUploadSlot, SourcePacketGate, SourcePacketOperatingPoint, SourcePolicyDirtyState,
        SourcePolicyPort, SourcePolicySignal, SourcePolicyUpdateSubscription,
        TransportAdapterError, TransportBitrateSnapshot, TransportMediaId, TransportResult,
        TransportSessionKey,
    },
};

#[cfg(test)]
mod tests;
