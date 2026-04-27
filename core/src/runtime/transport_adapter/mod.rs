#[cfg(any(test, feature = "testing-transport"))]
mod fake;

mod config;
mod runtime_adapter;
mod shard_set;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;

pub use config::{RtcTransportAdapterConfig, RtcTransportAdapterShardSetConfig};
pub use runtime_adapter::RuntimeTransportAdapter;

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
