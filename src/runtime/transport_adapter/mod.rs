#[cfg(any(test, feature = "testing-transport"))]
mod fake;

mod config;
mod facade;
mod shard_set;
mod source_policy;
#[cfg(any(test, feature = "testing-transport"))]
pub(crate) mod test_support;
mod types;

pub(crate) use config::{
    RtcTransportAdapterConfig, RtcTransportAdapterShardSetConfig, SessionBitrateLimits,
};
pub(crate) use facade::{
    MediaPort, NegotiationPort, ObservabilityPort, RuntimeTransportAdapter, SessionPort,
    SourcePolicyPort,
};
pub(crate) use source_policy::{
    SourcePolicyDirtyState, SourcePolicySignal, SourcePolicyUpdateSubscription,
};
pub(crate) use types::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportResult, TransportSessionKey,
};

#[cfg(test)]
mod tests;
