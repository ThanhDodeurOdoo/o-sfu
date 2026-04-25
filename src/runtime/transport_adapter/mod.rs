#[cfg(any(test, feature = "testing-transport"))]
mod fake;

mod config;
mod ports;
mod rtc_backend;
mod runtime_adapter;
mod shard_set;
mod source_policy;
#[cfg(any(test, feature = "testing-transport"))]
pub(crate) mod test_support;
mod types;

pub(crate) use config::{
    RtcTransportAdapterConfig, RtcTransportAdapterShardSetConfig, SessionBitrateLimits,
};
pub(crate) use ports::{
    MediaPort, NegotiationPort, ObservabilityPort, SessionPort, SourcePolicyPort,
};
pub(crate) use runtime_adapter::RuntimeTransportAdapter;
pub(crate) use source_policy::{
    SourcePolicyDirtyState, SourcePolicySignal, SourcePolicyUpdateSubscription,
};
pub use types::TransportSessionKey;
pub(crate) use types::{
    ActiveSpeakerSource, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
    TransportAdapterError, TransportBitrateSnapshot, TransportMediaId, TransportResult,
};

#[cfg(test)]
mod tests;
