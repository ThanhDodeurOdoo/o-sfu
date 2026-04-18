#[cfg(any(test, feature = "testing-transport"))]
mod fake;
#[cfg(test)]
mod fake_bootstrap;

mod config;
mod facade;
mod shard_set;
#[cfg(any(test, feature = "testing-transport"))]
pub(crate) mod test_support;
mod types;

pub(crate) use config::{RtcTransportAdapterConfig, RtcTransportAdapterShardSetConfig};
pub(crate) use facade::RuntimeTransportAdapter;
pub(crate) use types::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};

#[cfg(test)]
mod tests;
