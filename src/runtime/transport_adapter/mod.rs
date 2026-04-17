#[cfg(any(test, feature = "testing-transport"))]
mod fake;
#[cfg(test)]
mod fake_bootstrap;

mod config;
mod facade;
mod shard_set;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;
mod types;

pub(crate) use config::{RtcTransportAdapterConfig, RtcTransportAdapterShardSetConfig};
pub(crate) use facade::RuntimeTransportAdapter;
pub(crate) use types::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};

#[cfg(test)]
pub(crate) use types::{TransportConnectDirection, TransportConnectRequest};

#[cfg(any(test, feature = "testing-transport"))]
#[allow(
    unused_imports,
    reason = "the fake adapter stays re-exported for transport-focused tests and the feature-gated development seam"
)]
pub(crate) use test_support::FakeWebRtcAdapter;
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "fake transport events are re-exported for test modules even when one edit removes their current call sites"
)]
pub(crate) use test_support::FakeWebRtcEvent;

#[cfg(test)]
mod tests;
