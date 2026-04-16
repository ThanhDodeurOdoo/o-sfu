//! Runtime transport adapter facade for the `rtc` WebRTC backend.

mod facade;
mod runtime;

#[cfg(feature = "internal-benchmarks")]
mod benchmarks;

#[cfg(test)]
mod test_support;

pub(crate) use facade::RtcTransportAdapter;
