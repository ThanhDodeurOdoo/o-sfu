//! ================================================================
//! ===                 WORK IN PROGRESS                     =======
//! === <https://github.com/ThanhDodeurOdoo/o-sfu/issues/20> =======
//! ================================================================
mod service;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
mod user;

pub(crate) use service::RecordingService;

pub use crate::runtime::packet_sink_registry::{PacketSink as MediaPacketSink, into_packet_sink};
