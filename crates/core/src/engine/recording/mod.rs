//! ================================================================
//! ===                 WORK IN PROGRESS                     =======
//! === <https://github.com/ThanhDodeurOdoo/o-sfu/issues/20> =======
//! ================================================================
#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod service;
mod user;

pub(crate) use service::RecordingService;

pub use crate::engine::packet_sink_registry::PacketSink as MediaPacketSink;
