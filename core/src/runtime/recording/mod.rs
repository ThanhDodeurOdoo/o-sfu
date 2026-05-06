#![allow(
    dead_code,
    reason = "recording capture surfaces are live, but deferred finalization and federation work still leave some helpers unused on the main runtime path"
)]

mod media_source;
mod metadata;
mod ortp_format;
mod service;
mod stream_writer;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
mod user;

pub(crate) use media_source::MediaSource;
pub(crate) use ortp_format::OrtpFileHeader;
pub(crate) use service::RecordingService;

#[cfg(test)]
pub(crate) use crate::runtime::packet_sink_registry::RoomPacketSinkRegistry as MediaTap;
pub use crate::runtime::packet_sink_registry::{PacketSink as MediaPacketSink, into_packet_sink};
