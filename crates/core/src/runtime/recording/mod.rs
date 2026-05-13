mod metadata;
mod ortp_format;
mod service;
mod stream_writer;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
mod user;

pub(crate) use ortp_format::OrtpFileHeader;
pub(crate) use service::RecordingService;

pub use crate::runtime::packet_sink_registry::{PacketSink as MediaPacketSink, into_packet_sink};
