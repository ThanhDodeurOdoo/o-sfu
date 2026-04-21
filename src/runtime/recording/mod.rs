// TODO: needs documentation:
#![allow(
    dead_code,
    reason = "recording capture surfaces are live, but deferred finalization and federation work still leave some helpers unused on the main runtime path"
)]

mod media_source;
mod media_tap;
mod metadata;
mod ortp_format;
mod service;
mod session;
mod stream_writer;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use media_source::{MediaPacketSink, MediaSource, into_packet_sink};
pub use media_tap::ActiveChannelRegistry;
pub(crate) use media_tap::MediaTap;
pub(crate) use ortp_format::OrtpFileHeader;
pub(crate) use service::{RecordingRouterObserver, RecordingService};
