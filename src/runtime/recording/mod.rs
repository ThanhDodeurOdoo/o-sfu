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
mod tests;

#[cfg(test)]
pub(crate) use media_source::into_media_source;
pub(crate) use media_source::{MediaPacketSink, MediaSource, into_packet_sink};
pub(crate) use media_tap::MediaTap;
#[cfg(test)]
pub(crate) use metadata::{RecordingFileMetadata, RecordingMetadata, RecordingSegment};
pub(crate) use ortp_format::OrtpFileHeader;
#[cfg(test)]
pub(crate) use ortp_format::{OrtpCodec, OrtpFrameHeader};
#[cfg(test)]
pub(crate) use service::RecordingLifecycleState;
pub(crate) use service::{RecordingRouterObserver, RecordingService};
#[cfg(test)]
pub(crate) use stream_writer::StreamWriter;
