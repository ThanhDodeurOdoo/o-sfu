#![allow(
    dead_code,
    reason = "The recording step will be finalize later, just making the skeleton of the API so it's easier to design with it already in place"
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
pub(crate) use media_source::{MediaFrameSink, MediaSource, into_frame_sink};
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
