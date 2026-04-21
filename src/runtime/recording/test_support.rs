pub(crate) use super::media_source::into_media_source;
pub(crate) use super::metadata::{RecordingFileMetadata, RecordingMetadata, RecordingSegment};
pub(crate) use super::ortp_format::{OrtpCodec, OrtpFrameHeader};
pub(crate) use super::service::RecordingLifecycleState;
pub(crate) use super::stream_writer::StreamWriter;
use super::{MediaTap, service::RecordingTransitionError};
use crate::runtime::ChannelRuntimeId;

#[must_use]
pub(crate) fn is_channel_active(
    media_tap: &MediaTap,
    channel_runtime_id: ChannelRuntimeId,
) -> bool {
    media_tap.has_active_channel(channel_runtime_id)
}

#[must_use]
pub(crate) fn transition_error_state(error: RecordingTransitionError) -> RecordingLifecycleState {
    error.state
}
