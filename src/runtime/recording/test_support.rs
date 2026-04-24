use super::{MediaTap, service::RecordingTransitionError};
pub(crate) use super::{
    media_source::into_media_source,
    metadata::{RecordingFileMetadata, RecordingMetadata, RecordingSegment},
    ortp_format::{OrtpCodec, OrtpFrameHeader},
    service::RecordingLifecycleState,
    stream_writer::StreamWriter,
};
use crate::runtime::ChannelInstanceId;

#[must_use]
pub(crate) fn is_channel_active(
    media_tap: &MediaTap,
    channel_instance_id: ChannelInstanceId,
) -> bool {
    media_tap.has_active_channel(channel_instance_id)
}

#[must_use]
pub(crate) fn transition_error_state(error: RecordingTransitionError) -> RecordingLifecycleState {
    error.state
}
