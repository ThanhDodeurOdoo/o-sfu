use super::{MediaTap, service::RecordingTransitionError};
pub(crate) use super::{
    media_source::into_media_source,
    metadata::{RecordingFileMetadata, RecordingMetadata, RecordingSegment},
    ortp_format::{OrtpCodec, OrtpFrameHeader},
    service::RecordingLifecycleState,
    stream_writer::StreamWriter,
};
use crate::runtime::RoomInstanceId;

#[must_use]
pub(crate) fn is_room_active(media_tap: &MediaTap, room_instance_id: RoomInstanceId) -> bool {
    media_tap.has_active_room(room_instance_id)
}

#[must_use]
pub(crate) fn transition_error_state(error: RecordingTransitionError) -> RecordingLifecycleState {
    error.state
}
