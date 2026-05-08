use super::service::RecordingTransitionError;
pub(crate) use super::{
    metadata::{RecordingFileMetadata, RecordingMetadata, RecordingSegment},
    ortp_format::{OrtpCodec, OrtpFrameHeader},
    service::RecordingLifecycleState,
    stream_writer::StreamWriter,
};
use crate::runtime::{RoomInstanceId, packet_sink_registry::RoomPacketSinkRegistry};

#[must_use]
pub(crate) fn is_room_active(
    packet_sink_registry: &RoomPacketSinkRegistry,
    room_instance_id: RoomInstanceId,
) -> bool {
    packet_sink_registry.has_active_room(room_instance_id)
}

#[must_use]
pub(crate) fn transition_error_state(error: RecordingTransitionError) -> RecordingLifecycleState {
    error.state
}
