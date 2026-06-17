use super::Room;
use crate::engine::{ConnectionId, RecordingOptions, UserId};

impl Room {
    pub(crate) fn apply_recording_start(
        &self,
        _user_id: &UserId,
        _connection_id: ConnectionId,
        _options: RecordingOptions,
    ) -> bool {
        self.reject_recording_start()
    }

    pub(crate) fn apply_recording_stop(
        &self,
        _user_id: &UserId,
        _connection_id: ConnectionId,
    ) -> bool {
        self.reject_recording_stop()
    }

    fn reject_recording_start(&self) -> bool {
        self.metrics.record_recording_start_rejected();
        false
    }

    fn reject_recording_stop(&self) -> bool {
        self.metrics.record_recording_stop_rejected();
        false
    }
}
