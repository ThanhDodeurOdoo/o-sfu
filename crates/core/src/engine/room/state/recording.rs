use super::{
    super::{RoomEventMessage, RoomUserPermissions, outbound::MessageFanout},
    shared::RoomState,
};
use crate::engine::{ConnectionId, RecordingState, RecordingStateUpdate, StopCode, UserId};

impl RoomState {
    pub fn recording_request_context(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<(RoomUserPermissions, RecordingState)> {
        let user = self.user_for_connection(user_id, connection_id)?;
        Some((user.permissions, self.recording_state.clone()))
    }

    pub fn apply_recording_state_update(
        &mut self,
        state: RecordingState,
        stop_code: Option<StopCode>,
    ) -> Option<MessageFanout> {
        if self.recording_state == state && stop_code.is_none() {
            return None;
        }
        self.recording_state = state.clone();
        Some(self.fanout_all(&RoomEventMessage::RecordingStateChanged(
            RecordingStateUpdate { state, stop_code },
        )))
    }
}
