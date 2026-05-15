use super::{
    super::{RoomEventMessage, RoomUserPermissions, outbound::MessageFanout},
    shared::RoomState,
};
use crate::runtime::{ConnectionId, RecordingState, RecordingStateUpdate, StopCode, UserId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct RecordingRequestContext {
    permissions: RoomUserPermissions,
    recording_state: RecordingState,
}

impl RecordingRequestContext {
    #[must_use]
    pub const fn permissions(&self) -> RoomUserPermissions {
        self.permissions
    }

    #[must_use]
    pub const fn recording_state(&self) -> &RecordingState {
        &self.recording_state
    }
}

impl RoomState {
    pub fn recording_request_context(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<RecordingRequestContext> {
        let user = self.user_for_connection(user_id, connection_id)?;
        Some(RecordingRequestContext {
            permissions: user.permissions,
            recording_state: self.recording_state.clone(),
        })
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
