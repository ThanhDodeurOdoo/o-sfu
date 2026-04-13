use crate::signaling::shared::{
    RecordingState, RecordingStateUpdate, SessionId, SessionPermissions, StopCode,
};

use super::super::{ChannelEventMessage, outbound::MessageFanout};
use super::shared::ChannelState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct RecordingRequestContext {
    permissions: SessionPermissions,
    recording_state: RecordingState,
}

impl RecordingRequestContext {
    #[must_use]
    pub(in crate::runtime::channel) const fn permissions(&self) -> &SessionPermissions {
        &self.permissions
    }

    #[must_use]
    pub(in crate::runtime::channel) const fn recording_state(&self) -> &RecordingState {
        &self.recording_state
    }
}

impl ChannelState {
    pub(in crate::runtime::channel) fn recording_request_context(
        &self,
        session_id: &SessionId,
    ) -> Option<RecordingRequestContext> {
        let session = self.sessions.get(session_id)?;
        Some(RecordingRequestContext {
            permissions: session.permissions.clone(),
            recording_state: self.recording_state.clone(),
        })
    }

    pub(in crate::runtime::channel) fn apply_recording_state_update(
        &mut self,
        state: RecordingState,
        stop_code: Option<StopCode>,
    ) -> Option<MessageFanout> {
        if self.recording_state == state && stop_code.is_none() {
            return None;
        }
        self.recording_state = state.clone();
        Some(self.fanout_all(&ChannelEventMessage::RecordingStateChanged(
            RecordingStateUpdate { state, stop_code },
        )))
    }
}
