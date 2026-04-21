use o_sfu_protocol::shared::{RecordingState, RecordingStateUpdate, SessionId, StopCode};

use super::super::{ChannelEventMessage, ChannelSessionPermissions, outbound::MessageFanout};
use super::shared::ChannelState;
use crate::runtime::ConnectionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct RecordingRequestContext {
    permissions: ChannelSessionPermissions,
    recording_state: RecordingState,
}

impl RecordingRequestContext {
    #[must_use]
    pub(in crate::runtime::channel) const fn permissions(&self) -> ChannelSessionPermissions {
        self.permissions
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
        connection_id: ConnectionId,
    ) -> Option<RecordingRequestContext> {
        let session = self.session_for_connection(session_id, connection_id)?;
        Some(RecordingRequestContext {
            permissions: session.permissions,
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
