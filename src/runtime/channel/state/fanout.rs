use o_sfu_protocol::shared::SessionId;

use super::{
    super::{
        ChannelEventMessage,
        outbound::{MessageFanout, fanout_all, fanout_all_except},
    },
    shared::ChannelState,
};

impl ChannelState {
    pub(in crate::runtime::channel) fn fanout_all(
        &self,
        message: &ChannelEventMessage,
    ) -> MessageFanout {
        fanout_all(
            self.sessions.values().map(|session| session.sender.clone()),
            message,
        )
    }

    pub(in crate::runtime::channel) fn fanout_all_except(
        &self,
        message: &ChannelEventMessage,
        excluded_session_id: Option<&SessionId>,
    ) -> MessageFanout {
        fanout_all_except(
            self.sessions
                .iter()
                .filter(|(session_id, _session)| {
                    excluded_session_id.is_none_or(|excluded| excluded != *session_id)
                })
                .map(|(_session_id, session)| session.sender.clone()),
            message,
        )
    }
}
