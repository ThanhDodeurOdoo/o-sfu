use super::{
    super::{
        RoomEventMessage,
        outbound::{MessageFanout, fanout_all, fanout_all_except},
    },
    shared::RoomState,
};
use crate::runtime::UserId;

impl RoomState {
    pub(in crate::runtime::room) fn fanout_all(&self, message: &RoomEventMessage) -> MessageFanout {
        fanout_all(self.users.values().map(|user| user.sender.clone()), message)
    }

    pub(in crate::runtime::room) fn fanout_all_except(
        &self,
        message: &RoomEventMessage,
        excluded_user_id: Option<&UserId>,
    ) -> MessageFanout {
        fanout_all_except(
            self.users
                .iter()
                .filter(|(user_id, _session)| {
                    excluded_user_id.is_none_or(|excluded| excluded != *user_id)
                })
                .map(|(_user_id, user)| user.sender.clone()),
            message,
        )
    }
}
