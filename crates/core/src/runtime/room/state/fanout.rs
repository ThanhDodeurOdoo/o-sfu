use super::{
    super::{
        RoomEventMessage,
        outbound::{MessageFanout, fanout_all},
    },
    shared::RoomState,
};
use crate::runtime::UserId;

impl RoomState {
    pub fn fanout_all(&self, message: &RoomEventMessage) -> MessageFanout {
        fanout_all(self.users.values().map(|user| user.sender.clone()), message)
    }

    pub fn fanout_all_except(
        &self,
        message: &RoomEventMessage,
        excluded_user_id: Option<&UserId>,
    ) -> MessageFanout {
        fanout_all(
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
