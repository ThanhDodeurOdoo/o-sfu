use std::collections::BTreeMap;

use o_sfu_protocol::shared::{JsonPayload, RecordingStateUpdate, UserId, UserInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEventMessage {
    Broadcast {
        sender_id: UserId,
        message: JsonPayload,
    },
    UserJoined {
        user_id: UserId,
        info: UserInfo,
    },
    UserDeparted {
        user_id: UserId,
    },
    UserInfoChanged(BTreeMap<UserId, UserInfo>),
    RecordingStateChanged(RecordingStateUpdate),
}
