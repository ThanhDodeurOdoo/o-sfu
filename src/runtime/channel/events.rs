use std::collections::BTreeMap;

use o_sfu_protocol::shared::{JsonPayload, RecordingStateUpdate, SessionId, SessionInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChannelEventMessage {
    Broadcast {
        sender_id: SessionId,
        message: JsonPayload,
    },
    SessionJoined {
        session_id: SessionId,
        info: SessionInfo,
    },
    SessionDeparted {
        session_id: SessionId,
    },
    SessionInfoChanged(BTreeMap<SessionId, SessionInfo>),
    RecordingStateChanged(RecordingStateUpdate),
}
