use std::collections::BTreeMap;

use crate::signaling::{
    bundle_api::bundle_session_info_key,
    current_protocol::{
        CurrentBroadcastPayload, CurrentServerMessage, CurrentSessionDeparturePayload,
        CurrentSessionInfoSnapshotById,
    },
    shared::{JsonPayload, RecordingStateUpdate, SessionId, SessionInfo},
};

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

impl ChannelEventMessage {
    #[must_use]
    pub(crate) fn into_current_server_message(self) -> Option<CurrentServerMessage> {
        match self {
            Self::Broadcast { sender_id, message } => {
                Some(CurrentServerMessage::Broadcast(CurrentBroadcastPayload {
                    sender_id,
                    message,
                }))
            }
            Self::SessionJoined { .. } => None,
            Self::SessionDeparted { session_id } => Some(CurrentServerMessage::SessionDeparted(
                CurrentSessionDeparturePayload { session_id },
            )),
            Self::SessionInfoChanged(snapshot) => Some(CurrentServerMessage::SessionInfoChanged(
                into_legacy_session_info_snapshot(snapshot),
            )),
            Self::RecordingStateChanged(state) => {
                Some(CurrentServerMessage::ChannelStateChanged(state))
            }
        }
    }
}

fn into_legacy_session_info_snapshot(
    snapshot: BTreeMap<SessionId, SessionInfo>,
) -> CurrentSessionInfoSnapshotById {
    snapshot
        .into_iter()
        .map(|(session_id, info)| (bundle_session_info_key(&session_id), info))
        .collect()
}
