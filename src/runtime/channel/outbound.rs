use std::collections::BTreeMap;

use tokio::sync::mpsc;

use crate::signaling::{current_protocol::CurrentServerMessage, shared::SessionId};

use super::{SessionOutbound, state::ActiveSession};

pub(super) type OutboundSender = mpsc::UnboundedSender<SessionOutbound>;

#[derive(Debug, Clone)]
pub(super) struct MessageFanout {
    recipients: Vec<OutboundSender>,
    message: CurrentServerMessage,
}

impl MessageFanout {
    pub(super) fn emit(self) {
        for recipient in self.recipients {
            let _ = recipient.send(SessionOutbound::Message(self.message.clone()));
        }
    }
}

pub(super) fn fanout_all(
    sessions: &BTreeMap<SessionId, ActiveSession>,
    message: &CurrentServerMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: sessions
            .values()
            .map(|session| session.sender.clone())
            .collect(),
        message: message.clone(),
    }
}

pub(super) fn fanout_all_except(
    sessions: &BTreeMap<SessionId, ActiveSession>,
    message: &CurrentServerMessage,
    excluded_session_id: Option<&SessionId>,
) -> MessageFanout {
    MessageFanout {
        recipients: sessions
            .iter()
            .filter(|(session_id, _session)| {
                excluded_session_id.is_none_or(|excluded| excluded != *session_id)
            })
            .map(|(_session_id, session)| session.sender.clone())
            .collect(),
        message: message.clone(),
    }
}

pub(super) fn send_to_all(
    sessions: &BTreeMap<SessionId, ActiveSession>,
    message: &CurrentServerMessage,
) {
    fanout_all(sessions, message).emit();
}
