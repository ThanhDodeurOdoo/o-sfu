use std::collections::BTreeMap;

use crate::signaling::{current_protocol::CurrentServerMessage, shared::SessionId};

use super::{SessionOutbound, state::ActiveSession};

pub(super) fn send_to_all(
    sessions: &BTreeMap<SessionId, ActiveSession>,
    message: &CurrentServerMessage,
) {
    for session in sessions.values() {
        let _ = session
            .sender
            .send(SessionOutbound::Message(message.clone()));
    }
}

pub(super) fn send_to_all_except(
    sessions: &BTreeMap<SessionId, ActiveSession>,
    message: &CurrentServerMessage,
    excluded_session_id: Option<&SessionId>,
) {
    for (session_id, session) in sessions {
        if excluded_session_id.is_some_and(|excluded| excluded == session_id) {
            continue;
        }
        let _ = session
            .sender
            .send(SessionOutbound::Message(message.clone()));
    }
}
