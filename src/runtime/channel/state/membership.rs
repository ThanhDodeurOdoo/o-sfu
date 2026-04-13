use std::collections::{BTreeMap, BTreeSet};

use tracing::error;

use crate::runtime::transport_adapter::TransportConnectDirection;
use crate::signaling::{
    current_protocol::CurrentServerMessage,
    current_protocol::CurrentSessionDeparturePayload,
    ortc_mapper,
    protocol::WebSocketCloseCode,
    shared::{SessionId, SessionInfo, SessionPermissions},
    webrtc::RtpCapabilities as SignalingRtpCapabilities,
};

use super::super::{
    ChannelJoinError,
    outbound::{MessageFanout, OutboundSender},
    session_negotiation::{SessionNegotiation, SessionNegotiationUpdate},
};
use super::presence::SessionPresence;
use super::shared::{ActiveSession, ChannelState, TransportMediaRemoval};

#[derive(Debug)]
pub(in crate::runtime::channel) struct JoinSessionOutcome {
    pub(in crate::runtime::channel) connection_id: u64,
    replaced_sender: Option<OutboundSender>,
    departure_fanout: Option<MessageFanout>,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
}

impl JoinSessionOutcome {
    pub(in crate::runtime::channel) fn emit(self) {
        if let Some(sender) = self.replaced_sender {
            let _ = sender.send(super::super::SessionOutbound::Close(
                WebSocketCloseCode::Kicked,
            ));
        }
        if let Some(fanout) = self.departure_fanout {
            fanout.emit();
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct LeaveSessionOutcome {
    departure_fanout: MessageFanout,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
}

impl LeaveSessionOutcome {
    pub(in crate::runtime::channel) fn emit(self) {
        self.departure_fanout.emit();
    }
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct SessionInfoUpdateOutcome {
    fanout: MessageFanout,
}

impl SessionInfoUpdateOutcome {
    pub(in crate::runtime::channel) fn emit(self) {
        self.fanout.emit();
    }
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct DisconnectSessionsOutcome {
    kicked_senders: Vec<OutboundSender>,
    departure_fanouts: Vec<MessageFanout>,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
}

impl DisconnectSessionsOutcome {
    pub(in crate::runtime::channel) fn emit(self) {
        for sender in self.kicked_senders {
            let _ = sender.send(super::super::SessionOutbound::Close(
                WebSocketCloseCode::Kicked,
            ));
        }
        for fanout in self.departure_fanouts {
            fanout.emit();
        }
    }
}

impl ChannelState {
    pub(in crate::runtime::channel) fn apply_join(
        &mut self,
        session_id: &SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: OutboundSender,
    ) -> Result<JoinSessionOutcome, ChannelJoinError> {
        let is_new = !self.sessions.contains_key(session_id);
        if is_new && self.sessions.len() >= self.admission_policy.max_sessions {
            return Err(ChannelJoinError::ChannelFull);
        }
        let transport_removals = if is_new {
            Vec::new()
        } else {
            self.collect_consumer_transport_removals(&BTreeSet::from([session_id.clone()]))
        };
        let connection_id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        if !is_new {
            if self.topology.apply_client_leave(session_id).is_err() {
                error!(
                    ?session_id,
                    "failed to reset replaced session in channel router"
                );
                return Err(ChannelJoinError::RouterState);
            }
            self.purge_session_media_state(session_id);
        }
        if self
            .topology
            .apply_client_join(session_id, connection_id, &permissions)
            .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session join into channel router"
            );
            return Err(ChannelJoinError::RouterState);
        }

        let previous_sender = if let Some(session) = self.sessions.get_mut(session_id) {
            let old_sender = session.sender.clone();
            session.label.clone_from(&label);
            session.permissions.clone_from(&permissions);
            session.presence = SessionPresence::default();
            session.negotiation = SessionNegotiation::default();
            session.parsed_client_rtp_capabilities = None;
            session.connection_id = connection_id;
            session.sender = sender;
            Some(old_sender)
        } else {
            self.sessions.insert(
                session_id.clone(),
                ActiveSession {
                    label,
                    permissions,
                    presence: SessionPresence::default(),
                    negotiation: SessionNegotiation::default(),
                    parsed_client_rtp_capabilities: None,
                    connection_id,
                    sender,
                },
            );
            None
        };

        let departure_fanout = previous_sender.as_ref().map(|_| {
            self.fanout_all_except(
                &CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                    session_id: session_id.clone(),
                }),
                Some(session_id),
            )
        });
        Ok(JoinSessionOutcome {
            connection_id,
            replaced_sender: previous_sender,
            departure_fanout,
            transport_removals,
        })
    }

    pub(in crate::runtime::channel) fn apply_leave(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> Option<LeaveSessionOutcome> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        let transport_removals =
            self.collect_consumer_transport_removals(&BTreeSet::from([session_id.clone()]));
        if self.topology.apply_client_leave(session_id).is_err() {
            error!(
                ?session_id,
                "failed to mirror session leave into channel router"
            );
            return None;
        }
        self.sessions.remove(session_id);
        self.purge_session_media_state(session_id);
        Some(LeaveSessionOutcome {
            departure_fanout: self.fanout_all(&CurrentServerMessage::SessionDeparted(
                CurrentSessionDeparturePayload {
                    session_id: session_id.clone(),
                },
            )),
            transport_removals,
        })
    }

    pub(in crate::runtime::channel) fn apply_presence_update(
        &mut self,
        session_id: &SessionId,
        info: &SessionInfo,
        need_refresh: bool,
    ) -> Option<SessionInfoUpdateOutcome> {
        {
            let session = self.sessions.get_mut(session_id)?;
            session.presence.apply_update(info);
        }
        let snapshot = if need_refresh {
            self.session_info_snapshot_all()
        } else {
            BTreeMap::from([self.session_info_snapshot(session_id)?])
        };
        Some(SessionInfoUpdateOutcome {
            fanout: self.fanout_all(&CurrentServerMessage::SessionInfoChanged(snapshot)),
        })
    }

    pub(in crate::runtime::channel) fn apply_disconnect_sessions(
        &mut self,
        session_ids: &[SessionId],
    ) -> DisconnectSessionsOutcome {
        let departing_session_ids = session_ids.iter().cloned().collect::<BTreeSet<_>>();
        let transport_removals = self.collect_consumer_transport_removals(&departing_session_ids);
        let mut kicked_senders = Vec::new();
        let mut departed = Vec::new();
        for session_id in session_ids {
            if !self.sessions.contains_key(session_id) {
                continue;
            }
            if self.topology.apply_client_leave(session_id).is_err() {
                error!(
                    ?session_id,
                    "failed to mirror bulk disconnect into channel router"
                );
                continue;
            }
            if let Some(session) = self.sessions.remove(session_id) {
                self.purge_session_media_state(session_id);
                kicked_senders.push(session.sender);
                departed.push(session_id.clone());
            }
        }
        let departure_fanouts = departed
            .into_iter()
            .map(|departed_id| {
                self.fanout_all(&CurrentServerMessage::SessionDeparted(
                    CurrentSessionDeparturePayload {
                        session_id: departed_id,
                    },
                ))
            })
            .collect();
        DisconnectSessionsOutcome {
            kicked_senders,
            departure_fanouts,
            transport_removals,
        }
    }

    pub(in crate::runtime::channel) fn set_client_rtp_capabilities(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: SignalingRtpCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities =
            ortc_mapper::parse_rtp_capabilities(&capabilities.0);
        session
            .negotiation
            .set_client_rtp_capabilities(capabilities)
    }

    pub(in crate::runtime::channel) fn set_transport_connected(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        direction: TransportConnectDirection,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.negotiation.set_transport_connected(direction)
    }

    pub(in crate::runtime::channel) fn set_session_negotiated(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: SignalingRtpCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities =
            ortc_mapper::parse_rtp_capabilities(&capabilities.0);
        session.negotiation.set_session_negotiated(capabilities)
    }
}
