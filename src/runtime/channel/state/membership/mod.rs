use std::collections::{BTreeMap, BTreeSet};

use o_sfu_protocol::shared::{SessionId, SessionInfo};
use o_sfu_router::RouterError;
use tracing::{debug, error, warn};

use crate::runtime::ConnectionId;

use super::super::{
    ChannelEventMessage, ChannelJoinError, ChannelSessionPermissions, SessionCloseReason,
    outbound::{MessageFanout, OutboundSender},
    session_negotiation::{SessionNegotiation, SessionNegotiationUpdate},
    topology::ChannelTopology,
};
use super::layout::SessionLayout;
use super::presence::SessionPresence;
use super::shared::{ActiveSession, ChannelState, TransportMediaRemoval};

#[cfg(test)]
mod test_support;

#[derive(Debug)]
pub(in crate::runtime::channel) struct LifecycleEffects {
    pub(in crate::runtime::channel) close_requests: Vec<SessionCloseRequest>,
    pub(in crate::runtime::channel) fanouts: Vec<MessageFanout>,
}

impl LifecycleEffects {
    fn push_fanout(&mut self, fanout: Option<MessageFanout>) {
        if let Some(fanout) = fanout {
            self.fanouts.push(fanout);
        }
    }

    fn push_close_request(&mut self, request: Option<SessionCloseRequest>) {
        if let Some(request) = request {
            self.close_requests.push(request);
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct SessionCloseRequest {
    pub(in crate::runtime::channel) sender: OutboundSender,
    pub(in crate::runtime::channel) reason: SessionCloseReason,
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct JoinSessionOutcome {
    pub(in crate::runtime::channel) connection_id: ConnectionId,
    pub(in crate::runtime::channel) effects: LifecycleEffects,
    pub(in crate::runtime::channel) session_id: SessionId,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct LeaveSessionOutcome {
    pub(in crate::runtime::channel) effects: LifecycleEffects,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
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
    pub(in crate::runtime::channel) disconnected_session_ids: Vec<SessionId>,
    pub(in crate::runtime::channel) effects: LifecycleEffects,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
}

impl ChannelState {
    fn apply_join_topology(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        permissions: ChannelSessionPermissions,
        is_new: bool,
    ) -> Result<(), ChannelJoinError> {
        let mut topology = self.topology.clone();
        if topology
            .apply_client_join(
                session_id,
                connection_id.as_u64(),
                permissions.router_permissions(),
            )
            .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session join into channel router"
            );
            return Err(ChannelJoinError::RouterState);
        }
        if !is_new
            && self
                .reset_existing_session_routing(&mut topology, session_id)
                .is_err()
        {
            error!(
                ?session_id,
                "failed to reset replaced session routing in channel router"
            );
            return Err(ChannelJoinError::RouterState);
        }
        self.topology = topology;
        Ok(())
    }

    fn join_transport_removals(
        &self,
        session_id: &SessionId,
        is_new: bool,
    ) -> Vec<TransportMediaRemoval> {
        if is_new {
            return Vec::new();
        }
        self.collect_session_transport_removals(&BTreeSet::from([session_id.clone()]))
    }

    fn install_joined_session(
        &mut self,
        session_id: &SessionId,
        label: Option<String>,
        permissions: ChannelSessionPermissions,
        sender: OutboundSender,
        connection_id: ConnectionId,
    ) -> Option<OutboundSender> {
        if let Some(session) = self.sessions.get_mut(session_id) {
            let old_sender = session.sender.clone();
            session.label.clone_from(&label);
            session.permissions.clone_from(&permissions);
            session.presence = SessionPresence::default();
            session.layout = SessionLayout::default();
            session.negotiation = SessionNegotiation::default();
            session.parsed_client_rtp_capabilities = None;
            session.connection_id = connection_id;
            session.sender = sender;
            return Some(old_sender);
        }
        self.sessions.insert(
            session_id.clone(),
            ActiveSession {
                label,
                permissions,
                presence: SessionPresence::default(),
                layout: SessionLayout::default(),
                negotiation: SessionNegotiation::default(),
                desired_download_states: BTreeMap::new(),
                parsed_client_rtp_capabilities: None,
                connection_id,
                sender,
            },
        );
        None
    }

    pub(in crate::runtime::channel) fn apply_join(
        &mut self,
        session_id: &SessionId,
        label: Option<String>,
        permissions: impl Into<ChannelSessionPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
    ) -> Result<JoinSessionOutcome, ChannelJoinError> {
        let permissions = permissions.into();
        let is_new = !self.sessions.contains_key(session_id);
        if is_new && self.sessions.len() >= self.admission_policy.max_sessions {
            return Err(ChannelJoinError::ChannelFull);
        }
        let connection_id = ConnectionId::allocate(&mut self.next_connection_id);
        self.apply_join_topology(session_id, connection_id, permissions, is_new)?;
        let transport_removals = self.join_transport_removals(session_id, is_new);

        let previous_sender =
            self.install_joined_session(session_id, label, permissions, sender, connection_id);
        let had_previous_sender = previous_sender.is_some();
        if had_previous_sender {
            self.purge_session_media_state(session_id);
        }

        let mut effects = LifecycleEffects {
            close_requests: Vec::new(),
            fanouts: Vec::new(),
        };
        effects.push_close_request(previous_sender.map(|sender| SessionCloseRequest {
            sender,
            reason: SessionCloseReason::Replaced,
        }));
        effects.push_fanout(had_previous_sender.then(|| {
            self.fanout_all_except(
                &ChannelEventMessage::SessionDeparted {
                    session_id: session_id.clone(),
                },
                Some(session_id),
            )
        }));
        effects.push_fanout(if emit_joined_fanout {
            self.session_info_snapshot(session_id)
                .map(|(joined_session_id, info)| {
                    self.fanout_all_except(
                        &ChannelEventMessage::SessionJoined {
                            session_id: joined_session_id,
                            info,
                        },
                        Some(session_id),
                    )
                })
        } else {
            None
        });
        Ok(JoinSessionOutcome {
            connection_id,
            effects,
            session_id: session_id.clone(),
            transport_removals,
        })
    }

    fn reset_existing_session_routing(
        &self,
        topology: &mut ChannelTopology,
        session_id: &SessionId,
    ) -> Result<(), RouterError> {
        let routed_consumers = self
            .consumer_index
            .iter()
            .filter_map(|(key, consumer_state)| {
                (key.consumer_session_id == *session_id)
                    .then_some(consumer_state.routed_consumer_id)
            })
            .collect::<Vec<_>>();
        for routed_consumer_id in routed_consumers {
            topology.remove_consumer(routed_consumer_id)?;
        }

        let routed_producers = self
            .producers
            .values()
            .filter_map(|producer| {
                (producer.owner_session_id == *session_id).then_some(producer.routed_producer_id)
            })
            .collect::<Vec<_>>();
        for routed_producer_id in routed_producers {
            topology.remove_producer(routed_producer_id)?;
        }
        Ok(())
    }

    pub(in crate::runtime::channel) fn apply_leave(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> Option<LeaveSessionOutcome> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        let transport_removals =
            self.collect_session_transport_removals(&BTreeSet::from([session_id.clone()]));
        if self.topology.apply_client_leave(session_id).is_err() {
            error!(
                ?session_id,
                "failed to remove departed session from channel router"
            );
            return None;
        }
        self.purge_session_media_state(session_id);
        let session = self.sessions.remove(session_id)?;
        let mut effects = LifecycleEffects {
            close_requests: Vec::new(),
            fanouts: Vec::new(),
        };
        effects.push_fanout(Some(self.fanout_all(
            &ChannelEventMessage::SessionDeparted {
                session_id: session_id.clone(),
            },
        )));
        effects.push_close_request(Some(SessionCloseRequest {
            sender: session.sender,
            reason: SessionCloseReason::RemovedByRuntime,
        }));
        Some(LeaveSessionOutcome {
            effects,
            transport_removals,
        })
    }

    pub(in crate::runtime::channel) fn apply_presence_update(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        info: &SessionInfo,
        need_refresh: bool,
    ) -> Option<SessionInfoUpdateOutcome> {
        let Some(current_session) = self.sessions.get(session_id) else {
            warn!(
                ?session_id,
                connection_id = ?connection_id,
                ?info,
                need_refresh,
                "discarding session presence update because the session is missing"
            );
            return None;
        };
        if current_session.connection_id != connection_id {
            warn!(
                ?session_id,
                connection_id = ?connection_id,
                current_connection_id = ?current_session.connection_id,
                ?info,
                need_refresh,
                "discarding session presence update because the connection is stale"
            );
            return None;
        }
        {
            let session = self.session_mut_for_connection(session_id, connection_id)?;
            session.presence.apply_update(info);
        }
        let snapshot = if need_refresh {
            self.session_info_snapshot_all()
        } else {
            BTreeMap::from([self.session_info_snapshot(session_id)?])
        };
        debug!(
            ?session_id,
            connection_id = ?connection_id,
            ?info,
            need_refresh,
            snapshot_len = snapshot.len(),
            "applied session presence update and staged session info fanout"
        );
        Some(SessionInfoUpdateOutcome {
            fanout: self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)),
        })
    }

    pub(in crate::runtime::channel) fn set_session_negotiated(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        capabilities: &o_sfu_router::MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities = Some(capabilities.clone());
        session.negotiation.set_session_negotiated()
    }

    pub(in crate::runtime::channel) fn apply_disconnect_sessions(
        &mut self,
        session_ids: &[SessionId],
    ) -> DisconnectSessionsOutcome {
        let mut transport_removals = Vec::new();
        let mut close_requests = Vec::new();
        let mut disconnected_session_ids = Vec::new();
        let mut fanouts = Vec::new();
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
            transport_removals.extend(
                self.collect_session_transport_removals(&BTreeSet::from([session_id.clone()])),
            );
            if let Some(session) = self.sessions.remove(session_id) {
                self.purge_session_media_state(session_id);
                disconnected_session_ids.push(session_id.clone());
                close_requests.push(SessionCloseRequest {
                    sender: session.sender,
                    reason: SessionCloseReason::RemovedByRuntime,
                });
                fanouts.push(self.fanout_all(&ChannelEventMessage::SessionDeparted {
                    session_id: session_id.clone(),
                }));
            }
        }
        DisconnectSessionsOutcome {
            disconnected_session_ids,
            effects: LifecycleEffects {
                close_requests,
                fanouts,
            },
            transport_removals,
        }
    }

    pub(in crate::runtime::channel) fn broadcast_fanout(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) -> Option<MessageFanout> {
        self.session_for_connection(session_id, connection_id)?;
        Some(self.fanout_all_except(
            &ChannelEventMessage::Broadcast {
                sender_id: session_id.clone(),
                message,
            },
            Some(session_id),
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        reason = "state-level test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
    )]

    use std::sync::Arc;

    use o_sfu_router::{ConsumerId, MediaStream, ProducerId, RouterId};
    use tokio::sync::mpsc;

    use super::*;
    use crate::config::MediaCodecFlags;
    use crate::runtime::channel::{
        ChannelAdmissionPolicy,
        rtp_capabilities::router_rtp_capabilities,
        state::{ids::ProducerRuntimeId, shared::ConsumerKey, shared::ConsumerState},
        topology::{RoutedConsumerId, RoutedProducerId},
    };
    use crate::runtime::metrics::RuntimeMetrics;
    use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
    use crate::runtime::transport_adapter::TransportMediaId;
    use crate::runtime::{ChannelInstanceId, ConnectionId};
    use o_sfu_protocol::shared::{SessionPermissions, StreamType};
    use o_sfu_router::MediaKind;

    fn test_state() -> ChannelState {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        ChannelState::new(
            RouterId(1),
            ChannelAdmissionPolicy::new(4),
            router_rtp_capabilities(MediaCodecFlags::default()),
            Arc::new(RecordingService::new(
                ChannelInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            )),
        )
    }

    fn install_test_published_producer(
        state: &mut ChannelState,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        routed_producer_id: RoutedProducerId,
        transport_media_id: Option<TransportMediaId>,
    ) -> ProducerRuntimeId {
        let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
        state.producer_ids_by_owner_stream.insert(
            super::super::shared::ProducerKey::new(session_id, stream_type),
            producer_id,
        );
        state.producers.insert(
            producer_id,
            super::super::shared::PublishedProducer {
                owner_session_id: session_id.clone(),
                owner_connection_id: connection_id,
                stream_type,
                media_kind: MediaKind::Video,
                consumable_rtp_parameters: MediaStream::new(vec![], vec![], vec![]),
                routed_producer_id,
                transport_media_id,
                source_packet_selection: None,
                active: true,
            },
        );
        producer_id
    }

    #[test]
    fn disconnect_sessions_removes_current_members_and_fanouts_departures() {
        let mut state = test_state();
        let (sender_a, _receiver_a) = mpsc::unbounded_channel();
        let (sender_b, _receiver_b) = mpsc::unbounded_channel();
        assert!(
            state
                .apply_join(
                    &SessionId::Integer(1),
                    None,
                    SessionPermissions::default(),
                    sender_a,
                    false,
                )
                .is_ok()
        );
        assert!(
            state
                .apply_join(
                    &SessionId::Integer(2),
                    None,
                    SessionPermissions::default(),
                    sender_b,
                    false,
                )
                .is_ok()
        );

        let outcome =
            state.apply_disconnect_sessions(&[SessionId::Integer(1), SessionId::Integer(2)]);

        assert_eq!(state.sessions.len(), 0);
        assert_eq!(outcome.effects.close_requests.len(), 2);
        assert_eq!(outcome.effects.fanouts.len(), 2);
    }

    #[test]
    fn leave_removes_consumer_routes_for_departed_session() {
        let mut state = test_state();
        let (producer_sender, _producer_receiver) = mpsc::unbounded_channel();
        let (consumer_sender, _consumer_receiver) = mpsc::unbounded_channel();
        assert!(
            state
                .apply_join(
                    &SessionId::Integer(1),
                    None,
                    SessionPermissions::default(),
                    producer_sender,
                    false,
                )
                .is_ok()
        );
        assert!(
            state
                .apply_join(
                    &SessionId::Integer(2),
                    None,
                    SessionPermissions::default(),
                    consumer_sender,
                    false,
                )
                .is_ok()
        );
        let producer_connection_id = state
            .session_connection_id(&SessionId::Integer(1))
            .expect("producer session should exist");
        let consumer_connection_id = state
            .session_connection_id(&SessionId::Integer(2))
            .expect("consumer session should exist");
        let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(10));
        let producer_id = install_test_published_producer(
            &mut state,
            &SessionId::Integer(1),
            producer_connection_id,
            StreamType::Camera,
            routed_producer_id,
            Some(TransportMediaId::new(11)),
        );
        state.consumer_index.insert(
            ConsumerKey {
                consumer_session_id: SessionId::Integer(2),
                producer_session_id: SessionId::Integer(1),
                stream_type: StreamType::Camera,
            },
            ConsumerState {
                routed_consumer_id: RoutedConsumerId::new(RouterId(1), ConsumerId(20)),
                consumer_connection_id,
                source_connection_id: producer_connection_id,
                source_media: TransportMediaId::new(11),
                consumer_media: TransportMediaId::new(21),
            },
        );

        let outcome = state.apply_leave(&SessionId::Integer(2), consumer_connection_id);

        assert!(outcome.is_some());
        assert_eq!(state.consumer_index.len(), 0);
        assert_eq!(state.producers.len(), 1);
        assert!(state.producers.contains_key(&producer_id));
    }

    #[test]
    fn stale_connection_cannot_broadcast() {
        let mut state = test_state();
        let (sender, _receiver) = mpsc::unbounded_channel();
        assert!(
            state
                .apply_join(
                    &SessionId::Integer(1),
                    None,
                    SessionPermissions::default(),
                    sender,
                    false,
                )
                .is_ok()
        );

        let fanout = state.broadcast_fanout(
            &SessionId::Integer(1),
            ConnectionId::from_raw(999),
            serde_json::Value::String(String::from("hello")),
        );

        assert!(fanout.is_none());
    }

    #[test]
    fn presence_update_returns_none_for_stale_connection() {
        let mut state = test_state();
        let (sender, _receiver) = mpsc::unbounded_channel();
        assert!(
            state
                .apply_join(
                    &SessionId::Integer(1),
                    None,
                    SessionPermissions::default(),
                    sender,
                    false,
                )
                .is_ok()
        );

        let outcome = state.apply_presence_update(
            &SessionId::Integer(1),
            ConnectionId::from_raw(999),
            &SessionInfo::default(),
            false,
        );

        assert!(outcome.is_none());
    }

    #[test]
    fn disconnect_sessions_ignores_missing_members() {
        let mut state = test_state();
        let outcome = state.apply_disconnect_sessions(&[SessionId::Integer(1)]);

        assert!(outcome.transport_removals.is_empty());
        assert!(outcome.effects.close_requests.is_empty());
        assert!(outcome.effects.fanouts.is_empty());
    }
}
