use std::collections::{BTreeMap, BTreeSet};

use o_sfu_protocol::shared::{SessionId, SessionInfo};
use o_sfu_router::{MediaCapabilities, RouterError};
use tracing::{debug, error, warn};

#[cfg(test)]
use super::super::session_negotiation::SessionTransportReady;
use super::super::{
    ChannelEventMessage, ChannelJoinError, ChannelSessionPermissions, SessionCloseReason,
    outbound::{MessageFanout, OutboundSender},
    session_negotiation::{SessionNegotiation, SessionNegotiationUpdate},
    topology::ChannelTopology,
};
use super::layout::SessionLayout;
use super::presence::SessionPresence;
use super::shared::{ActiveSession, ChannelState, TransportMediaRemoval};

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
    pub(in crate::runtime::channel) connection_id: u64,
    pub(in crate::runtime::channel) effects: LifecycleEffects,
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
    pub(in crate::runtime::channel) effects: LifecycleEffects,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
}

impl ChannelState {
    fn apply_join_topology(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        permissions: ChannelSessionPermissions,
        is_new: bool,
    ) -> Result<(), ChannelJoinError> {
        let mut topology = self.topology.clone();
        if topology
            .apply_client_join(session_id, connection_id, permissions.router_permissions())
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
        connection_id: u64,
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
        let connection_id = self.next_connection_id;
        self.apply_join_topology(session_id, connection_id, permissions, is_new)?;
        let transport_removals = self.join_transport_removals(session_id, is_new);
        self.next_connection_id = self.next_connection_id.saturating_add(1);

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
        connection_id: u64,
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
                "failed to mirror session leave into channel router"
            );
            return None;
        }
        self.sessions.remove(session_id);
        self.purge_session_media_state(session_id);
        Some(LeaveSessionOutcome {
            effects: LifecycleEffects {
                close_requests: Vec::new(),
                fanouts: vec![self.fanout_all(&ChannelEventMessage::SessionDeparted {
                    session_id: session_id.clone(),
                })],
            },
            transport_removals,
        })
    }

    pub(in crate::runtime::channel) fn apply_presence_update(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        info: &SessionInfo,
        need_refresh: bool,
    ) -> Option<SessionInfoUpdateOutcome> {
        let Some(current_session) = self.sessions.get(session_id) else {
            warn!(
                ?session_id,
                connection_id,
                ?info,
                need_refresh,
                "discarding session presence update because the session is missing"
            );
            return None;
        };
        if current_session.connection_id != connection_id {
            warn!(
                ?session_id,
                connection_id,
                current_connection_id = current_session.connection_id,
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
            connection_id,
            ?info,
            need_refresh,
            snapshot_len = snapshot.len(),
            "applied session presence update and staged session info fanout"
        );
        Some(SessionInfoUpdateOutcome {
            fanout: self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)),
        })
    }

    pub(in crate::runtime::channel) fn apply_disconnect_sessions(
        &mut self,
        session_ids: &[SessionId],
    ) -> DisconnectSessionsOutcome {
        let mut transport_removals = Vec::new();
        let mut close_requests = Vec::new();
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
        connection_id: u64,
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

    // TODO: CLEANUP TESTING
    #[cfg(test)]
    pub(in crate::runtime::channel) fn set_client_rtp_capabilities(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: &MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities = Some(capabilities.clone());
        session.negotiation.set_client_rtp_capabilities()
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn set_transport_ready(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        readiness: SessionTransportReady,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.negotiation.set_transport_ready(readiness)
    }

    pub(in crate::runtime::channel) fn set_session_negotiated(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: &MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities = Some(capabilities.clone());
        session.negotiation.set_session_negotiated()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use o_sfu_router::{ConsumerId, ProducerId, RouterId, RtpParameters};
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
    use o_sfu_protocol::shared::{DownloadStates, SessionPermissions, StreamType};
    use o_sfu_router::MediaKind;

    fn test_state() -> ChannelState {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        ChannelState::new(
            RouterId(1),
            ChannelAdmissionPolicy::new(4),
            router_rtp_capabilities(MediaCodecFlags::default()),
            Arc::new(RecordingService::new(
                0,
                media_source,
                Arc::new(RuntimeMetrics::default()),
            )),
        )
    }

    fn install_test_published_producer(
        state: &mut ChannelState,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
        routed_producer_id: RoutedProducerId,
        transport_media_id: Option<TransportMediaId>,
    ) {
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
                consumable_rtp_parameters: RtpParameters::new(vec![], vec![], vec![]),
                routed_producer_id,
                transport_media_id,
                source_packet_selection: None,
                active: true,
            },
        );
        if let Some(transport_media_id) = transport_media_id {
            state
                .producer_stream_types_by_transport_media_id
                .insert(transport_media_id, stream_type);
        }
    }

    #[test]
    fn replacement_join_keeps_existing_channel_state_when_router_reset_fails() {
        let mut state = test_state();
        let session_id = SessionId::Integer(1);
        let (first_sender, _first_rx) = mpsc::unbounded_channel();
        let (replacement_sender, _replacement_rx) = mpsc::unbounded_channel();
        let initial_permissions = SessionPermissions {
            video_recording: Some(true),
            ..SessionPermissions::default()
        };

        let first_join =
            state.apply_join(&session_id, None, initial_permissions, first_sender, false);
        assert!(first_join.is_ok());
        let original_connection_id = state.session_connection_id(&session_id);

        let valid_routed_producer_id_result = state.topology.add_producer(
            &session_id,
            MediaKind::Video,
            o_sfu_router::StreamType::Camera,
        );
        assert!(
            valid_routed_producer_id_result.is_ok(),
            "valid producer route should be created: {valid_routed_producer_id_result:?}"
        );
        let Ok(valid_routed_producer_id) = valid_routed_producer_id_result else {
            return;
        };
        install_test_published_producer(
            &mut state,
            &session_id,
            original_connection_id.unwrap_or(u64::MAX),
            StreamType::Camera,
            valid_routed_producer_id,
            Some(TransportMediaId::new(1)),
        );
        install_test_published_producer(
            &mut state,
            &session_id,
            original_connection_id.unwrap_or(u64::MAX),
            StreamType::Screen,
            RoutedProducerId::new(RouterId(1), ProducerId(999)),
            None,
        );

        let replacement = state.apply_join(
            &session_id,
            Some(String::from("replacement")),
            SessionPermissions {
                transcription: Some(true),
                ..SessionPermissions::default()
            },
            replacement_sender,
            false,
        );
        assert!(matches!(replacement, Err(ChannelJoinError::RouterState)));
        assert_eq!(
            state.session_connection_id(&session_id),
            original_connection_id
        );
        assert_eq!(
            state.session_permissions(&session_id),
            Some(o_sfu_router::SessionPermissions::from_flags(
                o_sfu_router::SessionPermissionFlags {
                    transcription: false,
                    audio_recording: false,
                    video_recording: true,
                },
            ))
        );
        assert_eq!(state.producer_count(), 2);
        assert!(
            state
                .producer_route_target_for_session(&session_id, StreamType::Camera)
                .is_some(),
            "the valid staged producer should remain installed when replacement fails"
        );
        assert!(
            state
                .producer_route_target_for_session(&session_id, StreamType::Screen)
                .is_none(),
            "the invalid staged producer should stay untouched when replacement fails"
        );
        assert!(state.has_session(&session_id));
        assert_eq!(state.topology.session_count(), 1);
    }

    #[test]
    fn bulk_disconnect_ignores_missing_sessions_when_collecting_transport_removals() {
        let mut state = test_state();
        let producer_session_id = SessionId::Integer(1);
        let consumer_session_id = SessionId::Integer(2);
        let missing_session_id = SessionId::Integer(999);
        let (producer_sender, _producer_rx) = mpsc::unbounded_channel();
        let (consumer_sender, _consumer_rx) = mpsc::unbounded_channel();

        assert!(
            state
                .apply_join(
                    &producer_session_id,
                    None,
                    SessionPermissions::default(),
                    producer_sender,
                    false,
                )
                .is_ok()
        );
        let producer_connection_id = state
            .session_connection_id(&producer_session_id)
            .unwrap_or(u64::MAX);
        assert!(
            state
                .apply_join(
                    &consumer_session_id,
                    None,
                    SessionPermissions::default(),
                    consumer_sender,
                    false,
                )
                .is_ok()
        );
        let consumer_connection_id = state
            .session_connection_id(&consumer_session_id)
            .unwrap_or(u64::MAX);

        let producer_media = TransportMediaId::new(1);
        let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
        state.producer_ids_by_owner_stream.insert(
            super::super::shared::ProducerKey::new(&producer_session_id, StreamType::Camera),
            producer_id,
        );
        state.producers.insert(
            producer_id,
            super::super::shared::PublishedProducer {
                owner_session_id: producer_session_id.clone(),
                owner_connection_id: producer_connection_id,
                stream_type: StreamType::Camera,
                media_kind: MediaKind::Video,
                consumable_rtp_parameters: RtpParameters::new(vec![], vec![], vec![]),
                routed_producer_id: RoutedProducerId::new(RouterId(1), ProducerId(55)),
                transport_media_id: Some(producer_media),
                source_packet_selection: None,
                active: true,
            },
        );

        let consumer_media = TransportMediaId::default();
        state.consumer_index.insert(
            ConsumerKey {
                consumer_session_id: consumer_session_id.clone(),
                producer_session_id: producer_session_id.clone(),
                stream_type: StreamType::Camera,
            },
            ConsumerState {
                routed_consumer_id: RoutedConsumerId::new(RouterId(1), ConsumerId(55)),
                consumer_connection_id,
                source_connection_id: producer_connection_id,
                source_media: TransportMediaId::default(),
                consumer_media,
            },
        );

        let outcome =
            state.apply_disconnect_sessions(&[producer_session_id.clone(), missing_session_id]);

        assert_eq!(
            outcome.transport_removals,
            vec![
                TransportMediaRemoval {
                    session: SessionId::Integer(1),
                    connection: producer_connection_id,
                    transport_media: producer_media,
                },
                TransportMediaRemoval {
                    session: consumer_session_id,
                    connection: consumer_connection_id,
                    transport_media: consumer_media,
                },
            ]
        );
        assert!(!state.has_session(&producer_session_id));
        assert!(!state.consumer_index.contains_key(&ConsumerKey {
            consumer_session_id: SessionId::Integer(2),
            producer_session_id,
            stream_type: StreamType::Camera,
        }));
    }

    #[test]
    fn replacement_join_preserves_desired_download_states() {
        let mut state = test_state();
        let producer_session_id = SessionId::Integer(1);
        let consumer_session_id = SessionId::Integer(2);
        let (producer_sender, _producer_rx) = mpsc::unbounded_channel();
        let (consumer_sender, _consumer_rx) = mpsc::unbounded_channel();
        let (replacement_sender, _replacement_rx) = mpsc::unbounded_channel();

        assert!(
            state
                .apply_join(
                    &producer_session_id,
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
                    &consumer_session_id,
                    None,
                    SessionPermissions::default(),
                    consumer_sender,
                    false,
                )
                .is_ok()
        );

        state.apply_download_state_update(
            &consumer_session_id,
            state
                .session_connection_id(&consumer_session_id)
                .unwrap_or(u64::MAX),
            &producer_session_id,
            &DownloadStates {
                audio: Some(false),
                ..DownloadStates::default()
            },
        );

        assert!(
            state
                .apply_join(
                    &consumer_session_id,
                    Some(String::from("replacement")),
                    SessionPermissions::default(),
                    replacement_sender,
                    false,
                )
                .is_ok()
        );

        let desired_download_states = state
            .sessions
            .get(&consumer_session_id)
            .and_then(|session| session.desired_download_states.get(&producer_session_id));
        assert_eq!(
            desired_download_states,
            Some(&DownloadStates {
                audio: Some(false),
                ..DownloadStates::default()
            }),
        );
    }
}
