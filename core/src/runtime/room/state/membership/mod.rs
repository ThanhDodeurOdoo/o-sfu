use std::collections::{BTreeMap, BTreeSet};

use tracing::{debug, error, warn};

use super::{
    super::{
        RoomEventMessage, RoomJoinError, RoomUserPermissions, UserCloseReason,
        outbound::{MessageFanout, OutboundSender},
        topology::{RoomTopology, RoomTopologyError},
        user_negotiation::{UserNegotiation, UserNegotiationUpdate},
    },
    layout::UserLayout,
    presence::UserPresence,
    shared::{ActiveUser, RoomState, TransportMediaRemoval},
};
use crate::runtime::{ConnectionId, UserId, UserInfo};

#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

#[derive(Debug)]
pub(in crate::runtime::room) struct LifecycleEffects {
    pub(in crate::runtime::room) close_requests: Vec<UserCloseRequest>,
    pub(in crate::runtime::room) fanouts: Vec<MessageFanout>,
}

impl LifecycleEffects {
    fn push_fanout(&mut self, fanout: Option<MessageFanout>) {
        if let Some(fanout) = fanout {
            self.fanouts.push(fanout);
        }
    }

    fn push_close_request(&mut self, request: Option<UserCloseRequest>) {
        if let Some(request) = request {
            self.close_requests.push(request);
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime::room) struct UserCloseRequest {
    pub(in crate::runtime::room) sender: OutboundSender,
    pub(in crate::runtime::room) reason: UserCloseReason,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct JoinUserOutcome {
    pub(in crate::runtime::room) connection_id: ConnectionId,
    pub(in crate::runtime::room) effects: LifecycleEffects,
    pub(in crate::runtime::room) user_id: UserId,
    pub(in crate::runtime::room) transport_removals: Vec<TransportMediaRemoval>,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct LeaveUserOutcome {
    pub(in crate::runtime::room) effects: LifecycleEffects,
    pub(in crate::runtime::room) transport_removals: Vec<TransportMediaRemoval>,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct UserInfoUpdateOutcome {
    fanout: MessageFanout,
}

impl UserInfoUpdateOutcome {
    pub(in crate::runtime::room) fn emit(self) {
        self.fanout.emit();
    }
}

#[derive(Debug)]
pub(in crate::runtime::room) struct DisconnectedUser {
    pub(in crate::runtime::room) user_id: UserId,
    pub(in crate::runtime::room) connection_id: ConnectionId,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct DisconnectUsersOutcome {
    pub(in crate::runtime::room) disconnected_users: Vec<DisconnectedUser>,
    pub(in crate::runtime::room) effects: LifecycleEffects,
    pub(in crate::runtime::room) transport_removals: Vec<TransportMediaRemoval>,
}

impl RoomState {
    fn apply_join_topology(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        is_new: bool,
    ) -> Result<(), RoomJoinError> {
        let mut topology = self.topology.clone();
        if let Err(error) = topology.apply_client_join(user_id, connection_id.as_u64()) {
            error!(
                ?user_id,
                ?error,
                "failed to mirror user join into room router"
            );
            return Err(RoomJoinError::RouterState);
        }
        if !is_new && let Err(error) = self.reset_existing_session_routing(&mut topology, user_id) {
            error!(
                ?user_id,
                ?error,
                "failed to reset replaced user routing in room router"
            );
            return Err(RoomJoinError::RouterState);
        }
        self.topology = topology;
        Ok(())
    }

    fn join_transport_removals(
        &self,
        user_id: &UserId,
        is_new: bool,
    ) -> Vec<TransportMediaRemoval> {
        if is_new {
            return Vec::new();
        }
        self.collect_user_transport_removals(&BTreeSet::from([user_id.clone()]))
    }

    fn install_joined_session(
        &mut self,
        user_id: &UserId,
        label: Option<String>,
        permissions: RoomUserPermissions,
        sender: OutboundSender,
        connection_id: ConnectionId,
    ) -> Option<OutboundSender> {
        if let Some(user) = self.users.get_mut(user_id) {
            let old_sender = user.sender.clone();
            user.label.clone_from(&label);
            user.permissions.clone_from(&permissions);
            user.presence = UserPresence::default();
            user.layout = UserLayout::default();
            user.negotiation = UserNegotiation::default();
            user.parsed_client_rtp_capabilities = None;
            user.connection_id = connection_id;
            user.sender = sender;
            return Some(old_sender);
        }
        self.users.insert(
            user_id.clone(),
            ActiveUser {
                label,
                permissions,
                presence: UserPresence::default(),
                layout: UserLayout::default(),
                negotiation: UserNegotiation::default(),
                desired_download_states: BTreeMap::new(),
                parsed_client_rtp_capabilities: None,
                connection_id,
                sender,
            },
        );
        None
    }

    pub(in crate::runtime::room) fn apply_join(
        &mut self,
        user_id: &UserId,
        label: Option<String>,
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
    ) -> Result<JoinUserOutcome, RoomJoinError> {
        let permissions = permissions.into();
        let is_new = !self.users.contains_key(user_id);
        if is_new && self.users.len() >= self.admission_policy.max_sessions {
            return Err(RoomJoinError::RoomFull);
        }
        let connection_id = ConnectionId::allocate(&mut self.next_connection_id);
        self.apply_join_topology(user_id, connection_id, is_new)?;
        let transport_removals = self.join_transport_removals(user_id, is_new);

        let previous_sender =
            self.install_joined_session(user_id, label, permissions, sender, connection_id);
        let had_previous_sender = previous_sender.is_some();
        if had_previous_sender {
            self.purge_user_media_state(user_id);
        }

        let mut effects = LifecycleEffects {
            close_requests: Vec::new(),
            fanouts: Vec::new(),
        };
        effects.push_close_request(previous_sender.map(|sender| UserCloseRequest {
            sender,
            reason: UserCloseReason::Replaced,
        }));
        effects.push_fanout(had_previous_sender.then(|| {
            self.fanout_all_except(
                &RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                },
                Some(user_id),
            )
        }));
        effects.push_fanout(if emit_joined_fanout {
            self.user_info_snapshot(user_id)
                .map(|(joined_user_id, info)| {
                    self.fanout_all_except(
                        &RoomEventMessage::UserJoined {
                            user_id: joined_user_id,
                            info,
                        },
                        Some(user_id),
                    )
                })
        } else {
            None
        });
        Ok(JoinUserOutcome {
            connection_id,
            effects,
            user_id: user_id.clone(),
            transport_removals,
        })
    }

    fn reset_existing_session_routing(
        &self,
        topology: &mut RoomTopology,
        user_id: &UserId,
    ) -> Result<(), RoomTopologyError> {
        let routed_consumers = self
            .consumer_keys_for_user(user_id)
            .into_iter()
            .filter_map(|key| self.consumer_index.get(&key))
            .map(|consumer_state| consumer_state.routed_consumer_id)
            .collect::<Vec<_>>();
        for routed_consumer_id in routed_consumers {
            topology.remove_consumer(routed_consumer_id)?;
        }

        let routed_producers = self
            .producer_ids_for_user(user_id)
            .into_iter()
            .filter_map(|producer_id| self.producers.get(&producer_id))
            .map(|producer| producer.routed_producer_id)
            .collect::<Vec<_>>();
        for routed_producer_id in routed_producers {
            topology.remove_producer(routed_producer_id)?;
        }
        Ok(())
    }

    pub(in crate::runtime::room) fn apply_leave(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<LeaveUserOutcome> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        let transport_removals =
            self.collect_user_transport_removals(&BTreeSet::from([user_id.clone()]));
        if let Err(error) = self.topology.apply_client_leave(user_id) {
            error!(
                ?user_id,
                ?error,
                "failed to remove departed user from room router"
            );
            return None;
        }
        self.purge_user_media_state(user_id);
        let user = self.users.remove(user_id)?;
        let mut effects = LifecycleEffects {
            close_requests: Vec::new(),
            fanouts: Vec::new(),
        };
        effects.push_fanout(Some(self.fanout_all(&RoomEventMessage::UserDeparted {
            user_id: user_id.clone(),
        })));
        effects.push_close_request(Some(UserCloseRequest {
            sender: user.sender,
            reason: UserCloseReason::RemovedByRuntime,
        }));
        Some(LeaveUserOutcome {
            effects,
            transport_removals,
        })
    }

    pub(in crate::runtime::room) fn apply_presence_update(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: &UserInfo,
        need_refresh: bool,
    ) -> Option<UserInfoUpdateOutcome> {
        let Some(current_user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                need_refresh,
                "discarding user presence update because the user is missing"
            );
            return None;
        };
        if current_user.connection_id != connection_id {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                current_connection_id = ?current_user.connection_id,
                ?info,
                need_refresh,
                "discarding user presence update because the connection is stale"
            );
            return None;
        }
        {
            let user = self.user_mut_for_connection(user_id, connection_id)?;
            user.presence.apply_update(info);
        }
        let snapshot = if need_refresh {
            self.user_info_snapshot_all()
        } else {
            BTreeMap::from([self.user_info_snapshot(user_id)?])
        };
        debug!(
            ?user_id,
            connection_id = ?connection_id,
            ?info,
            need_refresh,
            snapshot_len = snapshot.len(),
            "applied user presence update and staged user info fanout"
        );
        Some(UserInfoUpdateOutcome {
            fanout: self.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)),
        })
    }

    pub(in crate::runtime::room) fn set_user_negotiated(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: &o_sfu_router::MediaCapabilities,
    ) -> UserNegotiationUpdate {
        let Some(user) = self.user_mut_for_connection(user_id, connection_id) else {
            return UserNegotiationUpdate::default();
        };
        user.parsed_client_rtp_capabilities = Some(capabilities.clone());
        user.negotiation.set_user_negotiated()
    }

    pub(in crate::runtime::room) fn apply_disconnect_users(
        &mut self,
        user_ids: &[UserId],
    ) -> DisconnectUsersOutcome {
        let mut transport_removals = Vec::new();
        let mut close_requests = Vec::new();
        let mut disconnected_users = Vec::new();
        let mut fanouts = Vec::new();
        for user_id in user_ids {
            if !self.users.contains_key(user_id) {
                continue;
            }
            if let Err(error) = self.topology.apply_client_leave(user_id) {
                error!(
                    ?user_id,
                    ?error,
                    "failed to mirror bulk disconnect into room router"
                );
                continue;
            }
            transport_removals
                .extend(self.collect_user_transport_removals(&BTreeSet::from([user_id.clone()])));
            if let Some(user) = self.users.remove(user_id) {
                let connection_id = user.connection_id;
                self.purge_user_media_state(user_id);
                disconnected_users.push(DisconnectedUser {
                    user_id: user_id.clone(),
                    connection_id,
                });
                close_requests.push(UserCloseRequest {
                    sender: user.sender,
                    reason: UserCloseReason::RemovedByRuntime,
                });
                fanouts.push(self.fanout_all(&RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                }));
            }
        }
        DisconnectUsersOutcome {
            disconnected_users,
            effects: LifecycleEffects {
                close_requests,
                fanouts,
            },
            transport_removals,
        }
    }

    pub(in crate::runtime::room) fn broadcast_fanout(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) -> Option<MessageFanout> {
        self.user_for_connection(user_id, connection_id)?;
        Some(self.fanout_all_except(
            &RoomEventMessage::Broadcast {
                sender_id: user_id.clone(),
                message,
            },
            Some(user_id),
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

    use o_sfu_router::{ConsumerId, MediaKind, MediaStream, ProducerId, RouterId};
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        MediaCodecFlags,
        runtime::{
            ConnectionId, RoomInstanceId, StreamType, UserPermissions,
            metrics::RuntimeMetrics,
            recording::{MediaSource, MediaTap, RecordingService},
            room::{
                RoomAdmissionPolicy,
                rtp_capabilities::router_rtp_capabilities,
                state::{
                    ids::ProducerRuntimeId,
                    shared::{
                        ConsumerKey, ConsumerState, SourceKey, SourceTransportMediaIndexEntry,
                    },
                },
                topology::{RoutedConsumerId, RoutedProducerId},
            },
            source_model::{
                PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
                PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
                SourceEncodingId,
            },
            transport_adapter::TransportMediaId,
        },
    };

    fn test_state() -> RoomState {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        RoomState::new(
            RouterId(1),
            RoomAdmissionPolicy::new(4),
            router_rtp_capabilities(MediaCodecFlags::default()),
            Arc::new(RecordingService::new(
                RoomInstanceId::from_raw(0),
                media_source,
                Arc::new(RuntimeMetrics::default()),
            )),
        )
    }

    fn install_test_published_producer(
        state: &mut RoomState,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        routed_producer_id: RoutedProducerId,
        transport_media_id: Option<TransportMediaId>,
    ) -> (ProducerRuntimeId, PublishedSourceId) {
        let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
        let source_id = PublishedSourceId::allocate(&mut state.next_source_id);
        let encoding_id = SourceEncodingId::allocate(&mut state.next_source_encoding_id);
        let source = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(user_id.clone()),
            stream_type,
            media_kind: MediaKind::Video,
            mid: None,
            encodings: vec![SourceEncodingDescriptor::new(
                SourceEncodingDescriptorParts {
                    encoding_id,
                    source_id,
                    rid: None,
                    primary_ssrc: None,
                    repair_ssrc: None,
                    max_bitrate: None,
                    resolution_scale: None,
                    max_framerate: None,
                    policy_role: None,
                    max_temporal_layer_id: None,
                    negotiated_format: None,
                },
            )],
        })
        .expect("test source graph should be valid");
        state.sources.insert(source_id, source);
        state
            .source_ids_by_owner_stream
            .insert(SourceKey::new(user_id, stream_type), source_id);
        state
            .producer_id_by_source_id
            .insert(source_id, producer_id);
        state.register_source_owner(user_id, source_id);
        state.producers.insert(
            producer_id,
            super::super::shared::PublishedProducer {
                source_id,
                owner_user_id: user_id.clone(),
                owner_connection_id: connection_id,
                stream_type,
                media_kind: MediaKind::Video,
                consumable_rtp_parameters: MediaStream::new(vec![], vec![], vec![]),
                routed_producer_id,
                transport_media_id,
                active: true,
            },
        );
        state.register_producer_owner(user_id, producer_id);
        if let Some(transport_media_id) = transport_media_id {
            state.source_transport_media_index.insert(
                transport_media_id,
                SourceTransportMediaIndexEntry::new(
                    source_id,
                    vec![encoding_id],
                    user_id.clone(),
                    connection_id,
                    stream_type,
                ),
            );
        }
        (producer_id, source_id)
    }

    #[test]
    fn disconnect_sessions_removes_current_members_and_fanouts_departures() {
        let mut state = test_state();
        let (sender_a, _receiver_a) = mpsc::unbounded_channel();
        let (sender_b, _receiver_b) = mpsc::unbounded_channel();
        assert!(
            state
                .apply_join(
                    &UserId::Integer(1),
                    None,
                    UserPermissions::default(),
                    sender_a,
                    false,
                )
                .is_ok()
        );
        assert!(
            state
                .apply_join(
                    &UserId::Integer(2),
                    None,
                    UserPermissions::default(),
                    sender_b,
                    false,
                )
                .is_ok()
        );

        let outcome = state.apply_disconnect_users(&[UserId::Integer(1), UserId::Integer(2)]);

        assert_eq!(state.users.len(), 0);
        assert_eq!(outcome.disconnected_users.len(), 2);
        assert!(outcome.disconnected_users.iter().any(|user| {
            user.user_id == UserId::Integer(1) && user.connection_id == ConnectionId::from_raw(0)
        }));
        assert!(outcome.disconnected_users.iter().any(|user| {
            user.user_id == UserId::Integer(2) && user.connection_id == ConnectionId::from_raw(1)
        }));
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
                    &UserId::Integer(1),
                    None,
                    UserPermissions::default(),
                    producer_sender,
                    false,
                )
                .is_ok()
        );
        assert!(
            state
                .apply_join(
                    &UserId::Integer(2),
                    None,
                    UserPermissions::default(),
                    consumer_sender,
                    false,
                )
                .is_ok()
        );
        let producer_connection_id = state
            .user_connection_id(&UserId::Integer(1))
            .expect("producer user should exist");
        let consumer_connection_id = state
            .user_connection_id(&UserId::Integer(2))
            .expect("consumer user should exist");
        let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(10));
        let (producer_id, source_id) = install_test_published_producer(
            &mut state,
            &UserId::Integer(1),
            producer_connection_id,
            StreamType::Camera,
            routed_producer_id,
            Some(TransportMediaId::new(11)),
        );
        let consumer_key = ConsumerKey::new(&UserId::Integer(2), source_id);
        state.consumer_index.insert(
            consumer_key.clone(),
            ConsumerState {
                routed_consumer_id: RoutedConsumerId::new(RouterId(1), ConsumerId(20)),
                consumer_connection_id,
                source_connection_id: producer_connection_id,
                source_media: TransportMediaId::new(11),
                consumer_media: TransportMediaId::new(21),
            },
        );
        state.register_consumer_key(&consumer_key);

        let outcome = state.apply_leave(&UserId::Integer(2), consumer_connection_id);

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
                    &UserId::Integer(1),
                    None,
                    UserPermissions::default(),
                    sender,
                    false,
                )
                .is_ok()
        );

        let fanout = state.broadcast_fanout(
            &UserId::Integer(1),
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
                    &UserId::Integer(1),
                    None,
                    UserPermissions::default(),
                    sender,
                    false,
                )
                .is_ok()
        );

        let outcome = state.apply_presence_update(
            &UserId::Integer(1),
            ConnectionId::from_raw(999),
            &UserInfo::default(),
            false,
        );

        assert!(outcome.is_none());
    }

    #[test]
    fn disconnect_sessions_ignores_missing_members() {
        let mut state = test_state();
        let outcome = state.apply_disconnect_users(&[UserId::Integer(1)]);

        assert!(outcome.transport_removals.is_empty());
        assert!(outcome.effects.close_requests.is_empty());
        assert!(outcome.effects.fanouts.is_empty());
    }

    #[test]
    fn replacement_join_clears_transport_media_owner_index() {
        let mut state = test_state();
        let user_id = UserId::Integer(1);
        let (sender, _receiver) = mpsc::unbounded_channel();
        let (replacement_sender, _replacement_receiver) = mpsc::unbounded_channel();
        assert!(
            state
                .apply_join(&user_id, None, UserPermissions::default(), sender, false,)
                .is_ok()
        );
        let connection_id = state
            .user_connection_id(&user_id)
            .expect("user should have a connection id");
        let transport_media_id = TransportMediaId::new(30);
        let routed_producer_id = state
            .topology
            .add_producer(&user_id, MediaKind::Video)
            .expect("replacement test producer route should be added");
        install_test_published_producer(
            &mut state,
            &user_id,
            connection_id,
            StreamType::Camera,
            routed_producer_id,
            Some(transport_media_id),
        );

        assert_eq!(
            state.inspect_producer_owner_user_id_for_transport_media_id(transport_media_id),
            Some(user_id.clone())
        );
        assert_eq!(
            state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
            Some(connection_id)
        );

        assert!(
            state
                .apply_join(
                    &user_id,
                    Some(String::from("replacement")),
                    UserPermissions::default(),
                    replacement_sender,
                    false,
                )
                .is_ok()
        );

        assert_eq!(
            state.inspect_producer_owner_user_id_for_transport_media_id(transport_media_id),
            None
        );
        assert_eq!(
            state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
            None
        );
        assert!(state.source_ids_by_owner.is_empty());
        assert!(state.producer_ids_by_owner.is_empty());
    }
}
