use std::collections::{BTreeMap, BTreeSet};

use tracing::{debug, error, warn};

use super::{
    super::{
        RoomEventMessage, RoomJoinError, RoomUserPermissions, UserCloseReason,
        outbound::{MessageFanout, OutboundSender},
        topology::{RoomTopology, RoomTopologyError, TopologyPressureSnapshot},
        user_negotiation::{UserNegotiation, UserNegotiationUpdate},
    },
    layout::UserLayout,
    presence::UserPresence,
    shared::{ActiveUser, RoomState, TransportMediaRemoval},
};
use crate::runtime::{ConnectionId, UserId, UserInfo};

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

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
    pub(in crate::runtime::room) transport_media_worker_id: usize,
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
    ) -> Result<usize, RoomJoinError> {
        let mut topology = self.topology.clone();
        if !is_new && let Err(error) = self.reset_existing_session_routing(&mut topology, user_id) {
            error!(
                ?user_id,
                ?error,
                "failed to reset replaced user routing in room router"
            );
            return Err(RoomJoinError::RouterState);
        }
        let pressure = self.topology_pressure_snapshot(is_new);
        let topology_result = if is_new {
            topology.apply_client_join_with_pressure(user_id, connection_id.as_u64(), pressure)
        } else {
            topology.replace_client_session_with_pressure(user_id, connection_id.as_u64(), pressure)
        };
        if let Err(error) = topology_result {
            error!(
                ?user_id,
                ?error,
                "failed to mirror user join into room router"
            );
            return Err(RoomJoinError::RouterState);
        }
        let Some(home_placement) = topology.home_placement_for_user(user_id) else {
            error!(
                ?user_id,
                "joined user has no home placement in room topology"
            );
            return Err(RoomJoinError::RouterState);
        };
        let media_worker_id = home_placement.media_worker;
        self.topology = topology;
        Ok(media_worker_id)
    }

    fn topology_pressure_snapshot(&self, is_new_join: bool) -> TopologyPressureSnapshot {
        let receiver_count = self.users.len().saturating_add(usize::from(is_new_join));
        let max_source_fanout = self
            .consumer_keys_by_source
            .values()
            .map(BTreeSet::len)
            .max()
            .unwrap_or_default();
        TopologyPressureSnapshot {
            receiver_count,
            active_consumer_count: self.consumer_index.len(),
            pending_consumer_count: self.pending_consumer_bootstraps.len(),
            max_source_fanout,
            ..Default::default()
        }
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
        let transport_media_worker_id = self.apply_join_topology(user_id, connection_id, is_new)?;
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
            transport_media_worker_id,
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
        if let Err(error) = self.topology.remove_session(user_id) {
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
            if let Err(error) = self.topology.remove_session(user_id) {
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
