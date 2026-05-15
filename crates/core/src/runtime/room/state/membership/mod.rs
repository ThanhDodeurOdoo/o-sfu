use std::collections::{BTreeMap, BTreeSet};

use tracing::{debug, error, warn};

use super::{
    super::{
        BroadcastPayload, BroadcastPayloadError, LocalRouterRuntimeContext, RoomEventMessage,
        RoomJoinError, RoomUserPermissions, UserCloseReason,
        outbound::{MessageFanout, OutboundSender},
        topology::{RoomTopology, RoomTopologyError},
        user_negotiation::{UserNegotiation, UserNegotiationUpdate},
    },
    layout::UserLayout,
    media::relay::RelayRouteEffect,
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
    pub close_requests: Vec<UserCloseRequest>,
    pub fanouts: Vec<MessageFanout>,
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
    pub sender: OutboundSender,
    pub reason: UserCloseReason,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct JoinUserOutcome {
    pub connection_id: ConnectionId,
    pub effects: LifecycleEffects,
    pub user_id: UserId,
    pub transport_home_placement: LocalRouterRuntimeContext,
    pub transport_media_worker_id: usize,
    pub transport_removals: Vec<TransportMediaRemoval>,
    pub relay_effects: Vec<RelayRouteEffect>,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct LeaveUserOutcome {
    pub effects: LifecycleEffects,
    pub transport_removals: Vec<TransportMediaRemoval>,
    pub relay_effects: Vec<RelayRouteEffect>,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct UserInfoUpdateOutcome {
    fanout: MessageFanout,
}

impl UserInfoUpdateOutcome {
    pub fn emit(self) {
        self.fanout.emit();
    }
}

#[derive(Debug)]
pub(in crate::runtime::room) struct DisconnectedUser {
    pub user_id: UserId,
    pub connection_id: ConnectionId,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct DisconnectUsersOutcome {
    pub disconnected_users: Vec<DisconnectedUser>,
    pub effects: LifecycleEffects,
    pub transport_removals: Vec<TransportMediaRemoval>,
    pub relay_effects: Vec<RelayRouteEffect>,
}

impl RoomState {
    fn apply_join_topology(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        is_new: bool,
        home_placement: LocalRouterRuntimeContext,
    ) -> Result<LocalRouterRuntimeContext, RoomJoinError> {
        let mut topology = self.topology.clone();
        if !is_new && let Err(error) = self.reset_existing_session_routing(&mut topology, user_id) {
            error!(
                ?user_id,
                ?error,
                "failed to reset replaced user routing in room router"
            );
            return Err(RoomJoinError::RouterState);
        }
        let topology_result = if is_new {
            topology.apply_client_join_on_placement(user_id, connection_id.as_u64(), home_placement)
        } else {
            topology.replace_client_session_on_placement(
                user_id,
                connection_id.as_u64(),
                home_placement,
            )
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
        self.topology = topology;
        Ok(home_placement)
    }

    #[cfg(test)]
    fn fallback_join_placement(&self) -> LocalRouterRuntimeContext {
        self.topology
            .local_placements()
            .into_iter()
            .next()
            .unwrap_or_else(|| LocalRouterRuntimeContext {
                router: self.topology.primary_router_id(),
                media_worker: 0,
            })
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
                desired_source_subscriptions: BTreeMap::new(),
                parsed_client_rtp_capabilities: None,
                connection_id,
                sender,
            },
        );
        None
    }

    #[cfg(test)]
    pub fn apply_join(
        &mut self,
        user_id: &UserId,
        label: Option<String>,
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
    ) -> Result<JoinUserOutcome, RoomJoinError> {
        self.apply_join_on_placement(
            user_id,
            label,
            permissions,
            sender,
            emit_joined_fanout,
            self.fallback_join_placement(),
        )
    }

    pub fn apply_join_on_placement(
        &mut self,
        user_id: &UserId,
        label: Option<String>,
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
        home_placement: LocalRouterRuntimeContext,
    ) -> Result<JoinUserOutcome, RoomJoinError> {
        let permissions = permissions.into();
        let is_new = !self.users.contains_key(user_id);
        if is_new && self.users.len() >= self.admission_policy.max_sessions {
            return Err(RoomJoinError::RoomFull);
        }
        let connection_id = ConnectionId::allocate(&mut self.next_connection_id);
        let transport_home_placement =
            self.apply_join_topology(user_id, connection_id, is_new, home_placement)?;
        let transport_media_worker_id = transport_home_placement.media_worker;
        let transport_removals = self.join_transport_removals(user_id, is_new);

        let previous_sender =
            self.install_joined_session(user_id, label, permissions, sender, connection_id);
        let had_previous_sender = previous_sender.is_some();
        let relay_effects = if had_previous_sender {
            self.purge_user_media_state(user_id)
        } else {
            Vec::new()
        };

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
            transport_home_placement,
            transport_media_worker_id,
            transport_removals,
            relay_effects,
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

    pub fn apply_leave(
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
        let topology_repair = self.topology.remove_session_repairing(user_id);
        if !topology_repair.is_clean() {
            error!(
                ?user_id,
                errors = ?topology_repair.errors(),
                "repaired departed user topology during room teardown"
            );
        }
        let relay_effects = self.purge_user_media_state(user_id);
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
            relay_effects,
        })
    }

    pub fn apply_presence_update(
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

    pub fn set_user_negotiated(
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

    pub fn apply_disconnect_users(&mut self, user_ids: &[UserId]) -> DisconnectUsersOutcome {
        let mut transport_removals = Vec::new();
        let mut close_requests = Vec::new();
        let mut disconnected_users = Vec::new();
        let mut fanouts = Vec::new();
        let mut relay_effects = Vec::new();
        for user_id in user_ids {
            if !self.users.contains_key(user_id) {
                continue;
            }
            let topology_repair = self.topology.remove_session_repairing(user_id);
            if !topology_repair.is_clean() {
                error!(
                    ?user_id,
                    errors = ?topology_repair.errors(),
                    "repaired disconnected user topology during room teardown"
                );
            }
            transport_removals
                .extend(self.collect_user_transport_removals(&BTreeSet::from([user_id.clone()])));
            if let Some(user) = self.users.remove(user_id) {
                let connection_id = user.connection_id;
                relay_effects.extend(self.purge_user_media_state(user_id));
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
            relay_effects,
        }
    }

    pub fn broadcast_fanout(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) -> Result<Option<MessageFanout>, BroadcastPayloadError> {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return Ok(None);
        }
        let message = BroadcastPayload::try_new(message)?;
        Ok(Some(self.fanout_all_except(
            &RoomEventMessage::Broadcast {
                sender_id: user_id.clone(),
                message,
            },
            Some(user_id),
        )))
    }
}
