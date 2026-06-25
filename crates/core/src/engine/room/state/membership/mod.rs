use std::{collections::BTreeMap, mem};

use o_sfu_router::rtp::MediaCapabilities;
use tracing::{debug, error, warn};

use super::{
    super::{
        BroadcastPayload, BroadcastPayloadError, RoomEventMessage, RoomJoinError, RoomMediaCounts,
        RoomUserPermissions, RouterPlacement, UserCloseReason,
        cleanup::TransportCleanupOperation,
        effects::RoomGaugeDelta,
        media_graph::{
            CommittedTransportReceipt, MediaTopologyEffects, SessionPlacementCommit,
            SessionPlacementRejection,
        },
        outbound::{MessageFanout, OutboundSender, fanout_all},
        user_negotiation::{UserNegotiation, UserNegotiationUpdate},
    },
    shared::{ActiveUser, RoomState},
};
#[cfg(test)]
use crate::engine::MediaWorkerId;
use crate::engine::{ConnectionId, UserId, UserInfo};

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;

#[derive(Debug, Default)]
pub struct LifecycleEffects {
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
pub struct UserCloseRequest {
    pub sender: OutboundSender,
    pub reason: UserCloseReason,
}

type RuntimeUserRemoval = (ActiveUser, MediaTopologyEffects);

#[derive(Debug)]
pub struct JoinCommit {
    pub counts: RoomGaugeDelta,
    pub effects: LifecycleEffects,
    pub receipt: CommittedTransportReceipt,
    pub media_effects: MediaTopologyEffects,
}

#[derive(Debug)]
pub enum ConnectionCloseCommit {
    Current {
        counts: RoomGaugeDelta,
        user_id: UserId,
        connection_id: ConnectionId,
        cleanup: Option<TransportCleanupOperation>,
        effects: LifecycleEffects,
        media_effects: MediaTopologyEffects,
    },
    StalePlacement {
        counts: RoomGaugeDelta,
        cleanup: TransportCleanupOperation,
    },
}

#[derive(Debug)]
pub struct DisconnectCommit {
    pub counts: RoomGaugeDelta,
    pub close_operations: Vec<TransportCleanupOperation>,
    pub effects: LifecycleEffects,
    pub media_effects: MediaTopologyEffects,
}

impl RoomState {
    pub fn fanout_all(&self, message: &RoomEventMessage) -> MessageFanout {
        fanout_all(self.users.values().map(|user| user.sender.clone()), message)
    }

    fn membership_delta(
        &self,
        users_before: usize,
        media_before: RoomMediaCounts,
    ) -> RoomGaugeDelta {
        RoomGaugeDelta::membership(
            users_before,
            self.users.len(),
            media_before,
            self.media_counts(),
        )
    }

    pub fn fanout_all_except(
        &self,
        message: &RoomEventMessage,
        excluded_user_id: &UserId,
    ) -> MessageFanout {
        fanout_all(
            self.users
                .iter()
                .filter(|(user_id, _session)| excluded_user_id != *user_id)
                .map(|(_user_id, user)| user.sender.clone()),
            message,
        )
    }

    fn apply_join_routing(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        is_new: bool,
        home_placement: RouterPlacement,
    ) -> Result<SessionPlacementCommit, RoomJoinError> {
        let previous_connection = if is_new {
            None
        } else {
            let Some(previous_connection) = self.users.get(user_id).map(|user| user.connection_id)
            else {
                error!(
                    ?user_id,
                    "missing previous room user for replacement join routing"
                );
                return Err(RoomJoinError::RouterState);
            };
            Some(previous_connection)
        };
        self.topology
            .commit_session_placement(user_id, connection_id, previous_connection, home_placement)
            .map_err(|rejection| {
                match rejection {
                    SessionPlacementRejection::MissingPreviousSession {
                        previous_connection,
                    } => {
                        error!(
                            ?user_id,
                            connection_id = ?previous_connection,
                            "missing committed routing session for replacement join"
                        );
                    }
                    SessionPlacementRejection::Router(error) => {
                        error!(
                            ?user_id,
                            ?error,
                            "failed to mirror user join into room router"
                        );
                    }
                }
                RoomJoinError::RouterState
            })
    }

    #[cfg(test)]
    fn fallback_join_placement(&self) -> RouterPlacement {
        RouterPlacement {
            router: self.topology.routing().usage_snapshot().primary_router(),
            media_worker: MediaWorkerId::from_raw(0),
        }
    }

    fn install_joined_session(
        &mut self,
        user_id: &UserId,
        permissions: RoomUserPermissions,
        sender: OutboundSender,
        connection_id: ConnectionId,
    ) -> Option<OutboundSender> {
        if let Some(user) = self.users.get_mut(user_id) {
            let old_sender = mem::replace(&mut user.sender, sender);
            user.permissions = permissions;
            user.reset_presentation();
            user.negotiation = UserNegotiation::default();
            user.parsed_client_rtp_capabilities = None;
            user.connection_id = connection_id;
            return Some(old_sender);
        }
        self.users.insert(
            user_id.clone(),
            ActiveUser {
                permissions,
                info: UserInfo::default(),
                server_featured: None,
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
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
    ) -> Result<JoinCommit, RoomJoinError> {
        self.apply_join_on_placement(
            user_id,
            permissions,
            sender,
            emit_joined_fanout,
            self.fallback_join_placement(),
        )
    }

    pub fn apply_join_on_placement(
        &mut self,
        user_id: &UserId,
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
        home_placement: RouterPlacement,
    ) -> Result<JoinCommit, RoomJoinError> {
        let permissions = permissions.into();
        let previous_connection = self.users.get(user_id).map(|user| user.connection_id);
        let is_new = previous_connection.is_none();
        if is_new && self.users.len() >= self.admission_policy.max_sessions {
            return Err(RoomJoinError::RoomFull);
        }
        let users_before = self.users.len();
        let media_before = self.media_counts();
        let connection_id = ConnectionId::allocate(&mut self.next_connection_id);
        let placement = self.apply_join_routing(user_id, connection_id, is_new, home_placement)?;
        let routing_receipt = placement.receipt;
        let mut media_effects = placement.replacement_effects;
        if let Some(previous_connection) = previous_connection {
            media_effects.extend_cleanup(
                self.staged_publishes
                    .cleanup_operations_for_connection(user_id, previous_connection),
            );
        }

        let previous_sender =
            self.install_joined_session(user_id, permissions, sender, connection_id);
        let had_previous_sender = previous_sender.is_some();

        let mut effects = LifecycleEffects::default();
        effects.push_close_request(previous_sender.map(|sender| UserCloseRequest {
            sender,
            reason: UserCloseReason::Replaced,
        }));
        effects.push_fanout(had_previous_sender.then(|| {
            self.fanout_all_except(
                &RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                },
                user_id,
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
                        user_id,
                    )
                })
        } else {
            None
        });
        Ok(JoinCommit {
            counts: self.membership_delta(users_before, media_before),
            effects,
            receipt: routing_receipt,
            media_effects,
        })
    }

    fn remove_runtime_user(&mut self, user_id: &UserId) -> Option<RuntimeUserRemoval> {
        let user = self.users.remove(user_id)?;
        let mut media_effects = self.topology.remove_session(user_id, user.connection_id);
        media_effects.extend_cleanup(
            self.staged_publishes
                .cleanup_operations_for_connection(user_id, user.connection_id),
        );
        Some((user, media_effects))
    }

    pub fn close_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<ConnectionCloseCommit> {
        let users_before = self.users.len();
        let media_before = self.media_counts();
        let cleanup = self
            .committed_transport_user_key(user_id, connection_id)
            .map(|session_key| TransportCleanupOperation::CloseUser { session_key });
        if self
            .users
            .get(user_id)
            .is_none_or(|user| user.connection_id != connection_id)
        {
            self.topology
                .unregister_committed_placement(user_id, connection_id);
            return cleanup.map(|cleanup| ConnectionCloseCommit::StalePlacement {
                counts: self.membership_delta(users_before, media_before),
                cleanup,
            });
        }
        let (user, media_effects) = self.remove_runtime_user(user_id)?;
        Some(ConnectionCloseCommit::Current {
            counts: self.membership_delta(users_before, media_before),
            user_id: user_id.clone(),
            connection_id,
            cleanup,
            effects: LifecycleEffects {
                close_requests: vec![UserCloseRequest {
                    sender: user.sender,
                    reason: UserCloseReason::RemovedByRuntime,
                }],
                fanouts: vec![self.fanout_all(&RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                })],
            },
            media_effects,
        })
    }

    pub fn apply_presence_update(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: &UserInfo,
        need_refresh: bool,
    ) -> Option<MessageFanout> {
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
            user.apply_info_update(info);
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
        Some(self.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)))
    }

    pub fn set_user_negotiated(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
    ) -> Option<UserNegotiationUpdate> {
        let user = self.user_mut_for_connection(user_id, connection_id)?;
        user.parsed_client_rtp_capabilities = Some(capabilities);
        Some(user.negotiation.mark_ready())
    }

    pub fn apply_disconnect_users(&mut self, user_ids: &[UserId]) -> DisconnectCommit {
        let users_before = self.users.len();
        let media_before = self.media_counts();
        let mut close_requests = Vec::new();
        let mut close_operations = Vec::new();
        let mut fanouts = Vec::new();
        let mut media_effects = MediaTopologyEffects::default();
        for user_id in user_ids {
            let Some(connection_id) = self.users.get(user_id).map(|user| user.connection_id) else {
                continue;
            };
            let close_operation = TransportCleanupOperation::CloseUser {
                session_key: self.transport_user_key(user_id, connection_id),
            };
            let Some((user, user_media_effects)) = self.remove_runtime_user(user_id) else {
                continue;
            };
            media_effects.extend(user_media_effects);
            close_operations.push(close_operation);
            close_requests.push(UserCloseRequest {
                sender: user.sender,
                reason: UserCloseReason::RemovedByRuntime,
            });
            fanouts.push(self.fanout_all(&RoomEventMessage::UserDeparted {
                user_id: user_id.clone(),
            }));
        }
        DisconnectCommit {
            counts: self.membership_delta(users_before, media_before),
            close_operations,
            effects: LifecycleEffects {
                close_requests,
                fanouts,
            },
            media_effects,
        }
    }

    /// # errors
    ///
    /// returns `BroadcastPayloadError` when the payload cannot fit within the
    /// room broadcast limit
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
            user_id,
        )))
    }
}
