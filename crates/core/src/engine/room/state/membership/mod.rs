use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    sync::Arc,
};

use o_sfu_router::rtp::MediaCapabilities;
use tracing::{debug, error, warn};

use super::{
    super::{
        BroadcastPayload, BroadcastPayloadError, RoomEventMessage, RoomJoinError, RoomMediaCounts,
        RoomUserPermissions, RouterPlacement, UserCloseReason,
        effects::{RoomGaugeDelta, transport::RoomTransportPlan},
        media_graph::{
            CommittedTransportReceipt, SessionPlacementCommit, SessionPlacementRejection,
        },
        outbound::{MessageFanout, OutboundSender, RemoteSourceSnapshot, fanout_all},
        user_negotiation::{UserNegotiation, UserNegotiationUpdate},
    },
    UserJoinedFanout,
    shared::{ActiveUser, RoomState},
};
#[cfg(test)]
use crate::engine::MediaWorkerId;
use crate::engine::{ConnectionId, UserId, UserInfo, media_transport::TransportTeardown};

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;

#[derive(Debug, Default)]
pub struct LifecycleEffects {
    pub close_requests: Vec<UserCloseRequest>,
    pub fanouts: Vec<MessageFanout>,
    pub source_snapshots: Vec<(OutboundSender, RemoteSourceSnapshot)>,
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

type RuntimeUserRemoval = (ActiveUser, RoomTransportPlan);

#[derive(Debug)]
pub struct PresenceCommit {
    pub fanout: MessageFanout,
    pub source_snapshots: Vec<(OutboundSender, RemoteSourceSnapshot)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteSourceRefresh {
    Skip,
    OwnerConsumers,
}

#[derive(Debug)]
pub struct JoinCommit {
    pub counts: RoomGaugeDelta,
    pub effects: LifecycleEffects,
    pub receipt: CommittedTransportReceipt,
    pub transport_plan: RoomTransportPlan,
}

#[allow(
    clippy::large_enum_variant,
    reason = "connection close is cold and boxing the room transport plan would add allocation without simplifying ownership"
)]
#[derive(Debug)]
pub enum ConnectionCloseCommit {
    Current {
        counts: RoomGaugeDelta,
        user_id: UserId,
        connection_id: ConnectionId,
        session_teardown: Option<TransportTeardown>,
        effects: LifecycleEffects,
        transport_plan: RoomTransportPlan,
    },
    StalePlacement {
        counts: RoomGaugeDelta,
        session_teardown: TransportTeardown,
    },
}

#[derive(Debug)]
pub struct DisconnectCommit {
    pub counts: RoomGaugeDelta,
    pub session_teardowns: Vec<TransportTeardown>,
    pub effects: LifecycleEffects,
    pub transport_plan: RoomTransportPlan,
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
            router: self.topology.router().placement_snapshot().primary(),
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
                user_id: Arc::new(user_id.clone()),
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
    ) -> Result<JoinCommit, RoomJoinError> {
        self.apply_join_on_placement(
            user_id,
            permissions,
            sender,
            UserJoinedFanout::Suppress,
            self.fallback_join_placement(),
        )
    }

    pub fn apply_join_on_placement(
        &mut self,
        user_id: &UserId,
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        joined_fanout: UserJoinedFanout,
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
        let mut source_recipients = if previous_connection.is_some() {
            self.topology
                .committed_consumer_user_ids_for_owner_sources(user_id)
        } else {
            BTreeSet::new()
        };
        source_recipients.remove(user_id);
        let placement = self.apply_join_routing(user_id, connection_id, is_new, home_placement)?;
        let receipt = placement.receipt;
        let mut transport_plan = placement.replacement_transport_plan;
        if let Some(previous_connection) = previous_connection {
            transport_plan.extend_teardown(
                self.staged_publishes
                    .take_teardowns_for_connection(user_id, previous_connection),
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
        effects
            .source_snapshots
            .extend(self.remote_source_snapshots_for_users(source_recipients, true));
        effects.push_fanout(had_previous_sender.then(|| {
            self.fanout_all_except(
                &RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                },
                user_id,
            )
        }));
        effects.push_fanout(if joined_fanout == UserJoinedFanout::Emit {
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
            receipt,
            transport_plan,
        })
    }

    fn remove_runtime_user(&mut self, user_id: &UserId) -> Option<RuntimeUserRemoval> {
        let user = self.users.remove(user_id)?;
        let mut transport_plan = self.topology.remove_session(user_id);
        transport_plan.extend_teardown(
            self.staged_publishes
                .take_teardowns_for_connection(user_id, user.connection_id),
        );
        Some((user, transport_plan))
    }

    pub fn close_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<ConnectionCloseCommit> {
        let users_before = self.users.len();
        let media_before = self.media_counts();
        if self
            .users
            .get(user_id)
            .is_none_or(|user| user.connection_id != connection_id)
        {
            let session_key = self
                .topology
                .retire_committed_placement(user_id, connection_id)?;
            return Some(ConnectionCloseCommit::StalePlacement {
                counts: self.membership_delta(users_before, media_before),
                session_teardown: TransportTeardown::CloseSession { session_key },
            });
        }
        let session_teardown = self
            .committed_transport_user_key(user_id, connection_id)
            .map(|session_key| TransportTeardown::CloseSession { session_key });
        let mut source_recipients = self
            .topology
            .committed_consumer_user_ids_for_owner_sources(user_id);
        source_recipients.remove(user_id);
        let (user, transport_plan) = self.remove_runtime_user(user_id)?;
        Some(ConnectionCloseCommit::Current {
            counts: self.membership_delta(users_before, media_before),
            user_id: user_id.clone(),
            connection_id,
            session_teardown,
            effects: LifecycleEffects {
                close_requests: vec![UserCloseRequest {
                    sender: user.sender,
                    reason: UserCloseReason::RemovedByRuntime,
                }],
                fanouts: vec![self.fanout_all(&RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                })],
                source_snapshots: self.remote_source_snapshots_for_users(source_recipients, true),
            },
            transport_plan,
        })
    }

    pub fn apply_presence_update(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: &UserInfo,
        refresh_sources: RemoteSourceRefresh,
    ) -> Option<PresenceCommit> {
        let Some(current_user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                ?refresh_sources,
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
                ?refresh_sources,
                "discarding user presence update because the connection is stale"
            );
            return None;
        }
        {
            let user = self.user_mut_for_connection(user_id, connection_id)?;
            user.apply_info_update(info);
        }
        let source_recipients = (refresh_sources == RemoteSourceRefresh::OwnerConsumers
            && (info.is_camera_on.is_some() || info.is_screen_sharing_on.is_some()))
        .then(|| {
            self.topology
                .committed_consumer_user_ids_for_owner_sources(user_id)
        });
        let snapshot = BTreeMap::from([self.user_info_snapshot(user_id)?]);
        debug!(
            ?user_id,
            connection_id = ?connection_id,
            ?info,
            ?refresh_sources,
            snapshot_len = snapshot.len(),
            "applied user presence update and staged user info fanout"
        );
        Some(PresenceCommit {
            fanout: self.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)),
            source_snapshots: source_recipients.map_or_else(Vec::new, |recipients| {
                self.remote_source_snapshots_for_users(recipients, false)
            }),
        })
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
        let mut source_recipients = BTreeSet::new();
        for user_id in user_ids {
            source_recipients.extend(
                self.topology
                    .committed_consumer_user_ids_for_owner_sources(user_id),
            );
        }
        for user_id in user_ids {
            source_recipients.remove(user_id);
        }
        let mut close_requests = Vec::new();
        let mut session_teardowns = Vec::new();
        let mut fanouts = Vec::new();
        let mut transport_plan = RoomTransportPlan::default();
        for user_id in user_ids {
            let Some(connection_id) = self.users.get(user_id).map(|user| user.connection_id) else {
                continue;
            };
            let session_teardown = TransportTeardown::CloseSession {
                session_key: self.transport_user_key(user_id, connection_id),
            };
            let Some((user, user_transport_plan)) = self.remove_runtime_user(user_id) else {
                continue;
            };
            transport_plan.extend(user_transport_plan);
            session_teardowns.push(session_teardown);
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
            session_teardowns,
            effects: LifecycleEffects {
                close_requests,
                fanouts,
                source_snapshots: self.remote_source_snapshots_for_users(source_recipients, true),
            },
            transport_plan,
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
