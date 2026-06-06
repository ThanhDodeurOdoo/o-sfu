//! pure membership transitions for authoritative room state
//!
//! this module owns the synchronous half of user lifecycle changes
//! it mutates `RoomState`, captures outbound effects and returns cleanup
//! intents that async room finalization can execute after the state lock is
//! released
//!
//! transport, websocket and diagnostics work must not happen here
//! every outcome is a snapshot of work that was valid at the moment the state
//! transition committed
//!
//! leave and bulk disconnect share the same runtime removal path so topology
//! repair, transport-media cleanup and relay-route release stay aligned when a
//! user disappears

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
};

use tracing::{debug, error, warn};

#[cfg(test)]
use super::super::LocalRouterRuntimeContext;
use super::{
    super::{
        BroadcastPayload, BroadcastPayloadError, ResolvedPlacement, RoomEventMessage,
        RoomJoinError, RoomUserPermissions, UserCloseReason,
        cleanup::TransportCleanupOperation,
        media_graph::ResolvedRelayRouteEffect,
        outbound::{MessageFanout, OutboundSender},
        routing::{CommittedRoutingReceipt, DisplacedRoutingSession},
        user_negotiation::{UserNegotiation, UserNegotiationUpdate},
    },
    shared::{ActiveUser, RoomState},
};
#[cfg(test)]
use crate::engine::MediaWorkerId;
use crate::engine::{ConnectionId, UserId, UserInfo};

#[cfg(test)]
mod tests;

/// deferred side effects captured while membership state is authoritative
///
/// callers emit these only after the `RoomState` guard is released
/// this keeps websocket sends and close notifications outside the synchronous
/// state transition while preserving the order chosen by the state layer
#[derive(Debug)]
pub(in crate::engine::room) struct LifecycleEffects {
    /// user-local close messages that should reach the removed session
    pub close_requests: Vec<UserCloseRequest>,
    /// room fan-outs that should reach peers after the transition commits
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

/// close request for a user sender that state has just detached
///
/// this carries the sender by value so async finalization does not need to look
/// the user up again after membership has moved on
#[derive(Debug)]
pub(in crate::engine::room) struct UserCloseRequest {
    /// outbound queue owned by the removed or replaced user
    pub sender: OutboundSender,
    /// lifecycle reason exposed to the websocket edge
    pub reason: UserCloseReason,
}

/// data removed from state when a runtime user leaves the authoritative set
///
/// the tuple keeps teardown internal to this module while making leave and
/// disconnect consume the same removal contract
type RuntimeUserRemoval = (
    ActiveUser,
    Vec<TransportCleanupOperation>,
    Vec<ResolvedRelayRouteEffect>,
);

/// committed join result passed to async room finalization
///
/// a join may replace an existing connection for the same user
/// replacement cleanup is represented as transport removals, relay effects and
/// close requests so finalization does not need to rediscover stale state
#[derive(Debug)]
pub(in crate::engine::room) struct JoinUserOutcome {
    /// websocket effects that should run after the state guard is released
    pub effects: LifecycleEffects,
    /// joined user id cloned for diagnostics after lock release
    pub user_id: UserId,
    /// committed routing receipt for the new connection
    pub routing_receipt: CommittedRoutingReceipt,
    /// transport cleanup detached from any connection replaced during the join
    pub transport_cleanup: Vec<TransportCleanupOperation>,
    /// relay routes that belonged to replaced media state
    pub relay_effects: Vec<ResolvedRelayRouteEffect>,
}

/// committed leave result for one current connection
///
/// stale leave requests return `None` before this is built
/// every field describes work derived from the removed user while state still
/// had the authoritative indexes
#[derive(Debug)]
pub(in crate::engine::room) struct LeaveUserOutcome {
    /// close and fan-out effects for the removed current connection
    pub effects: LifecycleEffects,
    /// transport cleanup detached by the leave transition
    pub transport_cleanup: Vec<TransportCleanupOperation>,
    /// relay routes that must be released after media state is purged
    pub relay_effects: Vec<ResolvedRelayRouteEffect>,
}

/// committed user-info update ready for post-lock fan-out
///
/// the state layer decides whether the connection is current
/// callers only emit the captured fan-out after the write guard is gone
#[derive(Debug)]
pub(in crate::engine::room) struct UserInfoUpdateOutcome {
    fanout: MessageFanout,
}

impl UserInfoUpdateOutcome {
    pub fn emit(self) {
        self.fanout.emit();
    }
}

/// user removed by a bulk disconnect request
///
/// only users present in `RoomState` are returned
/// missing ids create no cleanup or diagnostics work
#[derive(Debug)]
pub(in crate::engine::room) struct DisconnectedUser {
    /// removed runtime user id
    pub user_id: UserId,
    /// connection that was current when the disconnect committed
    pub connection_id: ConnectionId,
    /// resolved close operation for the disconnected transport user
    pub close_operation: TransportCleanupOperation,
}

/// committed bulk-disconnect result
///
/// the outcome contains one entry per live user removed by the request
/// transport cleanup and peer notifications are scoped to that committed set
#[derive(Debug)]
pub(in crate::engine::room) struct DisconnectUsersOutcome {
    /// users that were present and were removed by this transition
    pub disconnected_users: Vec<DisconnectedUser>,
    /// close and fan-out effects for the removed users
    pub effects: LifecycleEffects,
    /// transport cleanup detached by all removed users
    pub transport_cleanup: Vec<TransportCleanupOperation>,
    /// relay routes that must be released after media state is purged
    pub relay_effects: Vec<ResolvedRelayRouteEffect>,
}

impl RoomState {
    fn apply_join_routing(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        is_new: bool,
        home_placement: ResolvedPlacement,
    ) -> Result<(CommittedRoutingReceipt, Option<DisplacedRoutingSession>), RoomJoinError> {
        if !is_new {
            let Some(previous_connection) = self.users.get(user_id).map(|user| user.connection_id)
            else {
                error!(
                    ?user_id,
                    "missing previous room user for replacement join routing"
                );
                return Err(RoomJoinError::RouterState);
            };
            if self
                .routing
                .committed_transport_user_key(user_id, previous_connection)
                .is_none()
            {
                error!(
                    ?user_id,
                    connection_id = ?previous_connection,
                    "missing committed routing session for replacement join"
                );
                return Err(RoomJoinError::RouterState);
            }
        }
        let mut routing = self.routing.clone();
        let affected_consumers = if is_new {
            Vec::new()
        } else {
            self.media.routed_consumer_ids_affected_by_user(user_id)
        };
        let commit = routing
            .commit_session_placement(user_id, connection_id, home_placement, affected_consumers)
            .map_err(|error| {
                error!(
                    ?user_id,
                    ?error,
                    "failed to mirror user join into room router"
                );
                RoomJoinError::RouterState
            })?;
        self.routing = routing;
        Ok(commit)
    }

    #[cfg(test)]
    fn fallback_join_placement(&self) -> ResolvedPlacement {
        ResolvedPlacement::for_test(LocalRouterRuntimeContext {
            router: self.routing.primary_router_id(),
            media_worker: MediaWorkerId::from_raw(0),
        })
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
            let old_sender = mem::replace(&mut user.sender, sender);
            user.label = label;
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
                label,
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

    /// commit a join or replacement on a preselected room placement
    ///
    /// this is the pure state half of joining a runtime session
    /// it allocates the connection id, mirrors the session into topology and
    /// captures cleanup for any replaced session before installing the new user
    /// data
    ///
    /// # errors
    ///
    /// returns `RoomFull` when a new user would exceed the room admission cap
    /// returns `RouterState` when topology cannot mirror the join
    pub fn apply_join_on_placement(
        &mut self,
        user_id: &UserId,
        label: Option<String>,
        permissions: impl Into<RoomUserPermissions>,
        sender: OutboundSender,
        emit_joined_fanout: bool,
        home_placement: ResolvedPlacement,
    ) -> Result<JoinUserOutcome, RoomJoinError> {
        let permissions = permissions.into();
        let is_new = !self.users.contains_key(user_id);
        if is_new && self.users.len() >= self.admission_policy.max_sessions {
            return Err(RoomJoinError::RoomFull);
        }
        let connection_id = ConnectionId::allocate(&mut self.next_connection_id);
        let (routing_receipt, displaced_session) =
            self.apply_join_routing(user_id, connection_id, is_new, home_placement)?;
        let transport_cleanup = displaced_session.as_ref().map_or_else(Vec::new, |session| {
            vec![TransportCleanupOperation::CloseUser {
                session_key: session.transport_session_key.clone(),
                connection_id: session.connection_id,
            }]
        });

        let previous_sender =
            self.install_joined_session(user_id, label, permissions, sender, connection_id);
        let had_previous_sender = previous_sender.is_some();
        let relay_effects = if had_previous_sender {
            let relay_effects = self.purge_user_media_state(user_id);
            if let Some(session) = displaced_session.as_ref() {
                self.resolved_relay_route_effects_with_displaced(relay_effects, user_id, session)
            } else {
                self.resolved_relay_route_effects(relay_effects)
            }
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
            effects,
            user_id: user_id.clone(),
            routing_receipt,
            transport_cleanup,
            relay_effects,
        })
    }

    /// remove one current user and return every state-derived cleanup intent
    ///
    /// this is the shared teardown primitive for runtime leave and bulk
    /// disconnect
    /// it repairs router topology, captures transport-media removals and purges
    /// media indexes while the state layer still has the only authoritative
    /// view of the user's producers and consumers
    fn remove_runtime_user(&mut self, user_id: &UserId) -> Option<RuntimeUserRemoval> {
        let user = self.users.remove(user_id)?;
        let departing_user_ids = BTreeSet::from([user_id.clone()]);
        let transport_cleanup = self.transport_cleanup_operations(
            self.collect_user_transport_removals(&departing_user_ids),
        );
        let affected_consumers = self.media.routed_consumer_ids_affected_by_user(user_id);
        let relay_effects = self.purge_user_media_state(user_id);
        let relay_effects = self.resolved_relay_route_effects(relay_effects);
        let topology_repair = self
            .routing
            .remove_session_repairing(user_id, affected_consumers);
        if !topology_repair.is_clean() {
            error!(
                ?user_id,
                errors = ?topology_repair.errors(),
                "repaired user topology during room teardown"
            );
        }
        Some((user, transport_cleanup, relay_effects))
    }

    /// commit a leave for one current runtime connection
    ///
    /// the connection id is checked before any state is removed
    /// stale close requests return `None`, leaving async finalization to handle
    /// only best-effort transport cleanup for the requested identity
    pub fn apply_leave(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<LeaveUserOutcome> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        let (user, transport_cleanup, relay_effects) = self.remove_runtime_user(user_id)?;
        Some(LeaveUserOutcome {
            effects: LifecycleEffects {
                close_requests: vec![UserCloseRequest {
                    sender: user.sender,
                    reason: UserCloseReason::RemovedByRuntime,
                }],
                fanouts: vec![self.fanout_all(&RoomEventMessage::UserDeparted {
                    user_id: user_id.clone(),
                })],
            },
            transport_cleanup,
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
        Some(UserInfoUpdateOutcome {
            fanout: self.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)),
        })
    }

    pub fn set_user_negotiated(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: o_sfu_router::MediaCapabilities,
    ) -> Option<UserNegotiationUpdate> {
        let user = self.user_mut_for_connection(user_id, connection_id)?;
        user.parsed_client_rtp_capabilities = Some(capabilities);
        Some(user.negotiation.mark_ready())
    }

    /// remove every requested user that is still current in room state
    ///
    /// missing users are ignored so runtime disconnect can be idempotent across
    /// repeated manager or websocket cleanup paths
    /// every returned cleanup intent belongs to a user that was actually
    /// removed by this transition
    pub fn apply_disconnect_users(&mut self, user_ids: &[UserId]) -> DisconnectUsersOutcome {
        let mut transport_cleanup = Vec::new();
        let mut close_requests = Vec::new();
        let mut disconnected_users = Vec::new();
        let mut fanouts = Vec::new();
        let mut relay_effects = Vec::new();
        for user_id in user_ids {
            let Some(connection_id) = self.users.get(user_id).map(|user| user.connection_id) else {
                continue;
            };
            let close_operation = TransportCleanupOperation::CloseUser {
                session_key: self.transport_user_key(user_id, connection_id),
                connection_id,
            };
            let Some((user, user_transport_cleanup, user_relay_effects)) =
                self.remove_runtime_user(user_id)
            else {
                continue;
            };
            transport_cleanup.extend(user_transport_cleanup);
            relay_effects.extend(user_relay_effects);
            disconnected_users.push(DisconnectedUser {
                user_id: user_id.clone(),
                connection_id,
                close_operation,
            });
            close_requests.push(UserCloseRequest {
                sender: user.sender,
                reason: UserCloseReason::RemovedByRuntime,
            });
            fanouts.push(self.fanout_all(&RoomEventMessage::UserDeparted {
                user_id: user_id.clone(),
            }));
        }
        DisconnectUsersOutcome {
            disconnected_users,
            effects: LifecycleEffects {
                close_requests,
                fanouts,
            },
            transport_cleanup,
            relay_effects,
        }
    }

    /// build a broadcast fan-out for one current sender
    ///
    /// stale senders return `Ok(None)` because websocket tasks can race with
    /// replacement or disconnect after they have already accepted a message
    ///
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
            Some(user_id),
        )))
    }
}
