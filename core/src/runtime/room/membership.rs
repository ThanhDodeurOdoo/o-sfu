use o_sfu_router::MediaCapabilities;
use tokio::sync::mpsc;
use tracing::warn;

use super::{
    Room, RoomJoinError, RoomUserPermissions, UserOutbound,
    state::{
        DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects,
        TransportMediaRemoval,
    },
    user_negotiation::UserNegotiationUpdate,
};
use crate::{
    SessionNegotiationOutcome, UserInfoRefresh,
    runtime::{
        ConnectionId, UserId, UserInfo, UserPermissions,
        diagnostics::DiagnosticsEventData,
        telemetry::schema::event as telemetry_event,
        transport_adapter::{MediaPort, RuntimeTransportAdapter, SessionPort},
    },
};
#[derive(Clone, Copy)]
pub(in crate::runtime::room) struct UserCleanup<'a> {
    transport_adapter: Option<&'a RuntimeTransportAdapter>,
    clean_transport_state: bool,
}

impl<'a> UserCleanup<'a> {
    pub(in crate::runtime::room) const fn runtime(
        transport_adapter: &'a RuntimeTransportAdapter,
    ) -> Self {
        Self {
            transport_adapter: Some(transport_adapter),
            clean_transport_state: true,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) const fn state_only(
        transport_adapter: Option<&'a RuntimeTransportAdapter>,
    ) -> Self {
        Self {
            transport_adapter,
            clean_transport_state: false,
        }
    }

    pub(in crate::runtime::room) const fn transport_adapter(
        self,
    ) -> Option<&'a RuntimeTransportAdapter> {
        self.transport_adapter
    }

    const fn cleans_transport_state(self) -> bool {
        self.clean_transport_state
    }
}

enum UserTransition<'a> {
    Join {
        user_id: &'a UserId,
        label: Option<String>,
        permissions: RoomUserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        emit_joined_fanout: bool,
    },
    Close {
        user_id: &'a UserId,
        connection_id: ConnectionId,
    },
    Disconnect {
        user_ids: &'a [UserId],
    },
}

enum UserTransitionResult {
    Joined(ConnectionId),
    Applied,
    Missing,
}

enum UserTransitionOutcome {
    Join(JoinUserOutcome),
    Close {
        outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
    },
    Disconnect(DisconnectUsersOutcome),
}

impl Room {
    pub(crate) async fn add_user(
        &self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.join_session_with_cleanup(
            user_id,
            label,
            permissions,
            sender,
            UserCleanup::runtime(transport_adapter),
            true,
        )
        .await
    }

    /// Run the room-owned join transition and perform the deferred cleanup only
    /// after the state lock has been released.
    pub(in crate::runtime::room) async fn join_session_with_cleanup(
        &self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        cleanup: UserCleanup<'_>,
        emit_joined_fanout: bool,
    ) -> Result<ConnectionId, RoomJoinError> {
        let UserTransitionResult::Joined(connection_id) = self
            .run_session_transition(
                UserTransition::Join {
                    user_id: &user_id,
                    label,
                    permissions: permissions.into(),
                    sender,
                    emit_joined_fanout,
                },
                cleanup,
            )
            .await?
        else {
            return Err(RoomJoinError::RouterState);
        };
        Ok(connection_id)
    }

    pub(crate) async fn remove_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.run_session_transition(
            UserTransition::Close {
                user_id,
                connection_id,
            },
            UserCleanup::runtime(transport_adapter),
        )
        .await
        .is_ok_and(|result| !matches!(result, UserTransitionResult::Missing))
    }

    pub async fn broadcast(
        &self,
        sender_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) {
        let fanout = {
            let state = self.state.read().await;
            state.broadcast_fanout(sender_id, connection_id, message)
        };
        if let Some(fanout) = fanout {
            fanout.emit();
        }
    }

    pub async fn has_connection(&self, user_id: &UserId, connection_id: ConnectionId) -> bool {
        self.state.read().await.user_connection_id(user_id) == Some(connection_id)
    }

    pub(crate) async fn update_user_info_runtime_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.update_user_info_with_transport(
            user_id,
            connection_id,
            info,
            refresh,
            Some(transport_adapter),
        )
        .await;
    }

    async fn update_user_info_with_transport(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
        transport_adapter: Option<&RuntimeTransportAdapter>,
    ) {
        let need_refresh = refresh.is_needed();
        let outcome = {
            let mut state = self.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            if let Some(transport_adapter) = transport_adapter {
                self.sync_source_packet_selection_policy(
                    Some(transport_adapter),
                    transport_adapter,
                )
                .await;
            }
            outcome.emit();
        } else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                need_refresh,
                "user info update was rejected by room state"
            );
        }
    }

    pub(crate) async fn disconnect_sessions_runtime(
        &self,
        user_ids: &[UserId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.disconnect_users_with_cleanup(user_ids, UserCleanup::runtime(transport_adapter))
            .await;
    }

    pub(in crate::runtime::room) async fn disconnect_users_with_cleanup(
        &self,
        user_ids: &[UserId],
        cleanup: UserCleanup<'_>,
    ) {
        let _ = self
            .run_session_transition(UserTransition::Disconnect { user_ids }, cleanup)
            .await
            .ok();
    }

    pub(in crate::runtime::room) async fn cleanup_transport_removals(
        &self,
        cleanup: UserCleanup<'_>,
        removals: &[TransportMediaRemoval],
    ) {
        let Some(transport_adapter) = cleanup.transport_adapter() else {
            return;
        };
        if !cleanup.cleans_transport_state() {
            return;
        }
        for removal in removals {
            if transport_adapter
                .remove_media(
                    &self.transport_user_key(removal.user(), removal.connection()),
                    removal.transport_media(),
                )
                .await
                .is_err()
            {
                warn!(
                    user_id = ?removal.user(),
                    connection_id = ?removal.connection(),
                    transport_media_id = ?removal.transport_media(),
                    "transport adapter failed to remove transport media during room cleanup"
                );
            }
        }
    }

    async fn close_transport_user_for_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) {
        let Some(transport_adapter) = cleanup.transport_adapter() else {
            return;
        };
        if !cleanup.cleans_transport_state() {
            return;
        }
        self.close_transport_user(user_id, connection_id, transport_adapter)
            .await;
    }

    async fn close_transport_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        session_port: &impl SessionPort,
    ) {
        if session_port
            .close_session(&self.transport_user_key(user_id, connection_id))
            .await
            .is_err()
        {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                "transport adapter failed to close user during room cleanup"
            );
        }
    }

    async fn run_session_transition(
        &self,
        transition: UserTransition<'_>,
        cleanup: UserCleanup<'_>,
    ) -> Result<UserTransitionResult, RoomJoinError> {
        let Some(outcome) = self.apply_state_transition(transition).await? else {
            return Ok(UserTransitionResult::Missing);
        };
        Ok(self.finalize_session_transition(outcome, cleanup).await)
    }

    async fn apply_state_transition(
        &self,
        transition: UserTransition<'_>,
    ) -> Result<Option<UserTransitionOutcome>, RoomJoinError> {
        let outcome = match transition {
            UserTransition::Join {
                user_id,
                label,
                permissions,
                sender,
                emit_joined_fanout,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_join(user_id, label, permissions, sender, emit_joined_fanout)?
                };
                UserTransitionOutcome::Join(outcome)
            }
            UserTransition::Close {
                user_id,
                connection_id,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_leave(user_id, connection_id)
                };
                UserTransitionOutcome::Close {
                    outcome,
                    user_id: user_id.clone(),
                    connection_id,
                }
            }
            UserTransition::Disconnect { user_ids } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_disconnect_users(user_ids)
                };
                UserTransitionOutcome::Disconnect(outcome)
            }
        };
        Ok(Some(outcome))
    }

    async fn finalize_session_transition(
        &self,
        outcome: UserTransitionOutcome,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        match outcome {
            UserTransitionOutcome::Join(outcome) => {
                self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                    .await;
                if let Some(transport_adapter) = cleanup.transport_adapter() {
                    self.sync_source_packet_selection_policy(
                        Some(transport_adapter),
                        transport_adapter,
                    )
                    .await;
                }
                let connection_id = outcome.connection_id;
                let user_id = outcome.user_id.clone();
                Self::emit_lifecycle_effects(outcome.effects);
                self.diagnostics.record(
                    DiagnosticsEventData::for_user(
                        self.uuid(),
                        &user_id,
                        telemetry_event::USER_JOINED,
                    )
                    .with_connection_id(connection_id.as_u64())
                    .with_media_worker_id(self.media_worker_id()),
                );
                UserTransitionResult::Joined(connection_id)
            }
            UserTransitionOutcome::Close {
                outcome,
                user_id,
                connection_id,
            } => {
                let had_state = outcome.is_some();
                if let Some(outcome) = outcome {
                    self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                        .await;
                    Self::emit_lifecycle_effects(outcome.effects);
                }
                self.close_transport_user_for_cleanup(&user_id, connection_id, cleanup)
                    .await;
                if had_state {
                    self.diagnostics.record(
                        DiagnosticsEventData::for_user(
                            self.uuid(),
                            &user_id,
                            telemetry_event::USER_CLOSED,
                        )
                        .with_connection_id(connection_id.as_u64())
                        .with_media_worker_id(self.media_worker_id()),
                    );
                    self.diagnostics.forget_user(self.uuid(), &user_id);
                    if let Some(transport_adapter) = cleanup.transport_adapter() {
                        self.sync_source_packet_selection_policy(
                            Some(transport_adapter),
                            transport_adapter,
                        )
                        .await;
                    }
                }
                UserTransitionResult::Applied
            }
            UserTransitionOutcome::Disconnect(outcome) => {
                self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                    .await;
                for disconnected_session in &outcome.disconnected_users {
                    self.close_transport_user_for_cleanup(
                        &disconnected_session.user_id,
                        disconnected_session.connection_id,
                        cleanup,
                    )
                    .await;
                }
                for disconnected_session in &outcome.disconnected_users {
                    self.diagnostics.record(
                        DiagnosticsEventData::for_user(
                            self.uuid(),
                            &disconnected_session.user_id,
                            telemetry_event::USER_DISCONNECTED,
                        )
                        .with_media_worker_id(self.media_worker_id()),
                    );
                    self.diagnostics
                        .forget_user(self.uuid(), &disconnected_session.user_id);
                }
                if let Some(transport_adapter) = cleanup.transport_adapter() {
                    self.sync_source_packet_selection_policy(
                        Some(transport_adapter),
                        transport_adapter,
                    )
                    .await;
                }
                Self::emit_lifecycle_effects(outcome.effects);
                UserTransitionResult::Applied
            }
        }
    }

    pub(super) fn emit_lifecycle_effects(effects: LifecycleEffects) {
        for close_request in effects.close_requests {
            let _ = close_request
                .sender
                .send(UserOutbound::Close(close_request.reason));
        }
        for fanout in effects.fanouts {
            fanout.emit();
        }
    }

    /// Commit the answer-derived negotiated capability set for one live
    /// connection and trigger any consumer bootstrap that depends on it.
    pub async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &impl MediaPort,
    ) -> SessionNegotiationOutcome {
        let update = {
            let mut state = self.state.write().await;
            state.set_user_negotiated(user_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update(user_id, connection_id, update, media_port)
            .await
    }

    async fn apply_negotiation_update(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        update: UserNegotiationUpdate,
        media_port: &impl MediaPort,
    ) -> SessionNegotiationOutcome {
        if !update.session_present {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if update.became_consumer_ready {
            if !self
                .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_port)
                .await
            {
                return SessionNegotiationOutcome::StaleConnection;
            }
            self.request_active_video_consumer_keyframes(user_id, connection_id, media_port)
                .await;
        }
        SessionNegotiationOutcome::Applied
    }

    pub(crate) async fn apply_session_refreshed(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) -> SessionNegotiationOutcome {
        if !self
            .request_active_video_consumer_keyframes(user_id, connection_id, media_port)
            .await
        {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if !self
            .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_port)
            .await
        {
            return SessionNegotiationOutcome::StaleConnection;
        }
        SessionNegotiationOutcome::Applied
    }

    async fn request_active_video_consumer_keyframes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) -> bool {
        let Some(keyframe_refresh_targets) = ({
            let state = self.state.read().await;
            state.active_video_consumer_keyframe_refresh_targets(user_id, connection_id)
        }) else {
            return false;
        };
        for target in keyframe_refresh_targets {
            if media_port
                .request_consumer_keyframe(
                    &self.transport_user_key(user_id, connection_id),
                    target.consumer_media(),
                    &self.transport_user_key(
                        target.producer_user_id(),
                        target.producer_connection_id(),
                    ),
                    target.source_media(),
                )
                .await
                .is_err()
            {
                warn!(
                    ?user_id,
                    connection_id = ?connection_id,
                    producer_user_id = ?target.producer_user_id(),
                    source_transport_media_id = ?target.source_media(),
                    "transport adapter failed to request a refreshed consumer keyframe"
                );
            }
        }
        true
    }

    pub(super) async fn user_count(&self) -> usize {
        self.state.read().await.user_count()
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
