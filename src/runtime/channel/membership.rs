use o_sfu_router::MediaCapabilities;
use tokio::sync::mpsc;
use tracing::warn;

use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use o_sfu_protocol::shared::{SessionId, SessionInfo, SessionPermissions};

use super::{
    Channel, ChannelJoinError, ChannelSessionPermissions, SessionOutbound,
    session_negotiation::{SessionNegotiationUpdate, SessionTransportReady},
    state::{
        DisconnectSessionsOutcome, JoinSessionOutcome, LeaveSessionOutcome, LifecycleEffects,
        TransportMediaRemoval,
    },
};
#[cfg(test)]
use crate::runtime::transport_adapter::TransportMediaId;
#[cfg(test)]
use o_sfu_protocol::shared::StreamType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionCleanupPolicy {
    StateOnly,
    StateAndTransportMedia,
}

impl SessionCleanupPolicy {
    const fn removes_transport_media(self) -> bool {
        matches!(self, Self::StateAndTransportMedia)
    }
}

enum SessionTransition<'a> {
    Join {
        session_id: &'a SessionId,
        label: Option<String>,
        permissions: ChannelSessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        emit_joined_fanout: bool,
    },
    Leave {
        session_id: &'a SessionId,
        connection_id: u64,
    },
    Close {
        session_id: &'a SessionId,
        connection_id: u64,
    },
    Disconnect {
        session_ids: &'a [SessionId],
    },
}

enum SessionTransitionResult {
    Joined(u64),
    Applied,
    Missing,
}

enum SessionTransitionOutcome {
    Join(JoinSessionOutcome),
    Leave(LeaveSessionOutcome),
    Close {
        outcome: Option<LeaveSessionOutcome>,
        session_id: SessionId,
        connection_id: u64,
    },
    Disconnect(DisconnectSessionsOutcome),
}

impl Channel {
    #[cfg(test)]
    pub async fn join_session(
        &self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
    ) -> Result<u64, ChannelJoinError> {
        self.join_session_with_cleanup(
            session_id,
            label,
            permissions,
            sender,
            None,
            SessionCleanupPolicy::StateOnly,
        )
        .await
    }

    pub(crate) async fn join_session_runtime(
        &self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        transport_adapter: &RuntimeTransportAdapter,
        cleanup_policy: SessionCleanupPolicy,
    ) -> Result<u64, ChannelJoinError> {
        self.join_session_with_cleanup(
            session_id,
            label,
            permissions,
            sender,
            Some(transport_adapter),
            cleanup_policy,
        )
        .await
    }

    /// Run the room-owned join transition and perform the deferred cleanup only
    /// after the state lock has been released.
    async fn join_session_with_cleanup(
        &self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        transport_adapter: Option<&RuntimeTransportAdapter>,
        cleanup_policy: SessionCleanupPolicy,
    ) -> Result<u64, ChannelJoinError> {
        let SessionTransitionResult::Joined(connection_id) = self
            .run_session_transition(
                SessionTransition::Join {
                    session_id: &session_id,
                    label,
                    permissions: permissions.into(),
                    sender,
                    emit_joined_fanout: cleanup_policy.removes_transport_media(),
                },
                transport_adapter,
                cleanup_policy,
            )
            .await?
        else {
            return Err(ChannelJoinError::RouterState);
        };
        Ok(connection_id)
    }

    #[cfg(test)]
    pub async fn leave_session(&self, session_id: &SessionId, connection_id: u64) -> bool {
        self.leave_session_with_cleanup(
            session_id,
            connection_id,
            None,
            SessionCleanupPolicy::StateOnly,
        )
        .await
    }

    pub(crate) async fn leave_session_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
        cleanup_policy: SessionCleanupPolicy,
    ) -> bool {
        self.leave_session_with_cleanup(
            session_id,
            connection_id,
            Some(transport_adapter),
            cleanup_policy,
        )
        .await
    }

    pub(crate) async fn close_session_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
        cleanup_policy: SessionCleanupPolicy,
    ) -> bool {
        self.run_session_transition(
            SessionTransition::Close {
                session_id,
                connection_id,
            },
            Some(transport_adapter),
            cleanup_policy,
        )
        .await
        .is_ok_and(|result| !matches!(result, SessionTransitionResult::Missing))
    }

    async fn leave_session_with_cleanup(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: Option<&RuntimeTransportAdapter>,
        cleanup_policy: SessionCleanupPolicy,
    ) -> bool {
        self.run_session_transition(
            SessionTransition::Leave {
                session_id,
                connection_id,
            },
            transport_adapter,
            cleanup_policy,
        )
        .await
        .is_ok_and(|result| !matches!(result, SessionTransitionResult::Missing))
    }

    pub(crate) async fn broadcast_runtime(
        &self,
        sender_id: &SessionId,
        connection_id: u64,
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

    #[cfg(test)]
    pub async fn broadcast(&self, sender_id: &SessionId, message: serde_json::Value) {
        let Some(connection_id) = self.session_connection_id(sender_id).await else {
            return;
        };
        self.broadcast_runtime(sender_id, connection_id, message)
            .await;
    }

    pub(crate) async fn has_connection(&self, session_id: &SessionId, connection_id: u64) -> bool {
        self.state.read().await.session_connection_id(session_id) == Some(connection_id)
    }

    #[cfg(test)]
    pub async fn update_session_info(
        &self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
    ) {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            return;
        };
        self.update_session_info_with_transport(
            session_id,
            connection_id,
            info,
            need_refresh,
            None,
        )
        .await;
    }

    pub(crate) async fn update_session_info_runtime_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        info: SessionInfo,
        need_refresh: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.update_session_info_with_transport(
            session_id,
            connection_id,
            info,
            need_refresh,
            Some(transport_adapter),
        )
        .await;
    }

    #[cfg(test)]
    pub(crate) async fn update_session_info_runtime(
        &self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            return;
        };
        self.update_session_info_runtime_for_connection(
            session_id,
            connection_id,
            info,
            need_refresh,
            transport_adapter,
        )
        .await;
    }

    async fn update_session_info_with_transport(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        info: SessionInfo,
        need_refresh: bool,
        transport_adapter: Option<&RuntimeTransportAdapter>,
    ) {
        let outcome = {
            let mut state = self.state.write().await;
            state.apply_presence_update(session_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            self.sync_source_packet_selection_policy(transport_adapter)
                .await;
            outcome.emit();
        } else {
            warn!(
                ?session_id,
                connection_id,
                ?info,
                need_refresh,
                "session info update was rejected by channel state"
            );
        }
    }

    #[cfg(test)]
    pub async fn disconnect_sessions(&self, session_ids: &[SessionId]) {
        self.disconnect_sessions_with_cleanup(session_ids, None, SessionCleanupPolicy::StateOnly)
            .await;
    }

    pub(crate) async fn disconnect_sessions_runtime(
        &self,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
        cleanup_policy: SessionCleanupPolicy,
    ) {
        self.disconnect_sessions_with_cleanup(session_ids, Some(transport_adapter), cleanup_policy)
            .await;
    }

    async fn disconnect_sessions_with_cleanup(
        &self,
        session_ids: &[SessionId],
        transport_adapter: Option<&RuntimeTransportAdapter>,
        cleanup_policy: SessionCleanupPolicy,
    ) {
        let _ = self
            .run_session_transition(
                SessionTransition::Disconnect { session_ids },
                transport_adapter,
                cleanup_policy,
            )
            .await
            .ok();
    }

    pub(super) async fn cleanup_transport_removals(
        &self,
        transport_adapter: Option<&RuntimeTransportAdapter>,
        removals: &[TransportMediaRemoval],
        cleanup_policy: SessionCleanupPolicy,
    ) {
        let Some(transport_adapter) = transport_adapter else {
            return;
        };
        if !cleanup_policy.removes_transport_media() {
            return;
        }
        for removal in removals {
            if transport_adapter
                .remove_media(
                    &self.transport_session_key(removal.session(), removal.connection()),
                    removal.transport_media(),
                )
                .await
                .is_err()
            {
                warn!(
                    session_id = ?removal.session(),
                    connection_id = removal.connection(),
                    transport_media_id = ?removal.transport_media(),
                    "transport adapter failed to remove transport media during channel cleanup"
                );
            }
        }
    }

    async fn close_transport_session(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        if transport_adapter
            .close_session(&self.transport_session_key(session_id, connection_id))
            .await
            .is_err()
        {
            warn!(
                ?session_id,
                connection_id, "transport adapter failed to close session during channel cleanup"
            );
        }
    }

    async fn run_session_transition(
        &self,
        transition: SessionTransition<'_>,
        transport_adapter: Option<&RuntimeTransportAdapter>,
        cleanup_policy: SessionCleanupPolicy,
    ) -> Result<SessionTransitionResult, ChannelJoinError> {
        let Some(outcome) = self.apply_state_transition(transition).await? else {
            return Ok(SessionTransitionResult::Missing);
        };
        Ok(self
            .finalize_session_transition(outcome, transport_adapter, cleanup_policy)
            .await)
    }

    async fn apply_state_transition(
        &self,
        transition: SessionTransition<'_>,
    ) -> Result<Option<SessionTransitionOutcome>, ChannelJoinError> {
        let outcome = match transition {
            SessionTransition::Join {
                session_id,
                label,
                permissions,
                sender,
                emit_joined_fanout,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_join(session_id, label, permissions, sender, emit_joined_fanout)?
                };
                SessionTransitionOutcome::Join(outcome)
            }
            SessionTransition::Leave {
                session_id,
                connection_id,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_leave(session_id, connection_id)
                };
                let Some(outcome) = outcome else {
                    return Ok(None);
                };
                SessionTransitionOutcome::Leave(outcome)
            }
            SessionTransition::Close {
                session_id,
                connection_id,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_leave(session_id, connection_id)
                };
                SessionTransitionOutcome::Close {
                    outcome,
                    session_id: session_id.clone(),
                    connection_id,
                }
            }
            SessionTransition::Disconnect { session_ids } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_disconnect_sessions(session_ids)
                };
                SessionTransitionOutcome::Disconnect(outcome)
            }
        };
        Ok(Some(outcome))
    }

    async fn finalize_session_transition(
        &self,
        outcome: SessionTransitionOutcome,
        transport_adapter: Option<&RuntimeTransportAdapter>,
        cleanup_policy: SessionCleanupPolicy,
    ) -> SessionTransitionResult {
        match outcome {
            SessionTransitionOutcome::Join(outcome) => {
                self.cleanup_transport_removals(
                    transport_adapter,
                    &outcome.transport_removals,
                    cleanup_policy,
                )
                .await;
                self.sync_source_packet_selection_policy(transport_adapter)
                    .await;
                let connection_id = outcome.connection_id;
                Self::emit_lifecycle_effects(outcome.effects);
                SessionTransitionResult::Joined(connection_id)
            }
            SessionTransitionOutcome::Leave(outcome) => {
                self.cleanup_transport_removals(
                    transport_adapter,
                    &outcome.transport_removals,
                    cleanup_policy,
                )
                .await;
                self.sync_source_packet_selection_policy(transport_adapter)
                    .await;
                Self::emit_lifecycle_effects(outcome.effects);
                SessionTransitionResult::Applied
            }
            SessionTransitionOutcome::Close {
                outcome,
                session_id,
                connection_id,
            } => {
                let had_state = outcome.is_some();
                if let Some(outcome) = outcome {
                    self.cleanup_transport_removals(
                        transport_adapter,
                        &outcome.transport_removals,
                        cleanup_policy,
                    )
                    .await;
                    Self::emit_lifecycle_effects(outcome.effects);
                }
                if let Some(transport_adapter) = transport_adapter {
                    self.close_transport_session(&session_id, connection_id, transport_adapter)
                        .await;
                }
                if had_state {
                    self.sync_source_packet_selection_policy(transport_adapter)
                        .await;
                }
                SessionTransitionResult::Applied
            }
            SessionTransitionOutcome::Disconnect(outcome) => {
                self.cleanup_transport_removals(
                    transport_adapter,
                    &outcome.transport_removals,
                    cleanup_policy,
                )
                .await;
                self.sync_source_packet_selection_policy(transport_adapter)
                    .await;
                Self::emit_lifecycle_effects(outcome.effects);
                SessionTransitionResult::Applied
            }
        }
    }

    fn emit_lifecycle_effects(effects: LifecycleEffects) {
        for close_request in effects.close_requests {
            let _ = close_request
                .sender
                .send(SessionOutbound::Close(close_request.reason));
        }
        for fanout in effects.fanouts {
            fanout.emit();
        }
    }

    /// Record the browser-advertised RTP capabilities for one live connection
    /// and trigger any consumer bootstrap that becomes newly legal.
    pub(crate) async fn apply_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: MediaCapabilities,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.state.write().await;
            state.set_client_rtp_capabilities(session_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update(session_id, connection_id, update, transport_adapter)
            .await
    }

    async fn apply_transport_ready(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        readiness: SessionTransportReady,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.state.write().await;
            state.set_transport_ready(session_id, connection_id, readiness)
        };
        self.apply_negotiation_update(session_id, connection_id, update, transport_adapter)
            .await
    }

    pub(crate) async fn apply_publish_transport_ready(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_transport_ready(
            session_id,
            connection_id,
            SessionTransportReady::Publish,
            transport_adapter,
        )
        .await
    }

    pub(crate) async fn apply_consume_transport_ready(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_transport_ready(
            session_id,
            connection_id,
            SessionTransportReady::Consume,
            transport_adapter,
        )
        .await
    }

    /// Commit the answer-derived negotiated capability set for one live
    /// connection and trigger any consumer bootstrap that depends on it.
    pub(crate) async fn apply_session_negotiated(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: MediaCapabilities,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.state.write().await;
            state.set_session_negotiated(session_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update(session_id, connection_id, update, transport_adapter)
            .await
    }

    async fn apply_negotiation_update(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        update: SessionNegotiationUpdate,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        if !update.session_present {
            return false;
        }
        if update.became_consumer_ready {
            return self
                .bootstrap_missing_consumers_for_connection(
                    session_id,
                    connection_id,
                    transport_adapter,
                )
                .await;
        }
        true
    }

    pub(crate) async fn apply_session_refreshed(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.bootstrap_missing_consumers_for_connection(
            session_id,
            connection_id,
            transport_adapter,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn set_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
        capabilities: MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let mut state = self.state.write().await;
        let connection_id = state.session_connection_id(session_id).unwrap_or(u64::MAX);
        state.set_client_rtp_capabilities(session_id, connection_id, &capabilities)
    }

    #[cfg(test)]
    pub(super) async fn set_publish_transport_ready(
        &self,
        session_id: &SessionId,
    ) -> SessionNegotiationUpdate {
        let mut state = self.state.write().await;
        let connection_id = state.session_connection_id(session_id).unwrap_or(u64::MAX);
        state.set_transport_ready(session_id, connection_id, SessionTransportReady::Publish)
    }

    #[cfg(test)]
    pub(super) async fn set_consume_transport_ready(
        &self,
        session_id: &SessionId,
    ) -> SessionNegotiationUpdate {
        let mut state = self.state.write().await;
        let connection_id = state.session_connection_id(session_id).unwrap_or(u64::MAX);
        state.set_transport_ready(session_id, connection_id, SessionTransportReady::Consume)
    }

    pub(super) async fn session_count(&self) -> usize {
        self.state.read().await.session_count()
    }

    #[cfg(test)]
    pub(super) async fn router_session_count(&self) -> usize {
        let (count, _camera_count, _screen_count) = self.state.read().await.session_stats_counts();
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    pub(super) async fn router_session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.state.read().await.session_permissions(session_id)
    }

    #[cfg(test)]
    pub(super) async fn session_has_parsed_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> bool {
        self.state
            .read()
            .await
            .session_has_parsed_client_rtp_capabilities(session_id)
    }

    #[cfg(test)]
    pub(crate) async fn parsed_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::MediaCapabilities> {
        self.state
            .read()
            .await
            .parsed_client_rtp_capabilities(session_id)
    }

    #[cfg(test)]
    pub(crate) async fn session_connection_id(&self, session_id: &SessionId) -> Option<u64> {
        self.state.read().await.session_connection_id(session_id)
    }

    #[cfg(test)]
    pub(super) async fn producer_count(&self) -> usize {
        self.state.read().await.producer_count()
    }

    #[cfg(test)]
    pub(super) async fn consumer_count(&self) -> usize {
        self.state.read().await.consumer_count()
    }

    #[cfg(test)]
    pub(super) async fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.state.read().await.first_published_transport_media_id()
    }

    #[cfg(test)]
    pub(super) async fn producer_transport_media_id(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.state
            .read()
            .await
            .producer_transport_media_id(session_id, connection_id, stream_type)
    }

    #[cfg(test)]
    pub(super) async fn has_producer_route_target(
        &self,
        owner_session_id: &SessionId,
        owner_connection_id: u64,
        stream_type: StreamType,
    ) -> bool {
        self.state
            .read()
            .await
            .producer_route_target(owner_session_id, owner_connection_id, stream_type)
            .is_some()
    }

    #[cfg(test)]
    pub(super) async fn producer_stream_type_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.state
            .read()
            .await
            .producer_stream_type_for_transport_media_id(transport_media_id)
    }

    #[cfg(test)]
    pub(super) async fn session_info_snapshot(
        &self,
        session_id: &SessionId,
    ) -> Option<(SessionId, SessionInfo)> {
        self.state.read().await.session_info_snapshot(session_id)
    }

    #[cfg(test)]
    pub(super) async fn has_session(&self, session_id: &SessionId) -> bool {
        self.state.read().await.has_session(session_id)
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
