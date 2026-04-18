use o_sfu_router::MediaCapabilities;
use tokio::sync::mpsc;
use tracing::warn;

use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use o_sfu_protocol::shared::{SessionId, SessionInfo, SessionPermissions};

use super::{
    Channel, ChannelJoinError, ChannelSessionPermissions, SessionOutbound,
    session_negotiation::SessionNegotiationUpdate,
    state::{
        DisconnectSessionsOutcome, JoinSessionOutcome, LeaveSessionOutcome, LifecycleEffects,
        TransportMediaRemoval,
    },
};
#[derive(Clone, Copy)]
pub(in crate::runtime::channel) struct SessionCleanup<'a> {
    transport_adapter: Option<&'a RuntimeTransportAdapter>,
    remove_transport_media: bool,
}

impl<'a> SessionCleanup<'a> {
    pub(in crate::runtime::channel) const fn runtime(
        transport_adapter: &'a RuntimeTransportAdapter,
    ) -> Self {
        Self {
            transport_adapter: Some(transport_adapter),
            remove_transport_media: true,
        }
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) const fn state_only(
        transport_adapter: Option<&'a RuntimeTransportAdapter>,
    ) -> Self {
        Self {
            transport_adapter,
            remove_transport_media: false,
        }
    }

    pub(in crate::runtime::channel) const fn transport_adapter(
        self,
    ) -> Option<&'a RuntimeTransportAdapter> {
        self.transport_adapter
    }

    const fn removes_transport_media(self) -> bool {
        self.remove_transport_media
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
    Close {
        outcome: Option<LeaveSessionOutcome>,
        session_id: SessionId,
        connection_id: u64,
    },
    Disconnect(DisconnectSessionsOutcome),
}

impl Channel {
    pub(crate) async fn join_session_runtime(
        &self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<u64, ChannelJoinError> {
        self.join_session_with_cleanup(
            session_id,
            label,
            permissions,
            sender,
            SessionCleanup::runtime(transport_adapter),
            true,
        )
        .await
    }

    /// Run the room-owned join transition and perform the deferred cleanup only
    /// after the state lock has been released.
    pub(in crate::runtime::channel) async fn join_session_with_cleanup(
        &self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        cleanup: SessionCleanup<'_>,
        emit_joined_fanout: bool,
    ) -> Result<u64, ChannelJoinError> {
        let SessionTransitionResult::Joined(connection_id) = self
            .run_session_transition(
                SessionTransition::Join {
                    session_id: &session_id,
                    label,
                    permissions: permissions.into(),
                    sender,
                    emit_joined_fanout,
                },
                cleanup,
            )
            .await?
        else {
            return Err(ChannelJoinError::RouterState);
        };
        Ok(connection_id)
    }

    pub(crate) async fn close_session_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.run_session_transition(
            SessionTransition::Close {
                session_id,
                connection_id,
            },
            SessionCleanup::runtime(transport_adapter),
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

    pub(crate) async fn has_connection(&self, session_id: &SessionId, connection_id: u64) -> bool {
        self.state.read().await.session_connection_id(session_id) == Some(connection_id)
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

    pub(crate) async fn disconnect_sessions_runtime(
        &self,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.disconnect_sessions_with_cleanup(
            session_ids,
            SessionCleanup::runtime(transport_adapter),
        )
        .await;
    }

    pub(in crate::runtime::channel) async fn disconnect_sessions_with_cleanup(
        &self,
        session_ids: &[SessionId],
        cleanup: SessionCleanup<'_>,
    ) {
        let _ = self
            .run_session_transition(SessionTransition::Disconnect { session_ids }, cleanup)
            .await
            .ok();
    }

    pub(in crate::runtime::channel) async fn cleanup_transport_removals(
        &self,
        cleanup: SessionCleanup<'_>,
        removals: &[TransportMediaRemoval],
    ) {
        let Some(transport_adapter) = cleanup.transport_adapter() else {
            return;
        };
        if !cleanup.removes_transport_media() {
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
        cleanup: SessionCleanup<'_>,
    ) -> Result<SessionTransitionResult, ChannelJoinError> {
        let Some(outcome) = self.apply_state_transition(transition).await? else {
            return Ok(SessionTransitionResult::Missing);
        };
        Ok(self.finalize_session_transition(outcome, cleanup).await)
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
        cleanup: SessionCleanup<'_>,
    ) -> SessionTransitionResult {
        match outcome {
            SessionTransitionOutcome::Join(outcome) => {
                self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                    .await;
                self.sync_source_packet_selection_policy(cleanup.transport_adapter())
                    .await;
                let connection_id = outcome.connection_id;
                Self::emit_lifecycle_effects(outcome.effects);
                SessionTransitionResult::Joined(connection_id)
            }
            SessionTransitionOutcome::Close {
                outcome,
                session_id,
                connection_id,
            } => {
                let had_state = outcome.is_some();
                if let Some(outcome) = outcome {
                    self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                        .await;
                    Self::emit_lifecycle_effects(outcome.effects);
                }
                if let Some(transport_adapter) = cleanup.transport_adapter() {
                    self.close_transport_session(&session_id, connection_id, transport_adapter)
                        .await;
                }
                if had_state {
                    self.sync_source_packet_selection_policy(cleanup.transport_adapter())
                        .await;
                }
                SessionTransitionResult::Applied
            }
            SessionTransitionOutcome::Disconnect(outcome) => {
                self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                    .await;
                self.sync_source_packet_selection_policy(cleanup.transport_adapter())
                    .await;
                Self::emit_lifecycle_effects(outcome.effects);
                SessionTransitionResult::Applied
            }
        }
    }

    pub(super) fn emit_lifecycle_effects(effects: LifecycleEffects) {
        for close_request in effects.close_requests {
            let _ = close_request
                .sender
                .send(SessionOutbound::Close(close_request.reason));
        }
        for fanout in effects.fanouts {
            fanout.emit();
        }
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

    pub(super) async fn session_count(&self) -> usize {
        self.state.read().await.session_count()
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
