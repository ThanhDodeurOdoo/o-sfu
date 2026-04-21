use o_sfu_protocol::shared::{SessionId, SessionInfo, SessionPermissions};
use o_sfu_protocol::signaling::RecordingOptions;
use tokio::sync::mpsc;

use crate::runtime::ConnectionId;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;

use super::super::super::{Channel, ChannelJoinError, SessionCleanup, SessionOutbound};

#[derive(Clone, Copy)]
pub(crate) struct ChannelTestLifecycle<'a> {
    pub(super) channel: &'a Channel,
}

impl ChannelTestLifecycle<'_> {
    pub(crate) async fn join_session(
        self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
    ) -> Result<ConnectionId, ChannelJoinError> {
        self.channel
            .join_session_with_cleanup(
                session_id,
                label,
                permissions,
                sender,
                SessionCleanup::state_only(None),
                false,
            )
            .await
    }

    pub(crate) async fn join_session_without_transport_cleanup(
        self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<ConnectionId, ChannelJoinError> {
        self.channel
            .join_session_with_cleanup(
                session_id,
                label,
                permissions,
                sender,
                SessionCleanup::state_only(Some(transport_adapter)),
                false,
            )
            .await
    }

    pub(crate) async fn leave_session(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> bool {
        self.leave_session_with_cleanup(session_id, connection_id, SessionCleanup::state_only(None))
            .await
    }

    pub(crate) async fn leave_session_runtime(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.leave_session_with_cleanup(
            session_id,
            connection_id,
            SessionCleanup::runtime(transport_adapter),
        )
        .await
    }

    pub(crate) async fn leave_session_without_transport_cleanup(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.leave_session_with_cleanup(
            session_id,
            connection_id,
            SessionCleanup::state_only(Some(transport_adapter)),
        )
        .await
    }

    async fn leave_session_with_cleanup(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        cleanup: SessionCleanup<'_>,
    ) -> bool {
        let channel = self.channel;
        let outcome = {
            let mut state = channel.state.write().await;
            state.apply_leave(session_id, connection_id)
        };
        let Some(outcome) = outcome else {
            return false;
        };
        channel
            .cleanup_transport_removals(cleanup, &outcome.transport_removals)
            .await;
        if let Some(transport_adapter) = cleanup.transport_adapter() {
            channel
                .sync_source_packet_selection_policy(Some(transport_adapter), transport_adapter)
                .await;
        }
        Channel::emit_lifecycle_effects(outcome.effects);
        true
    }

    pub(crate) async fn broadcast(self, sender_id: &SessionId, message: serde_json::Value) {
        let Some(connection_id) = self
            .channel
            .state
            .read()
            .await
            .session_connection_id(sender_id)
        else {
            return;
        };
        self.channel
            .broadcast_runtime(sender_id, connection_id, message)
            .await;
    }

    pub(crate) async fn update_session_info(
        self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
    ) {
        let Some(connection_id) = self
            .channel
            .state
            .read()
            .await
            .session_connection_id(session_id)
        else {
            return;
        };
        let outcome = {
            let mut state = self.channel.state.write().await;
            state.apply_presence_update(session_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            outcome.emit();
        }
    }

    pub(crate) async fn update_session_info_runtime(
        self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self
            .channel
            .test_api()
            .inspect()
            .session_connection_id(session_id)
            .await
        else {
            return;
        };
        self.channel
            .update_session_info_runtime_for_connection(
                session_id,
                connection_id,
                info,
                need_refresh,
                transport_adapter,
            )
            .await;
    }

    pub(crate) async fn disconnect_sessions(self, session_ids: &[SessionId]) {
        self.channel
            .disconnect_sessions_with_cleanup(session_ids, SessionCleanup::state_only(None))
            .await;
    }

    pub(crate) async fn disconnect_sessions_without_transport_cleanup(
        self,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.channel
            .disconnect_sessions_with_cleanup(
                session_ids,
                SessionCleanup::state_only(Some(transport_adapter)),
            )
            .await;
    }

    pub(crate) async fn start_recording(
        self,
        session_id: &SessionId,
        options: RecordingOptions,
    ) -> bool {
        let Some(connection_id) = self
            .channel
            .test_api()
            .inspect()
            .session_connection_id(session_id)
            .await
        else {
            self.channel.metrics.record_recording_start_rejected();
            return false;
        };
        self.channel
            .start_recording_runtime(session_id, connection_id, options)
            .await
    }

    pub(crate) async fn stop_recording(self, session_id: &SessionId) -> bool {
        let Some(connection_id) = self
            .channel
            .test_api()
            .inspect()
            .session_connection_id(session_id)
            .await
        else {
            self.channel.metrics.record_recording_stop_rejected();
            return false;
        };
        self.channel
            .stop_recording_runtime(session_id, connection_id)
            .await
    }
}
