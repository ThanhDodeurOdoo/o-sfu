use o_sfu_protocol::shared::{DownloadStates, StreamType, UserId, UserInfo};
use o_sfu_router::MediaCapabilities;

use super::Room;
use crate::{
    MediaRoom,
    runtime::{ConnectionId, transport_adapter::RuntimeTransportAdapter},
    transport::{AppliedSessionAnswer, TransportSessionKey},
};

impl MediaRoom<RuntimeTransportAdapter> for Room {
    fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.transport_user_key(user_id, connection_id)
    }

    async fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.router_rtp_capabilities().await
    }

    async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_session_negotiated(user_id, connection_id, capabilities, media_port)
            .await
    }

    async fn apply_session_refreshed(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_session_refreshed(user_id, connection_id, media_port)
            .await
    }

    async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.has_staged_publish(user_id, connection_id, stream_type)
            .await
    }

    async fn is_stream_published(&self, user_id: &UserId, stream_type: StreamType) -> bool {
        self.is_stream_published(user_id, stream_type).await
    }

    async fn set_publication_active(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        active: bool,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.set_publication_active_runtime(
            user_id,
            connection_id,
            stream_type,
            active,
            media_port,
        )
        .await;
    }

    async fn update_subscription(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.update_subscription_runtime(
            user_id,
            connection_id,
            target_user_id,
            states,
            media_port,
        )
        .await;
    }

    async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        need_refresh: bool,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.update_user_info_runtime_for_connection(
            user_id,
            connection_id,
            info,
            need_refresh,
            media_port,
        )
        .await;
    }

    async fn stage_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> bool {
        self.stage_negotiated_publish(user_id, connection_id, stream_type, media_port)
            .await
    }

    async fn rollback_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> bool {
        self.rollback_staged_publish(user_id, connection_id, stream_type, media_port)
            .await
    }

    async fn rollback_connection_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.rollback_staged_publishes_for_connection(user_id, connection_id, media_port)
            .await;
    }

    async fn commit_staged_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        applied_answer: &AppliedSessionAnswer,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.commit_staged_publishes(
            user_id,
            connection_id,
            applied_answer,
            media_port,
            media_port,
        )
        .await;
    }

    async fn unpublish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> bool {
        self.unpublish_track(user_id, connection_id, stream_type, media_port)
            .await
    }
}
