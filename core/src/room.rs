use o_sfu_protocol::shared::{DownloadStates, StreamType, UserId, UserInfo};
use o_sfu_router::MediaCapabilities;

use crate::{
    ConnectionId,
    transport::{AppliedSessionAnswer, MediaPort, ObservabilityPort, TransportSessionKey},
};

#[allow(
    async_fn_in_trait,
    reason = "media room calls are static-dispatch bridges from the server room into the media core facade"
)]
pub trait MediaRoom<T>
where
    T: MediaPort + ObservabilityPort + Send + Sync,
{
    fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey;

    async fn router_rtp_capabilities(&self) -> MediaCapabilities;

    async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &T,
    ) -> bool;

    async fn apply_session_refreshed(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &T,
    ) -> bool;

    async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool;

    async fn is_stream_published(&self, user_id: &UserId, stream_type: StreamType) -> bool;

    async fn set_publication_active(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        active: bool,
        media_port: &T,
    );

    async fn update_subscription(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &T,
    );

    async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        need_refresh: bool,
        media_port: &T,
    );

    async fn stage_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &T,
    ) -> bool;

    async fn rollback_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &T,
    ) -> bool;

    async fn rollback_connection_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &T,
    );

    async fn commit_staged_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        applied_answer: &AppliedSessionAnswer,
        media_port: &T,
    );

    async fn unpublish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &T,
    ) -> bool;
}
