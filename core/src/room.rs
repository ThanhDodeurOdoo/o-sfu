use o_sfu_router::MediaCapabilities;

use crate::{
    ConnectionId,
    runtime::{DownloadStates, StreamType, UserId, UserInfo},
    transport::{AppliedSessionAnswer, MediaPort, ObservabilityPort, TransportSessionKey},
};

#[derive(Debug, Clone)]
pub struct MediaSessionContext<'a> {
    user_id: &'a UserId,
    connection_id: ConnectionId,
    transport_user_key: TransportSessionKey,
}

impl<'a> MediaSessionContext<'a> {
    #[must_use]
    pub const fn new(
        user_id: &'a UserId,
        connection_id: ConnectionId,
        transport_user_key: TransportSessionKey,
    ) -> Self {
        Self {
            user_id,
            connection_id,
            transport_user_key,
        }
    }

    #[must_use]
    pub const fn user_id(&self) -> &'a UserId {
        self.user_id
    }

    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    #[must_use]
    pub const fn transport_user_key(&self) -> &TransportSessionKey {
        &self.transport_user_key
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationActivity {
    Active,
    Inactive,
}

impl PublicationActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserInfoRefresh {
    Needed,
    NotNeeded,
}

impl UserInfoRefresh {
    #[must_use]
    pub const fn from_needed(needed: bool) -> Self {
        if needed {
            Self::Needed
        } else {
            Self::NotNeeded
        }
    }

    #[must_use]
    pub const fn is_needed(self) -> bool {
        matches!(self, Self::Needed)
    }
}

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

    fn media_session_context<'a>(
        &self,
        user_id: &'a UserId,
        connection_id: ConnectionId,
    ) -> MediaSessionContext<'a> {
        MediaSessionContext::new(
            user_id,
            connection_id,
            self.transport_user_key(user_id, connection_id),
        )
    }

    async fn router_rtp_capabilities(&self) -> MediaCapabilities;

    async fn apply_session_negotiated(
        &self,
        session: &MediaSessionContext<'_>,
        capabilities: MediaCapabilities,
        media_port: &T,
    ) -> bool;

    async fn apply_session_refreshed(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &T,
    ) -> bool;

    async fn has_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
    ) -> bool;

    async fn is_stream_published(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
    ) -> bool;

    async fn set_publication_active(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        activity: PublicationActivity,
        media_port: &T,
    );

    async fn update_subscription(
        &self,
        session: &MediaSessionContext<'_>,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &T,
    );

    async fn update_user_info(
        &self,
        session: &MediaSessionContext<'_>,
        info: UserInfo,
        refresh: UserInfoRefresh,
        media_port: &T,
    );

    async fn stage_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &T,
    ) -> bool;

    async fn rollback_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &T,
    ) -> bool;

    async fn rollback_connection_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &T,
    );

    async fn commit_staged_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        applied_answer: &AppliedSessionAnswer,
        media_port: &T,
    );

    async fn unpublish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &T,
    ) -> bool;
}
