use o_sfu_router::MediaCapabilities;

use super::Room;
use crate::{
    MediaRoom, MediaSessionContext, PublicationActivity, PublicationActivityOutcome,
    PublishStageOutcome, RollbackStagedPublishOutcome, SessionNegotiationOutcome,
    SubscriptionUpdateOutcome, UnpublishOutcome, UserInfoRefresh,
    runtime::{
        ConnectionId, DownloadStates, StreamType, UserId, UserInfo,
        transport_adapter::RuntimeTransportAdapter,
    },
    transport::{AppliedSessionAnswer, TransportAdapterError, TransportSessionKey},
};

struct RoomMediaSession<'a, 'session> {
    room: &'a Room,
    context: &'a MediaSessionContext<'session>,
    media_port: &'a RuntimeTransportAdapter,
}

impl<'a, 'session> RoomMediaSession<'a, 'session> {
    const fn new(
        room: &'a Room,
        context: &'a MediaSessionContext<'session>,
        media_port: &'a RuntimeTransportAdapter,
    ) -> Self {
        Self {
            room,
            context,
            media_port,
        }
    }

    const fn user_id(&self) -> &UserId {
        self.context.user_id()
    }

    const fn connection_id(&self) -> ConnectionId {
        self.context.connection_id()
    }

    async fn apply_negotiated(self, capabilities: MediaCapabilities) -> SessionNegotiationOutcome {
        self.room
            .apply_session_negotiated(
                self.user_id(),
                self.connection_id(),
                capabilities,
                self.media_port,
            )
            .await
    }

    async fn apply_refreshed(self) -> SessionNegotiationOutcome {
        self.room
            .apply_session_refreshed(self.user_id(), self.connection_id(), self.media_port)
            .await
    }

    async fn set_publication_activity(
        self,
        stream_type: StreamType,
        activity: PublicationActivity,
    ) -> PublicationActivityOutcome {
        self.room
            .set_publication_active_runtime(
                self.user_id(),
                self.connection_id(),
                stream_type,
                activity,
                self.media_port,
            )
            .await
    }

    async fn update_subscription(
        self,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) -> SubscriptionUpdateOutcome {
        self.room
            .update_subscription_runtime(
                self.user_id(),
                self.connection_id(),
                target_user_id,
                states,
                self.media_port,
            )
            .await
    }

    async fn update_user_info(self, info: UserInfo, refresh: UserInfoRefresh) {
        self.room
            .update_user_info_runtime_for_connection(
                self.user_id(),
                self.connection_id(),
                info,
                refresh,
                self.media_port,
            )
            .await;
    }

    async fn stage_publish(
        self,
        stream_type: StreamType,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        self.room
            .stage_negotiated_publish(
                self.user_id(),
                self.connection_id(),
                stream_type,
                self.media_port,
            )
            .await
    }

    async fn rollback_staged_publish(
        self,
        stream_type: StreamType,
    ) -> RollbackStagedPublishOutcome {
        self.room
            .rollback_staged_publish(
                self.user_id(),
                self.connection_id(),
                stream_type,
                self.media_port,
            )
            .await
    }

    async fn rollback_connection_publishes(self) {
        self.room
            .rollback_staged_publishes_for_connection(
                self.user_id(),
                self.connection_id(),
                self.media_port,
            )
            .await;
    }

    async fn commit_staged_publishes(self, applied_answer: &AppliedSessionAnswer) {
        self.room
            .commit_staged_publishes(
                self.user_id(),
                self.connection_id(),
                applied_answer,
                self.media_port,
                self.media_port,
            )
            .await;
    }

    async fn unpublish(self, stream_type: StreamType) -> UnpublishOutcome {
        self.room
            .unpublish_track(
                self.user_id(),
                self.connection_id(),
                stream_type,
                self.media_port,
            )
            .await
    }
}

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
        session: &MediaSessionContext<'_>,
        capabilities: MediaCapabilities,
        media_port: &RuntimeTransportAdapter,
    ) -> SessionNegotiationOutcome {
        RoomMediaSession::new(self, session, media_port)
            .apply_negotiated(capabilities)
            .await
    }

    async fn apply_session_refreshed(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &RuntimeTransportAdapter,
    ) -> SessionNegotiationOutcome {
        RoomMediaSession::new(self, session, media_port)
            .apply_refreshed()
            .await
    }

    async fn has_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
    ) -> bool {
        self.has_staged_publish(session.user_id(), session.connection_id(), stream_type)
            .await
    }

    async fn is_stream_published(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
    ) -> bool {
        self.is_stream_published(session.user_id(), stream_type)
            .await
    }

    async fn set_publication_active(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        activity: PublicationActivity,
        media_port: &RuntimeTransportAdapter,
    ) -> PublicationActivityOutcome {
        RoomMediaSession::new(self, session, media_port)
            .set_publication_activity(stream_type, activity)
            .await
    }

    async fn update_subscription(
        &self,
        session: &MediaSessionContext<'_>,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &RuntimeTransportAdapter,
    ) -> SubscriptionUpdateOutcome {
        RoomMediaSession::new(self, session, media_port)
            .update_subscription(target_user_id, states)
            .await
    }

    async fn update_user_info(
        &self,
        session: &MediaSessionContext<'_>,
        info: UserInfo,
        refresh: UserInfoRefresh,
        media_port: &RuntimeTransportAdapter,
    ) {
        RoomMediaSession::new(self, session, media_port)
            .update_user_info(info, refresh)
            .await;
    }

    async fn stage_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        RoomMediaSession::new(self, session, media_port)
            .stage_publish(stream_type)
            .await
    }

    async fn rollback_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> RollbackStagedPublishOutcome {
        RoomMediaSession::new(self, session, media_port)
            .rollback_staged_publish(stream_type)
            .await
    }

    async fn rollback_connection_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &RuntimeTransportAdapter,
    ) {
        RoomMediaSession::new(self, session, media_port)
            .rollback_connection_publishes()
            .await;
    }

    async fn commit_staged_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        applied_answer: &AppliedSessionAnswer,
        media_port: &RuntimeTransportAdapter,
    ) {
        RoomMediaSession::new(self, session, media_port)
            .commit_staged_publishes(applied_answer)
            .await;
    }

    async fn unpublish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> UnpublishOutcome {
        RoomMediaSession::new(self, session, media_port)
            .unpublish(stream_type)
            .await
    }
}
