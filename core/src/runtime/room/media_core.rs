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
        self.apply_session_negotiated(
            session.user_id(),
            session.connection_id(),
            capabilities,
            media_port,
        )
        .await
    }

    async fn apply_session_refreshed(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &RuntimeTransportAdapter,
    ) -> SessionNegotiationOutcome {
        self.apply_session_refreshed(session.user_id(), session.connection_id(), media_port)
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
        self.set_publication_active_runtime(
            session.user_id(),
            session.connection_id(),
            stream_type,
            activity,
            media_port,
        )
        .await
    }

    async fn update_subscription(
        &self,
        session: &MediaSessionContext<'_>,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &RuntimeTransportAdapter,
    ) -> SubscriptionUpdateOutcome {
        self.update_subscription_runtime(
            session.user_id(),
            session.connection_id(),
            target_user_id,
            states,
            media_port,
        )
        .await
    }

    async fn update_user_info(
        &self,
        session: &MediaSessionContext<'_>,
        info: UserInfo,
        refresh: UserInfoRefresh,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.update_user_info_runtime_for_connection(
            session.user_id(),
            session.connection_id(),
            info,
            refresh,
            media_port,
        )
        .await;
    }

    async fn stage_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        self.stage_negotiated_publish(
            session.user_id(),
            session.connection_id(),
            stream_type,
            media_port,
        )
        .await
    }

    async fn rollback_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> RollbackStagedPublishOutcome {
        self.rollback_staged_publish(
            session.user_id(),
            session.connection_id(),
            stream_type,
            media_port,
        )
        .await
    }

    async fn rollback_connection_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.rollback_staged_publishes_for_connection(
            session.user_id(),
            session.connection_id(),
            media_port,
        )
        .await;
    }

    async fn commit_staged_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        applied_answer: &AppliedSessionAnswer,
        media_port: &RuntimeTransportAdapter,
    ) {
        self.commit_staged_publishes(
            session.user_id(),
            session.connection_id(),
            applied_answer,
            media_port,
            media_port,
        )
        .await;
    }

    async fn unpublish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &RuntimeTransportAdapter,
    ) -> UnpublishOutcome {
        self.unpublish_track(
            session.user_id(),
            session.connection_id(),
            stream_type,
            media_port,
        )
        .await
    }
}
