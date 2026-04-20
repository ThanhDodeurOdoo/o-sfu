use o_sfu_protocol::shared::{SessionId, SessionInfo, StreamType};

use crate::runtime::transport_adapter::TransportMediaId;

use super::super::super::Channel;

#[derive(Clone, Copy)]
pub(crate) struct ChannelTestInspect<'a> {
    pub(super) channel: &'a Channel,
}

impl ChannelTestInspect<'_> {
    pub(crate) async fn router_session_count(self) -> usize {
        let (count, _camera_count, _screen_count) =
            self.channel.state.read().await.session_stats_counts();
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    pub(crate) async fn router_session_permissions(
        self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.channel
            .state
            .read()
            .await
            .session_permissions(session_id)
    }

    pub(crate) async fn session_has_parsed_client_rtp_capabilities(
        self,
        session_id: &SessionId,
    ) -> bool {
        self.channel
            .state
            .read()
            .await
            .session_has_parsed_client_rtp_capabilities(session_id)
    }

    pub(crate) async fn parsed_client_rtp_capabilities(
        self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::MediaCapabilities> {
        self.channel
            .state
            .read()
            .await
            .parsed_client_rtp_capabilities(session_id)
    }

    pub(crate) async fn session_connection_id(self, session_id: &SessionId) -> Option<u64> {
        self.channel
            .state
            .read()
            .await
            .session_connection_id(session_id)
    }

    pub(crate) async fn producer_count(self) -> usize {
        self.channel.state.read().await.producer_count()
    }

    pub(crate) async fn consumer_count(self) -> usize {
        self.channel.state.read().await.consumer_count()
    }

    pub(crate) async fn first_published_transport_media_id(self) -> Option<TransportMediaId> {
        self.channel
            .state
            .read()
            .await
            .first_published_transport_media_id()
    }

    pub(crate) async fn producer_transport_media_id(
        self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.channel.state.read().await.producer_transport_media_id(
            session_id,
            connection_id,
            stream_type,
        )
    }

    pub(crate) async fn has_producer_route_target(
        self,
        owner_session_id: &SessionId,
        owner_connection_id: u64,
        stream_type: StreamType,
    ) -> bool {
        self.channel
            .state
            .read()
            .await
            .producer_route_target(owner_session_id, owner_connection_id, stream_type)
            .is_some()
    }

    pub(crate) async fn producer_stream_type_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.channel
            .state
            .read()
            .await
            .producer_stream_type_for_transport_media_id(transport_media_id)
    }

    pub(crate) async fn session_info_snapshot(
        self,
        session_id: &SessionId,
    ) -> Option<(SessionId, SessionInfo)> {
        self.channel
            .state
            .read()
            .await
            .session_info_snapshot(session_id)
    }

    pub(crate) async fn has_session(self, session_id: &SessionId) -> bool {
        self.channel.state.read().await.has_session(session_id)
    }

    #[must_use]
    pub(crate) const fn media_worker_id(self) -> usize {
        self.channel.definition.media_worker_id()
    }
}
