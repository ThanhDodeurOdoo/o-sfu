use o_sfu_router::RouterId;

use super::super::super::{Room, RoomUserPermissions, TestPlacementReason};
#[cfg(test)]
use crate::engine::source_model::PublishedSourceId;
use crate::engine::{
    ConnectionId, TestSourceKind, UserId, UserInfo,
    media_transport::TransportMediaId,
    source_model::{
        SourceEncodingId,
        test_support::{source_kind_for_stream_id, stream_id_for_source},
    },
};

#[derive(Clone, Copy)]
pub struct RoomTestInspect<'a> {
    pub(super) room: &'a Room,
}

impl RoomTestInspect<'_> {
    pub async fn router_user_count(self) -> usize {
        let (count, _active_stream_counts) = self.room.state.read().await.user_stats_counts();
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    pub async fn room_user_permissions(self, user_id: &UserId) -> Option<RoomUserPermissions> {
        self.room.state.read().await.session_permissions(user_id)
    }

    pub async fn session_has_parsed_client_rtp_capabilities(self, user_id: &UserId) -> bool {
        self.room
            .state
            .read()
            .await
            .session_has_parsed_client_rtp_capabilities(user_id)
    }

    pub async fn session_client_rtp_codec_names(self, user_id: &UserId) -> Option<Vec<String>> {
        self.room
            .state
            .read()
            .await
            .session_client_rtp_codec_names(user_id)
    }

    pub async fn user_connection_id(self, user_id: &UserId) -> Option<ConnectionId> {
        self.room.state.read().await.user_connection_id(user_id)
    }

    pub async fn producer_count(self) -> usize {
        self.room.state.read().await.producer_count()
    }

    pub async fn consumer_count(self) -> usize {
        self.room.state.read().await.consumer_count()
    }

    pub async fn first_published_transport_media_id(self) -> Option<TransportMediaId> {
        self.room
            .state
            .read()
            .await
            .first_published_transport_media_id()
    }

    pub async fn producer_transport_media_id(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        self.room.state.read().await.producer_transport_media_id(
            user_id,
            connection_id,
            stream_type,
        )
    }

    pub async fn has_producer_route_target(
        self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> bool {
        self.room
            .state
            .read()
            .await
            .producer_route_target(
                owner_user_id,
                owner_connection_id,
                &stream_id_for_source(stream_type),
            )
            .is_some()
    }

    pub async fn producer_stream_type_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<TestSourceKind> {
        self.room
            .state
            .read()
            .await
            .producer_stream_id_for_transport_media_id(transport_media_id)
            .and_then(|stream_id| source_kind_for_stream_id(&stream_id))
    }

    pub async fn producer_owner_user_id_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserId> {
        self.room
            .state
            .read()
            .await
            .inspect_producer_owner_user_id_for_transport_media_id(transport_media_id)
    }

    pub async fn producer_owner_connection_id_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<ConnectionId> {
        self.room
            .state
            .read()
            .await
            .inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id)
    }

    #[cfg(test)]
    pub async fn source_id_for_owner_stream(
        self,
        owner_user_id: &UserId,
        stream_type: TestSourceKind,
    ) -> Option<PublishedSourceId> {
        self.room
            .state
            .read()
            .await
            .source_id_for_owner_stream(owner_user_id, stream_type)
    }

    #[cfg(test)]
    pub async fn contains_consumer_source_selection(
        self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        self.room
            .state
            .read()
            .await
            .contains_consumer_source_selection(consumer_user_id, source_id)
    }

    pub async fn source_encoding_ids_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<Vec<SourceEncodingId>> {
        self.room
            .state
            .read()
            .await
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
    }

    pub async fn user_info_snapshot(self, user_id: &UserId) -> Option<(UserId, UserInfo)> {
        self.room.state.read().await.user_info_snapshot(user_id)
    }

    pub async fn has_session(self, user_id: &UserId) -> bool {
        self.room.state.read().await.has_session(user_id)
    }

    #[must_use]
    pub fn recording_address(&self) -> Option<&str> {
        self.room.definition.recording_address()
    }

    pub async fn topology_home_router_id(self, user_id: &UserId) -> Option<RouterId> {
        self.room
            .state
            .read()
            .await
            .topology_home_router_id(user_id)
    }

    pub async fn topology_home_media_worker_id(self, user_id: &UserId) -> Option<usize> {
        let connection_id = self.room.state.read().await.user_connection_id(user_id)?;
        Some(
            self.room
                .placement_state
                .media_worker_id_for_connection(connection_id),
        )
    }

    pub async fn topology_router_count(self) -> usize {
        self.room.state.read().await.topology_router_count()
    }

    pub fn load_triggered_last_decision_reason(self) -> Option<TestPlacementReason> {
        self.room
            .load_triggered_last_decision_reason()
            .map(TestPlacementReason::from)
    }

    #[must_use]
    pub fn media_worker_id(self) -> usize {
        self.room.placement_state.media_worker_id()
    }
}
