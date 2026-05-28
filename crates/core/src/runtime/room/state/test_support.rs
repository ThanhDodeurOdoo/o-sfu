use o_sfu_router::RouterId;
#[cfg(test)]
use {super::super::media_graph::ConsumerKey, crate::runtime::source_model::PublishedSourceId};

use super::{super::media_graph::SourceTransportMediaIndexEntry, shared::RoomState};
use crate::runtime::{
    ConnectionId, TestSourceKind, UserId,
    media_transport::TransportMediaId,
    room::RoomUserPermissions,
    source_model::{SourceEncodingId, test_support::stream_id_for_source},
};

impl RoomState {
    pub fn session_permissions(&self, user_id: &UserId) -> Option<RoomUserPermissions> {
        self.users.get(user_id).map(|user| user.permissions)
    }

    pub fn session_has_parsed_client_rtp_capabilities(&self, user_id: &UserId) -> bool {
        self.users
            .get(user_id)
            .and_then(|user| user.parsed_client_rtp_capabilities.as_ref())
            .is_some()
    }

    pub fn session_client_rtp_codec_names(&self, user_id: &UserId) -> Option<Vec<String>> {
        self.users
            .get(user_id)
            .and_then(|user| user.parsed_client_rtp_capabilities.as_ref())
            .map(|capabilities| {
                capabilities
                    .codecs()
                    .map(|codec| codec.codec_name().to_owned())
                    .collect()
            })
    }

    pub fn producer_count(&self) -> usize {
        self.media.producer_count()
    }

    pub fn consumer_count(&self) -> usize {
        self.media.consumer_count()
    }

    pub fn has_session(&self, user_id: &UserId) -> bool {
        self.users.contains_key(user_id)
    }

    pub fn topology_home_router_id(&self, user_id: &UserId) -> Option<RouterId> {
        self.topology.home_router_id_for_user(user_id)
    }

    pub fn topology_router_count(&self) -> usize {
        self.topology.router_count()
    }

    pub fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.media.first_published_transport_media_id()
    }

    pub fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        self.media.producer_transport_media_id(
            user_id,
            connection_id,
            &stream_id_for_source(stream_type),
        )
    }

    pub fn inspect_producer_owner_user_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserId> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.owner_user_id().clone())
    }

    #[cfg(test)]
    pub fn inspect_source_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<PublishedSourceId> {
        self.source_transport_media_entry(transport_media_id)
            .map(SourceTransportMediaIndexEntry::source_id)
    }

    #[cfg(test)]
    pub fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_type: TestSourceKind,
    ) -> Option<PublishedSourceId> {
        self.media
            .source_id_for_owner_stream(owner_user_id, &stream_id_for_source(stream_type))
    }

    #[cfg(test)]
    pub fn contains_consumer_source_selection(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        self.media
            .consumer_source_selection(&ConsumerKey::new(consumer_user_id, source_id))
            .is_some()
    }

    pub fn inspect_source_encoding_ids_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Vec<SourceEncodingId>> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.encoding_ids().to_vec())
    }

    pub fn inspect_producer_owner_connection_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<ConnectionId> {
        self.source_transport_media_entry(transport_media_id)
            .map(SourceTransportMediaIndexEntry::owner_connection_id)
    }
}
