use o_sfu_router::RouterId;

use super::shared::{RoomState, SourceKey};
#[cfg(test)]
use crate::runtime::source_model::PublishedSourceId;
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
        self.media.producers.len()
    }

    pub fn consumer_count(&self) -> usize {
        self.media.consumer_index.len()
    }

    pub fn has_session(&self, user_id: &UserId) -> bool {
        self.users.contains_key(user_id)
    }

    pub fn topology_home_router_id(&self, user_id: &UserId) -> Option<RouterId> {
        self.topology.home_router_id_for_user(user_id)
    }

    pub fn topology_home_media_worker_id(&self, user_id: &UserId) -> Option<usize> {
        self.topology
            .home_placement_for_user(user_id)
            .map(|placement| placement.media_worker)
    }

    pub fn topology_router_count(&self) -> usize {
        self.topology.router_count()
    }

    pub fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.media
            .producers
            .values()
            .find_map(|producer| producer.transport_media_id)
    }

    pub fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        let producer_id = self.producer_id_for_source_key(&SourceKey::new(
            user_id,
            &stream_id_for_source(stream_type),
        ))?;
        let producer = self.media.producers.get(&producer_id)?;
        if producer.owner_connection_id != connection_id {
            return None;
        }
        producer.transport_media_id
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
            .map(|entry| entry.source_id)
    }

    pub fn inspect_source_encoding_ids_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Vec<SourceEncodingId>> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.encoding_ids.clone())
    }

    pub fn inspect_producer_owner_connection_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<ConnectionId> {
        self.source_transport_media_entry(transport_media_id)
            .map(super::shared::SourceTransportMediaIndexEntry::owner_connection_id)
    }
}
