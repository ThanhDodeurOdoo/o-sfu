use o_sfu_protocol::shared::{StreamType, UserId};
use o_sfu_router::MediaCapabilities as RouterRtpCapabilities;

use super::shared::{RoomState, SourceKey};
use crate::runtime::{
    ConnectionId,
    room::RoomUserPermissions,
    source_model::{PublishedSourceId, SourceEncodingId},
    transport_adapter::TransportMediaId,
};

impl RoomState {
    pub(in crate::runtime::room) fn session_permissions(
        &self,
        user_id: &UserId,
    ) -> Option<RoomUserPermissions> {
        self.users.get(user_id).map(|user| user.permissions)
    }

    pub(in crate::runtime::room) fn session_has_parsed_client_rtp_capabilities(
        &self,
        user_id: &UserId,
    ) -> bool {
        self.users
            .get(user_id)
            .and_then(|user| user.parsed_client_rtp_capabilities.as_ref())
            .is_some()
    }

    pub(in crate::runtime::room) fn parsed_client_rtp_capabilities(
        &self,
        user_id: &UserId,
    ) -> Option<RouterRtpCapabilities> {
        self.users
            .get(user_id)
            .and_then(|user| user.parsed_client_rtp_capabilities.clone())
    }

    pub(in crate::runtime::room) fn producer_count(&self) -> usize {
        self.producers.len()
    }

    pub(in crate::runtime::room) fn consumer_count(&self) -> usize {
        self.consumer_index.len()
    }

    pub(in crate::runtime::room) fn has_session(&self, user_id: &UserId) -> bool {
        self.users.contains_key(user_id)
    }

    pub(in crate::runtime::room) fn first_published_transport_media_id(
        &self,
    ) -> Option<TransportMediaId> {
        self.producers
            .values()
            .find_map(|producer| producer.transport_media_id)
    }

    pub(in crate::runtime::room) fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        let producer_id = self.producer_id_for_source_key(&SourceKey::new(user_id, stream_type))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id != connection_id {
            return None;
        }
        producer.transport_media_id
    }

    pub(in crate::runtime::room) fn inspect_producer_owner_user_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserId> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.owner_user_id().clone())
    }

    pub(in crate::runtime::room) fn inspect_source_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<PublishedSourceId> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.source_id)
    }

    pub(in crate::runtime::room) fn inspect_source_encoding_ids_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Vec<SourceEncodingId>> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.encoding_ids.clone())
    }

    pub(in crate::runtime::room) fn inspect_producer_owner_connection_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<ConnectionId> {
        self.source_transport_media_entry(transport_media_id)
            .map(super::shared::SourceTransportMediaIndexEntry::owner_connection_id)
    }
}
