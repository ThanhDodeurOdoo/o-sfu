use o_sfu_protocol::shared::{SessionId, StreamType};
use o_sfu_router::MediaCapabilities as RouterRtpCapabilities;

use crate::runtime::ConnectionId;
use crate::runtime::transport_adapter::TransportMediaId;

use super::shared::{ChannelState, ProducerKey};

impl ChannelState {
    pub(in crate::runtime::channel) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.sessions
            .get(session_id)
            .map(|session| session.permissions.router_permissions())
    }

    pub(in crate::runtime::channel) fn session_has_parsed_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> bool {
        self.sessions
            .get(session_id)
            .and_then(|session| session.parsed_client_rtp_capabilities.as_ref())
            .is_some()
    }

    pub(in crate::runtime::channel) fn parsed_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterRtpCapabilities> {
        self.sessions
            .get(session_id)
            .and_then(|session| session.parsed_client_rtp_capabilities.clone())
    }

    pub(in crate::runtime::channel) fn producer_count(&self) -> usize {
        self.producers.len()
    }

    pub(in crate::runtime::channel) fn consumer_count(&self) -> usize {
        self.consumer_index.len()
    }

    pub(in crate::runtime::channel) fn has_session(&self, session_id: &SessionId) -> bool {
        self.sessions.contains_key(session_id)
    }

    pub(in crate::runtime::channel) fn first_published_transport_media_id(
        &self,
    ) -> Option<TransportMediaId> {
        self.producers
            .values()
            .find_map(|producer| producer.transport_media_id)
    }

    pub(in crate::runtime::channel) fn producer_transport_media_id(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        let producer_id = self
            .producer_ids_by_owner_stream
            .get(&ProducerKey::new(session_id, stream_type))?;
        let producer = self.producers.get(producer_id)?;
        if producer.owner_connection_id != connection_id {
            return None;
        }
        producer.transport_media_id
    }

    pub(in crate::runtime::channel) fn inspect_producer_owner_session_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<SessionId> {
        self.producer_transport_media_entry(transport_media_id)
            .map(|entry| entry.owner_session_id().clone())
    }

    pub(in crate::runtime::channel) fn inspect_producer_owner_connection_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<ConnectionId> {
        self.producer_transport_media_entry(transport_media_id)
            .map(super::shared::ProducerTransportMediaIndexEntry::owner_connection_id)
    }
}
