use o_sfu_protocol::shared::SessionId;
use o_sfu_router::MediaCapabilities;

use crate::runtime::ConnectionId;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;

use super::super::super::Channel;
use super::super::super::session_negotiation::{SessionNegotiationUpdate, SessionTransportReady};

#[derive(Clone, Copy)]
pub(crate) struct ChannelTestNegotiation<'a> {
    pub(super) channel: &'a Channel,
}

impl ChannelTestNegotiation<'_> {
    pub(crate) async fn apply_client_rtp_capabilities(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.channel.state.write().await;
            state.set_client_rtp_capabilities_for_test(session_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update_for_test(session_id, connection_id, update, transport_adapter)
            .await
    }

    pub(crate) async fn apply_publish_transport_ready(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Publish,
            transport_adapter,
        )
        .await
    }

    pub(crate) async fn apply_consume_transport_ready(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Consume,
            transport_adapter,
        )
        .await
    }

    async fn apply_transport_ready_for_test(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        readiness: SessionTransportReady,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.channel.state.write().await;
            state.set_transport_ready_for_test(session_id, connection_id, readiness)
        };
        self.apply_negotiation_update_for_test(session_id, connection_id, update, transport_adapter)
            .await
    }

    async fn apply_negotiation_update_for_test(
        self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        update: SessionNegotiationUpdate,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        if !update.session_present {
            return false;
        }
        if update.became_consumer_ready {
            return self
                .channel
                .bootstrap_missing_consumers_for_connection(
                    session_id,
                    connection_id,
                    transport_adapter,
                )
                .await;
        }
        true
    }

    pub(crate) async fn set_client_rtp_capabilities(
        self,
        session_id: &SessionId,
        capabilities: MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let mut state = self.channel.state.write().await;
        let connection_id = state
            .session_connection_id(session_id)
            .unwrap_or(ConnectionId::from_raw(u64::MAX));
        state.set_client_rtp_capabilities_for_test(session_id, connection_id, &capabilities)
    }

    pub(crate) async fn set_publish_transport_ready(
        self,
        session_id: &SessionId,
    ) -> SessionNegotiationUpdate {
        let mut state = self.channel.state.write().await;
        let connection_id = state
            .session_connection_id(session_id)
            .unwrap_or(ConnectionId::from_raw(u64::MAX));
        state.set_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Publish,
        )
    }

    pub(crate) async fn set_consume_transport_ready(
        self,
        session_id: &SessionId,
    ) -> SessionNegotiationUpdate {
        let mut state = self.channel.state.write().await;
        let connection_id = state
            .session_connection_id(session_id)
            .unwrap_or(ConnectionId::from_raw(u64::MAX));
        state.set_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Consume,
        )
    }
}
