use o_sfu_protocol::shared::{DownloadStates, StreamType, UserId};
use o_sfu_router::{
    MediaKind, MediaStream as RouterRtpParameters, derive_consumable_rtp_parameters,
};
use tracing::warn;

use super::super::super::{Room, media_transaction::PendingPublishTransaction};
use crate::runtime::{
    ConnectionId,
    transport_adapter::{MediaPort, RuntimeTransportAdapter, TransportMediaId},
};

#[derive(Debug, Clone)]
pub(crate) struct NegotiatedPublish {
    pub(crate) connection_id: ConnectionId,
    pub(crate) stream_type: StreamType,
    pub(crate) media_kind: MediaKind,
    pub(crate) transport_media_id: TransportMediaId,
    pub(crate) consumable_rtp_parameters: o_sfu_router::MediaStream,
}

#[derive(Clone, Copy)]
pub(crate) struct RoomTestMedia<'a> {
    pub(super) room: &'a Room,
}

impl RoomTestMedia<'_> {
    pub(crate) async fn publish_negotiated_track(
        self,
        user_id: &UserId,
        publish: NegotiatedPublish,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let validated_descriptor = {
            let state = self.room.state.read().await;
            state.validate_publish_descriptor(
                user_id,
                publish.connection_id,
                publish.stream_type,
                publish.media_kind,
            )?
        };
        PendingPublishTransaction::new(validated_descriptor, publish.transport_media_id)
            .commit_with_parameters(
                self.room,
                transport_adapter,
                transport_adapter,
                publish.consumable_rtp_parameters,
            )
            .await
    }

    pub(crate) async fn publish_track(
        self,
        user_id: &UserId,
        stream_type: StreamType,
        media_kind: MediaKind,
        producer_rtp_parameters: RouterRtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let publish_prerequisites = {
            let state = self.room.state.read().await;
            state.publish_prerequisites(user_id)?
        };
        let publisher_connection_id = publish_prerequisites.connection_id();
        let router_capabilities = publish_prerequisites.router_capabilities();
        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&producer_rtp_parameters, &router_capabilities)
                .map_err(|error| {
                    warn!(
                        ?user_id,
                        ?error,
                        "failed to derive consumable RTP parameters for producer"
                    );
                })
                .ok()?;
        let validated_descriptor = {
            let state = self.room.state.read().await;
            state.validate_publish_descriptor(
                user_id,
                publisher_connection_id,
                stream_type,
                media_kind,
            )?
        };
        let transport_media_id = match transport_adapter
            .publish_media(
                &self
                    .room
                    .transport_user_key(user_id, publisher_connection_id),
                media_kind,
                &producer_rtp_parameters,
            )
            .await
        {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    ?user_id,
                    connection_id = ?publisher_connection_id,
                    ?stream_type,
                    "transport adapter rejected publish media declaration"
                );
                return None;
            }
        };
        PendingPublishTransaction::new(validated_descriptor, transport_media_id)
            .commit_with_parameters(
                self.room,
                transport_adapter,
                transport_adapter,
                consumable_rtp_parameters,
            )
            .await
    }

    pub(crate) async fn set_publication_active(
        self,
        user_id: &UserId,
        stream_type: StreamType,
        active: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await
        else {
            return;
        };
        self.room
            .set_publication_active_runtime(
                user_id,
                connection_id,
                stream_type,
                active,
                transport_adapter,
            )
            .await;
    }

    pub(crate) async fn update_subscription(
        self,
        user_id: &UserId,
        target_user_id: &UserId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await
        else {
            return;
        };
        self.room
            .update_subscription_runtime(
                user_id,
                connection_id,
                target_user_id,
                states,
                transport_adapter,
            )
            .await;
    }
}
