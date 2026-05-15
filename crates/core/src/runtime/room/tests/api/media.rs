use o_sfu_router::{
    MediaKind, MediaStream as RouterRtpParameters, derive_consumable_rtp_parameters,
};
use tracing::warn;

use super::super::super::{Room, media_transaction::PendingPublishTransaction};
use crate::runtime::{
    ConnectionId, TestSourceKind, UserId,
    media_transport::{MediaTransport, TransportMediaId},
    source_model::{
        SourcePublishIntent, UserStreamId, test_support::source_publish_intent_for_source,
    },
};

#[derive(Debug, Clone)]
pub struct NegotiatedPublish {
    pub connection_id: ConnectionId,
    pub stream_type: TestSourceKind,
    pub media_kind: MediaKind,
    pub transport_media_id: TransportMediaId,
    pub consumable_rtp_parameters: o_sfu_router::MediaStream,
}

#[derive(Clone, Copy)]
pub struct RoomTestMedia<'a> {
    pub(super) room: &'a Room,
}

impl RoomTestMedia<'_> {
    pub async fn publish_negotiated_track(
        self,
        user_id: &UserId,
        publish: NegotiatedPublish,
        media_transport: &MediaTransport,
    ) -> Option<UserStreamId> {
        let intent = source_publish_intent_for_source(publish.stream_type);
        let validated_descriptor = {
            let state = self.room.state.read().await;
            state.validate_publish_descriptor(user_id, publish.connection_id, &intent)?
        };
        PendingPublishTransaction::new(validated_descriptor, publish.transport_media_id)
            .commit_with_parameters(
                self.room,
                media_transport,
                media_transport,
                publish.consumable_rtp_parameters,
            )
            .await
    }

    pub async fn publish_track(
        self,
        user_id: &UserId,
        stream_type: TestSourceKind,
        media_kind: MediaKind,
        producer_rtp_parameters: RouterRtpParameters,
        media_transport: &MediaTransport,
    ) -> Option<UserStreamId> {
        let intent = source_publish_intent_for_source(stream_type);
        self.publish_intent(
            user_id,
            &intent,
            media_kind,
            producer_rtp_parameters,
            media_transport,
        )
        .await
    }

    pub async fn publish_intent(
        self,
        user_id: &UserId,
        intent: &SourcePublishIntent,
        media_kind: MediaKind,
        producer_rtp_parameters: RouterRtpParameters,
        media_transport: &MediaTransport,
    ) -> Option<UserStreamId> {
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
            state.validate_publish_descriptor(user_id, publisher_connection_id, intent)?
        };
        let transport_media_id = match media_transport
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
                    stream_id = %intent.stream_id(),
                    "media transport rejected publish media declaration"
                );
                return None;
            }
        };
        PendingPublishTransaction::new(validated_descriptor, transport_media_id)
            .commit_with_parameters(
                self.room,
                media_transport,
                media_transport,
                consumable_rtp_parameters,
            )
            .await
    }
}
