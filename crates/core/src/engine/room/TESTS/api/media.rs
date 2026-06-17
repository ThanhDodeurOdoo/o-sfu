use std::collections::BTreeMap;

use o_sfu_router::{
    MediaKind, MediaStream as RouterRtpParameters, derive_consumable_rtp_parameters,
};
use tracing::warn;

use super::super::super::{Room, transition::StagedPublish};
use crate::{
    UnpublishIntentOutcome,
    engine::{
        ConnectionId, TestSourceKind, UserId,
        media_transport::{MediaTransport, TransportMediaId},
        source_model::{
            SourcePublishIntent, SourceSubscriptionIntent, UserStreamId,
            test_support::source_publish_intent_for_source,
        },
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

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestPublishIntentOutcome {
    Noop,
    Queue,
    Activated,
    Staged,
}

#[cfg(test)]
impl From<crate::PublishIntentOutcome> for TestPublishIntentOutcome {
    fn from(outcome: crate::PublishIntentOutcome) -> Self {
        match outcome {
            crate::PublishIntentOutcome::Noop => Self::Noop,
            crate::PublishIntentOutcome::Queue => Self::Queue,
            crate::PublishIntentOutcome::Activated => Self::Activated,
            crate::PublishIntentOutcome::Staged => Self::Staged,
        }
    }
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
            state.validate_publish(user_id, publish.connection_id, &intent)?
        };
        StagedPublish::new(validated_descriptor, publish.transport_media_id)
            .commit_with_parameters(
                self.room
                    .user_operation(user_id, publish.connection_id, media_transport),
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
        let (publisher_connection_id, capabilities) = {
            let state = self.room.state.read().await;
            let user = state.users.get(user_id)?;
            if !user.negotiation.can_publish() {
                return None;
            }
            (user.connection_id, state.router_rtp_capabilities())
        };
        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&producer_rtp_parameters, &capabilities)
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
            state.validate_publish(user_id, publisher_connection_id, intent)?
        };
        let session_key = self
            .room
            .transport_user_key(user_id, publisher_connection_id)
            .await;
        let transport_media_id = match media_transport
            .publish_media(&session_key, media_kind, &producer_rtp_parameters)
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
        StagedPublish::new(validated_descriptor, transport_media_id)
            .commit_with_parameters(
                self.room
                    .user_operation(user_id, publisher_connection_id, media_transport),
                consumable_rtp_parameters,
            )
            .await
    }

    pub async fn unpublish_track(
        self,
        user_id: &UserId,
        stream_id: &UserStreamId,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await
        else {
            return false;
        };
        self.room
            .user_operation(user_id, connection_id, media_transport)
            .stop_publish(stream_id)
            .await
            != UnpublishIntentOutcome::Noop
    }

    pub async fn set_publication_active(
        self,
        user_id: &UserId,
        stream_id: &UserStreamId,
        active: bool,
        media_transport: &MediaTransport,
    ) -> bool {
        let connection_id = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await;
        let Some(connection_id) = connection_id else {
            return false;
        };
        self.room
            .user_operation(user_id, connection_id, media_transport)
            .set_publication_active(stream_id, active)
            .await
            .is_some()
    }

    pub async fn update_subscription(
        self,
        receiver_id: &UserId,
        source_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(receiver_id)
            .await
        else {
            return false;
        };
        self.room
            .user_operation(receiver_id, connection_id, media_transport)
            .apply_receiver_intent(source_id, intents)
            .await
            .is_some()
    }

    #[must_use]
    pub fn has_staged_publish(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.room
            .has_staged_publish(user_id, connection_id, stream_id)
    }
}
