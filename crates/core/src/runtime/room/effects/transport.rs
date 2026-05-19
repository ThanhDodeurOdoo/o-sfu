use o_sfu_router::{MediaKind, MediaStream as RouterRtpParameters};

use crate::runtime::{
    ConnectionId, UserId,
    media_transport::{
        ConsumerActivity, ConsumerPacketGateUpdate, MediaTransport, ProducerActivity,
        TransportAdapterError, TransportConsumerRoute, TransportMediaId, TransportRelayRouteEffect,
        TransportSessionKey,
    },
    source_model::UserStreamId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct PublishReservationContinuation {
    pub(in crate::runtime::room) user: UserId,
    pub(in crate::runtime::room) connection: ConnectionId,
    pub(in crate::runtime::room) stream: UserStreamId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct ConsumerCreationContinuation {
    pub(in crate::runtime::room) user: UserId,
    pub(in crate::runtime::room) connection: ConnectionId,
    pub(in crate::runtime::room) stream: UserStreamId,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) enum RoomTransportEffect {
    PublishReservation {
        continuation: PublishReservationContinuation,
        session_key: TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
    },
    ConsumerCreation {
        continuation: ConsumerCreationContinuation,
        consumer_session_key: TransportSessionKey,
        media_kind: MediaKind,
        producer_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        consumer_rtp_parameters: RouterRtpParameters,
    },
    MediaRemoval {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
    },
    SessionClose {
        session_key: TransportSessionKey,
    },
    RelayRoute(TransportRelayRouteEffect),
    ProducerActivity {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    },
    ConsumerActivity {
        route: TransportConsumerRoute,
        activity: ConsumerActivity,
    },
    PacketGateBatch(Vec<ConsumerPacketGateUpdate>),
    KeyframeRequest {
        route: TransportConsumerRoute,
    },
}

impl RoomTransportEffect {
    pub(in crate::runtime::room) async fn execute_unit(
        &self,
        media_transport: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::MediaRemoval {
                session_key,
                transport_media_id,
            } => {
                media_transport
                    .remove_media(session_key, *transport_media_id)
                    .await
            }
            Self::SessionClose { session_key } => media_transport.close_session(session_key).await,
            Self::RelayRoute(effect) => media_transport.apply_relay_route_effect(effect).await,
            Self::ProducerActivity {
                session_key,
                transport_media_id,
                activity,
            } => {
                media_transport
                    .set_producer_active(session_key, *transport_media_id, *activity)
                    .await
            }
            Self::ConsumerActivity { route, activity } => {
                media_transport.set_consumer_active(route, *activity).await
            }
            Self::KeyframeRequest { route } => {
                media_transport.request_consumer_keyframe(route).await
            }
            Self::PublishReservation { .. }
            | Self::ConsumerCreation { .. }
            | Self::PacketGateBatch(_) => Ok(()),
        }
    }

    pub(in crate::runtime::room) async fn execute_publish_reservation(
        &self,
        media_transport: &MediaTransport,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let Self::PublishReservation {
            continuation,
            session_key,
            media_kind,
            rtp_parameters,
        } = self
        else {
            return Err(TransportAdapterError::InvalidInput);
        };
        let _ = (
            &continuation.user,
            continuation.connection,
            &continuation.stream,
        );
        media_transport
            .publish_media(session_key, *media_kind, rtp_parameters)
            .await
    }

    pub(in crate::runtime::room) async fn execute_consumer_creation(
        &self,
        media_transport: &MediaTransport,
    ) -> Result<(TransportMediaId, Option<String>), TransportAdapterError> {
        let Self::ConsumerCreation {
            continuation,
            consumer_session_key,
            media_kind,
            producer_session_key,
            source_transport_media_id,
            consumer_rtp_parameters,
        } = self
        else {
            return Err(TransportAdapterError::InvalidInput);
        };
        let _ = (
            &continuation.user,
            continuation.connection,
            &continuation.stream,
        );
        let transport_media_id = media_transport
            .consume_media(
                consumer_session_key,
                *media_kind,
                producer_session_key,
                *source_transport_media_id,
                consumer_rtp_parameters,
            )
            .await?;
        let mid = media_transport
            .transport_media_mid(consumer_session_key, transport_media_id)
            .await;
        Ok((transport_media_id, mid))
    }

    pub(in crate::runtime::room) async fn execute_packet_gate_batch(
        &self,
        media_transport: &MediaTransport,
    ) -> Vec<Result<(), TransportAdapterError>> {
        let Self::PacketGateBatch(updates) = self else {
            return Vec::new();
        };
        media_transport.set_consumer_packet_gates(updates).await
    }
}
