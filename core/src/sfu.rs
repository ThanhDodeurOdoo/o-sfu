use o_sfu_protocol::shared::{DownloadStates, StreamType, UserId, UserInfo};
use o_sfu_router::MediaCapabilities;

use crate::{
    ConnectionId, CoreOptions, MediaRoom,
    transport::{
        AppliedSessionAnswer, MediaPort, NegotiationPort, ObservabilityPort, SessionOffer,
        SessionUploadEncoding, SessionUploadSlot, TransportAdapterError, TransportSessionHealth,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OfferedMediaCapabilities(MediaCapabilities);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaNegotiationOffer {
    pub sdp: String,
    pub upload_slots: Vec<MediaUploadSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaUploadSlot {
    pub mid: String,
    pub kind: o_sfu_router::MediaKind,
    pub codecs: Vec<String>,
    pub simulcast_encodings: Vec<MediaUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaUploadEncoding {
    pub rid: String,
    pub max_bitrate: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaEndpointHealth {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SfuCoreError {
    Transport(TransportAdapterError),
    CapabilityProjection(TransportAdapterError),
    UserStateCommitRejected,
    UserStateRefreshRejected,
}

#[derive(Debug, Clone)]
pub struct SfuCore<T> {
    _options: CoreOptions,
    transport_adapter: T,
}

impl<T> SfuCore<T>
where
    T: Clone + MediaPort + NegotiationPort + ObservabilityPort + Send + Sync,
{
    #[must_use]
    pub fn new(options: CoreOptions, transport_adapter: T) -> Self {
        Self {
            _options: options,
            transport_adapter,
        }
    }

    #[must_use]
    pub fn endpoint_health<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<MediaEndpointHealth>
    where
        R: MediaRoom<T>,
    {
        let session_key = room.transport_user_key(user_id, connection_id);
        self.transport_adapter
            .session_transport_health(&session_key)
            .map(|health| match health {
                TransportSessionHealth::Connected => MediaEndpointHealth::Connected,
                TransportSessionHealth::Disconnected => MediaEndpointHealth::Disconnected,
            })
    }

    pub async fn create_initial_offer<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Result<(MediaNegotiationOffer, OfferedMediaCapabilities), SfuCoreError>
    where
        R: MediaRoom<T>,
    {
        let offered_capabilities = OfferedMediaCapabilities(room.router_rtp_capabilities().await);
        let session_key = room.transport_user_key(user_id, connection_id);
        let offer = self
            .transport_adapter
            .create_initial_session_offer(&session_key)
            .await
            .map_err(SfuCoreError::Transport)?;
        Ok((MediaNegotiationOffer::from(offer), offered_capabilities))
    }

    pub async fn create_renegotiation_offer<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Result<Option<MediaNegotiationOffer>, SfuCoreError>
    where
        R: MediaRoom<T>,
    {
        let session_key = room.transport_user_key(user_id, connection_id);
        match self
            .transport_adapter
            .create_session_renegotiation_offer(&session_key)
            .await
        {
            Ok(offer) => Ok(Some(MediaNegotiationOffer::from(offer))),
            Err(TransportAdapterError::UnsupportedFeature) => Ok(None),
            Err(error) => Err(SfuCoreError::Transport(error)),
        }
    }

    pub async fn apply_initial_answer<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        answer_sdp: &str,
        offered_capabilities: &OfferedMediaCapabilities,
    ) -> Result<(), SfuCoreError>
    where
        R: MediaRoom<T>,
    {
        let applied_answer = self
            .apply_transport_answer(room, user_id, connection_id, answer_sdp)
            .await?;
        let client_capabilities = self
            .transport_adapter
            .negotiated_client_rtp_capabilities(answer_sdp, &offered_capabilities.0)
            .map_err(SfuCoreError::CapabilityProjection)?;
        if !room
            .apply_session_negotiated(
                user_id,
                connection_id,
                client_capabilities,
                &self.transport_adapter,
            )
            .await
        {
            return Err(SfuCoreError::UserStateCommitRejected);
        }
        self.commit_staged_publishes(room, user_id, connection_id, &applied_answer)
            .await;
        Ok(())
    }

    pub async fn apply_renegotiation_answer<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        answer_sdp: &str,
    ) -> Result<(), SfuCoreError>
    where
        R: MediaRoom<T>,
    {
        let applied_answer = self
            .apply_transport_answer(room, user_id, connection_id, answer_sdp)
            .await?;
        if !room
            .apply_session_refreshed(user_id, connection_id, &self.transport_adapter)
            .await
        {
            return Err(SfuCoreError::UserStateRefreshRejected);
        }
        self.commit_staged_publishes(room, user_id, connection_id, &applied_answer)
            .await;
        Ok(())
    }

    pub async fn has_staged_publish<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool
    where
        R: MediaRoom<T>,
    {
        room.has_staged_publish(user_id, connection_id, stream_type)
            .await
    }

    pub async fn is_stream_published<R>(
        &self,
        room: &R,
        user_id: &UserId,
        stream_type: StreamType,
    ) -> bool
    where
        R: MediaRoom<T>,
    {
        room.is_stream_published(user_id, stream_type).await
    }

    pub async fn set_publication_active<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        active: bool,
    ) where
        R: MediaRoom<T>,
    {
        room.set_publication_active(
            user_id,
            connection_id,
            stream_type,
            active,
            &self.transport_adapter,
        )
        .await;
    }

    pub async fn update_subscription<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) where
        R: MediaRoom<T>,
    {
        room.update_subscription(
            user_id,
            connection_id,
            target_user_id,
            states,
            &self.transport_adapter,
        )
        .await;
    }

    pub async fn update_user_info<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        need_refresh: bool,
    ) where
        R: MediaRoom<T>,
    {
        room.update_user_info(
            user_id,
            connection_id,
            info,
            need_refresh,
            &self.transport_adapter,
        )
        .await;
    }

    pub async fn stage_publish<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool
    where
        R: MediaRoom<T>,
    {
        room.stage_publish(user_id, connection_id, stream_type, &self.transport_adapter)
            .await
    }

    pub async fn rollback_staged_publish<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool
    where
        R: MediaRoom<T>,
    {
        room.rollback_staged_publish(user_id, connection_id, stream_type, &self.transport_adapter)
            .await
    }

    pub async fn rollback_connection_publishes<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) where
        R: MediaRoom<T>,
    {
        room.rollback_connection_publishes(user_id, connection_id, &self.transport_adapter)
            .await;
    }

    pub async fn unpublish<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool
    where
        R: MediaRoom<T>,
    {
        room.unpublish(user_id, connection_id, stream_type, &self.transport_adapter)
            .await
    }

    async fn apply_transport_answer<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, SfuCoreError>
    where
        R: MediaRoom<T>,
    {
        let session_key = room.transport_user_key(user_id, connection_id);
        self.transport_adapter
            .apply_session_answer(&session_key, answer_sdp)
            .await
            .map_err(SfuCoreError::Transport)
    }

    async fn commit_staged_publishes<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        applied_answer: &AppliedSessionAnswer,
    ) where
        R: MediaRoom<T>,
    {
        room.commit_staged_publishes(
            user_id,
            connection_id,
            applied_answer,
            &self.transport_adapter,
        )
        .await;
    }
}

impl From<SessionOffer> for MediaNegotiationOffer {
    fn from(offer: SessionOffer) -> Self {
        let (sdp, upload_slots) = offer.into_parts();
        Self {
            sdp,
            upload_slots: upload_slots
                .into_iter()
                .map(MediaUploadSlot::from)
                .collect(),
        }
    }
}

impl From<SessionUploadSlot> for MediaUploadSlot {
    fn from(slot: SessionUploadSlot) -> Self {
        Self {
            mid: slot.mid,
            kind: slot.kind,
            codecs: slot.codecs,
            simulcast_encodings: slot
                .simulcast_encodings
                .into_iter()
                .map(MediaUploadEncoding::from)
                .collect(),
        }
    }
}

impl From<SessionUploadEncoding> for MediaUploadEncoding {
    fn from(encoding: SessionUploadEncoding) -> Self {
        Self {
            rid: encoding.rid,
            max_bitrate: encoding.max_bitrate,
        }
    }
}
