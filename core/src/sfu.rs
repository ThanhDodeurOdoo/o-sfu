use o_sfu_router::MediaCapabilities;

use crate::{
    ConnectionId, CoreOptions, MediaRoom, MediaSessionContext, PublicationActivity,
    UserInfoRefresh,
    runtime::{DownloadStates, StreamType, UserId, UserInfo},
    transport::{
        AppliedSessionAnswer, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
        TransportAdapterError, TransportFacade, TransportSessionHealth,
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

#[derive(Debug)]
pub struct MediaSession<'a, R, T>
where
    T: TransportFacade,
    R: MediaRoom<T>,
{
    core: &'a SfuCore<T>,
    room: &'a R,
    context: MediaSessionContext<'a>,
}

impl<T> SfuCore<T>
where
    T: TransportFacade,
{
    #[must_use]
    pub fn new(options: CoreOptions, transport_adapter: T) -> Self {
        Self {
            _options: options,
            transport_adapter,
        }
    }

    #[must_use]
    pub fn session<'a, R>(
        &'a self,
        room: &'a R,
        user_id: &'a UserId,
        connection_id: ConnectionId,
    ) -> MediaSession<'a, R, T>
    where
        R: MediaRoom<T>,
    {
        MediaSession {
            core: self,
            room,
            context: room.media_session_context(user_id, connection_id),
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
        self.session(room, user_id, connection_id).endpoint_health()
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
        self.session(room, user_id, connection_id)
            .create_initial_offer()
            .await
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
        self.session(room, user_id, connection_id)
            .create_renegotiation_offer()
            .await
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
        self.session(room, user_id, connection_id)
            .apply_initial_answer(answer_sdp, offered_capabilities)
            .await
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
        self.session(room, user_id, connection_id)
            .apply_renegotiation_answer(answer_sdp)
            .await
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
        self.session(room, user_id, connection_id)
            .has_staged_publish(stream_type)
            .await
    }

    pub async fn is_stream_published<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool
    where
        R: MediaRoom<T>,
    {
        self.session(room, user_id, connection_id)
            .is_stream_published(stream_type)
            .await
    }

    pub async fn set_publication_active<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        activity: PublicationActivity,
    ) where
        R: MediaRoom<T>,
    {
        self.session(room, user_id, connection_id)
            .set_publication_activity(stream_type, activity)
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
        self.session(room, user_id, connection_id)
            .update_subscription(target_user_id, states)
            .await;
    }

    pub async fn update_user_info<R>(
        &self,
        room: &R,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
    ) where
        R: MediaRoom<T>,
    {
        self.session(room, user_id, connection_id)
            .update_user_info(info, refresh)
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
        self.session(room, user_id, connection_id)
            .stage_publish(stream_type)
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
        self.session(room, user_id, connection_id)
            .rollback_staged_publish(stream_type)
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
        self.session(room, user_id, connection_id)
            .rollback_connection_publishes()
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
        self.session(room, user_id, connection_id)
            .unpublish(stream_type)
            .await
    }
}

impl<R, T> MediaSession<'_, R, T>
where
    T: TransportFacade,
    R: MediaRoom<T>,
{
    #[must_use]
    pub fn endpoint_health(&self) -> Option<MediaEndpointHealth> {
        self.core
            .transport_adapter
            .session_transport_health(self.context.transport_user_key())
            .map(|health| match health {
                TransportSessionHealth::Connected => MediaEndpointHealth::Connected,
                TransportSessionHealth::Disconnected => MediaEndpointHealth::Disconnected,
            })
    }

    pub async fn create_initial_offer(
        &self,
    ) -> Result<(MediaNegotiationOffer, OfferedMediaCapabilities), SfuCoreError> {
        let offered_capabilities =
            OfferedMediaCapabilities(self.room.router_rtp_capabilities().await);
        let offer = self
            .core
            .transport_adapter
            .create_initial_session_offer(self.context.transport_user_key())
            .await
            .map_err(SfuCoreError::Transport)?;
        Ok((MediaNegotiationOffer::from(offer), offered_capabilities))
    }

    pub async fn create_renegotiation_offer(
        &self,
    ) -> Result<Option<MediaNegotiationOffer>, SfuCoreError> {
        match self
            .core
            .transport_adapter
            .create_session_renegotiation_offer(self.context.transport_user_key())
            .await
        {
            Ok(offer) => Ok(Some(MediaNegotiationOffer::from(offer))),
            Err(TransportAdapterError::UnsupportedFeature) => Ok(None),
            Err(error) => Err(SfuCoreError::Transport(error)),
        }
    }

    pub async fn apply_initial_answer(
        &self,
        answer_sdp: &str,
        offered_capabilities: &OfferedMediaCapabilities,
    ) -> Result<(), SfuCoreError> {
        let applied_answer = self.apply_transport_answer(answer_sdp).await?;
        let client_capabilities = self
            .core
            .transport_adapter
            .negotiated_client_rtp_capabilities(answer_sdp, &offered_capabilities.0)
            .map_err(SfuCoreError::CapabilityProjection)?;
        if !self
            .room
            .apply_session_negotiated(
                &self.context,
                client_capabilities,
                &self.core.transport_adapter,
            )
            .await
        {
            return Err(SfuCoreError::UserStateCommitRejected);
        }
        self.commit_staged_publishes(&applied_answer).await;
        Ok(())
    }

    pub async fn apply_renegotiation_answer(&self, answer_sdp: &str) -> Result<(), SfuCoreError> {
        let applied_answer = self.apply_transport_answer(answer_sdp).await?;
        if !self
            .room
            .apply_session_refreshed(&self.context, &self.core.transport_adapter)
            .await
        {
            return Err(SfuCoreError::UserStateRefreshRejected);
        }
        self.commit_staged_publishes(&applied_answer).await;
        Ok(())
    }

    pub async fn has_staged_publish(&self, stream_type: StreamType) -> bool {
        self.room
            .has_staged_publish(&self.context, stream_type)
            .await
    }

    pub async fn is_stream_published(&self, stream_type: StreamType) -> bool {
        self.room
            .is_stream_published(&self.context, stream_type)
            .await
    }

    pub async fn set_publication_activity(
        &self,
        stream_type: StreamType,
        activity: PublicationActivity,
    ) {
        self.room
            .set_publication_active(
                &self.context,
                stream_type,
                activity,
                &self.core.transport_adapter,
            )
            .await;
    }

    pub async fn update_subscription(&self, target_user_id: &UserId, states: &DownloadStates) {
        self.room
            .update_subscription(
                &self.context,
                target_user_id,
                states,
                &self.core.transport_adapter,
            )
            .await;
    }

    pub async fn update_user_info(&self, info: UserInfo, refresh: UserInfoRefresh) {
        self.room
            .update_user_info(&self.context, info, refresh, &self.core.transport_adapter)
            .await;
    }

    pub async fn stage_publish(&self, stream_type: StreamType) -> bool {
        self.room
            .stage_publish(&self.context, stream_type, &self.core.transport_adapter)
            .await
    }

    pub async fn rollback_staged_publish(&self, stream_type: StreamType) -> bool {
        self.room
            .rollback_staged_publish(&self.context, stream_type, &self.core.transport_adapter)
            .await
    }

    pub async fn rollback_connection_publishes(&self) {
        self.room
            .rollback_connection_publishes(&self.context, &self.core.transport_adapter)
            .await;
    }

    pub async fn unpublish(&self, stream_type: StreamType) -> bool {
        self.room
            .unpublish(&self.context, stream_type, &self.core.transport_adapter)
            .await
    }

    async fn apply_transport_answer(
        &self,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, SfuCoreError> {
        self.core
            .transport_adapter
            .apply_session_answer(self.context.transport_user_key(), answer_sdp)
            .await
            .map_err(SfuCoreError::Transport)
    }

    async fn commit_staged_publishes(&self, applied_answer: &AppliedSessionAnswer) {
        self.room
            .commit_staged_publishes(&self.context, applied_answer, &self.core.transport_adapter)
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
