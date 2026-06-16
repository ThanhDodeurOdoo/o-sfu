use std::{collections::BTreeMap, mem::take, sync::Arc};

use crate::{
    Bitrate, ConnectionId, PublishIntentOutcome, UnpublishIntentOutcome,
    engine::{
        AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState, UserId,
        UserInfo,
        media_transport::{
            MediaTransport, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
            TransportAdapterError, TransportSessionHealth, TransportSessionKey,
        },
        room::{BroadcastPayloadError, Room, RoomUserOperation},
        source_model::{SourcePublishIntent, SourceSubscriptionIntent, UserStreamId},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiationOffer {
    /// do not parse this outside the transport boundary for routing decisions
    pub sdp: String,
    pub upload_slots: Vec<UploadSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSlot {
    pub mid: String,
    pub kind: o_sfu_router::MediaKind,
    pub codecs: Vec<String>,
    pub simulcast_encodings: Vec<UploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadEncoding {
    pub rid: String,
    pub max_bitrate: Option<Bitrate>,
    pub resolution_scale: Option<u16>,
    pub max_framerate: Option<u16>,
}

#[derive(Debug, Default)]
enum SessionPhase {
    #[default]
    BeforeInitialOffer,
    Stable {
        queued_publishes: BTreeMap<UserStreamId, SourcePublishIntent>,
    },
    WaitingForAnswer(InFlightOffer),
}

#[derive(Debug)]
struct InFlightOffer {
    purpose: SessionOfferPurpose,
    queued_publishes: BTreeMap<UserStreamId, SourcePublishIntent>,
    follow_up_renegotiation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenegotiationDecision {
    CreateOfferNow,
    QueuedAfterAnswer,
    IgnoredBeforeInitialOffer,
}

impl SessionPhase {
    fn can_stage_publish(&self) -> bool {
        !matches!(self, Self::WaitingForAnswer(_))
    }

    fn has_queued_publish(&self, stream_id: &UserStreamId) -> bool {
        match self {
            Self::BeforeInitialOffer => false,
            Self::Stable { queued_publishes } => queued_publishes.contains_key(stream_id),
            Self::WaitingForAnswer(pending) => pending.queued_publishes.contains_key(stream_id),
        }
    }

    fn queue_publish(&mut self, stream_id: UserStreamId, intent: SourcePublishIntent) {
        match self {
            Self::BeforeInitialOffer => {}
            Self::Stable { queued_publishes } => {
                queued_publishes.insert(stream_id, intent);
            }
            Self::WaitingForAnswer(pending) => {
                pending.queued_publishes.insert(stream_id, intent);
            }
        }
    }

    fn remove_queued_publish(&mut self, stream_id: &UserStreamId) -> bool {
        match self {
            Self::BeforeInitialOffer => false,
            Self::Stable { queued_publishes } => queued_publishes.remove(stream_id).is_some(),
            Self::WaitingForAnswer(pending) => pending.queued_publishes.remove(stream_id).is_some(),
        }
    }

    fn clear_queued_publishes(&mut self) {
        match self {
            Self::BeforeInitialOffer => {}
            Self::Stable { queued_publishes } => queued_publishes.clear(),
            Self::WaitingForAnswer(pending) => pending.queued_publishes.clear(),
        }
    }

    fn request_renegotiation(&mut self) -> RenegotiationDecision {
        match self {
            Self::BeforeInitialOffer => RenegotiationDecision::IgnoredBeforeInitialOffer,
            Self::Stable { .. } => RenegotiationDecision::CreateOfferNow,
            Self::WaitingForAnswer(pending) => {
                pending.follow_up_renegotiation = true;
                RenegotiationDecision::QueuedAfterAnswer
            }
        }
    }

    fn mark_follow_up_renegotiation(&mut self) {
        if let Self::WaitingForAnswer(pending) = self {
            pending.follow_up_renegotiation = true;
        }
    }

    fn wait_for_answer(&mut self, purpose: SessionOfferPurpose) {
        *self = Self::WaitingForAnswer(InFlightOffer {
            purpose,
            queued_publishes: BTreeMap::new(),
            follow_up_renegotiation: false,
        });
    }

    fn pending_offer_purpose(&self) -> Option<&SessionOfferPurpose> {
        let Self::WaitingForAnswer(pending) = self else {
            return None;
        };
        Some(&pending.purpose)
    }

    fn complete_answer(&mut self) -> Option<bool> {
        let Self::WaitingForAnswer(pending) = self else {
            return None;
        };
        let queued_publishes = take(&mut pending.queued_publishes);
        let follow_up_renegotiation = pending.follow_up_renegotiation;
        *self = Self::Stable { queued_publishes };
        Some(follow_up_renegotiation)
    }

    fn take_queued_publishes(&mut self) -> BTreeMap<UserStreamId, SourcePublishIntent> {
        match self {
            Self::BeforeInitialOffer => BTreeMap::new(),
            Self::Stable { queued_publishes } => take(queued_publishes),
            Self::WaitingForAnswer(pending) => take(&mut pending.queued_publishes),
        }
    }
}

#[derive(Debug, Clone)]
enum SessionOfferPurpose {
    EstablishSession,
    RefreshSession,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    #[error("no pending media request")]
    NoPendingRequest,
    #[error(transparent)]
    Core(#[from] SfuCoreError),
}

impl SessionError {
    #[must_use]
    pub const fn is_client_error(self) -> bool {
        match self {
            Self::NoPendingRequest => true,
            Self::Core(error) => error.is_client_error(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    Renegotiation(NegotiationOffer),
    Publication {
        stream_id: UserStreamId,
        active: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SfuCoreError {
    #[error("transport operation failed")]
    Transport(#[source] TransportAdapterError),
    #[error("capability projection failed")]
    CapabilityProjection(#[source] TransportAdapterError),
    #[error("session negotiation rejected")]
    SessionNegotiationRejected,
    #[error("session refresh rejected")]
    SessionRefreshRejected,
    #[error("subscription update rejected")]
    SubscriptionUpdateRejected,
}

impl SfuCoreError {
    #[must_use]
    pub const fn is_client_error(self) -> bool {
        matches!(
            self,
            Self::Transport(TransportAdapterError::InvalidInput)
                | Self::CapabilityProjection(_)
                | Self::SessionNegotiationRejected
                | Self::SessionRefreshRejected
                | Self::SubscriptionUpdateRejected
        )
    }
}

#[derive(Debug, Clone)]
pub struct SfuCore {
    media_transport: MediaTransport,
}

/// mutating calls revalidate the connection before committing room state
#[derive(Debug)]
pub struct MediaSession {
    core: SfuCore,
    room: Arc<Room>,
    user_id: UserId,
    connection_id: ConnectionId,
    transport_user_key: TransportSessionKey,
    phase: SessionPhase,
}

impl SfuCore {
    #[must_use]
    pub fn new(media_transport: MediaTransport) -> Self {
        Self { media_transport }
    }

    #[must_use]
    pub async fn session(
        &self,
        room: &Arc<Room>,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> MediaSession {
        let transport_user_key = room.transport_user_key(user_id, connection_id).await;
        self.session_with_transport_key(room, user_id, connection_id, transport_user_key)
    }

    #[must_use]
    pub fn session_with_transport_key(
        &self,
        room: &Arc<Room>,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_user_key: TransportSessionKey,
    ) -> MediaSession {
        MediaSession {
            core: self.clone(),
            room: Arc::clone(room),
            user_id: user_id.clone(),
            connection_id,
            transport_user_key,
            phase: SessionPhase::default(),
        }
    }
}

impl MediaSession {
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the transport cannot create the
    /// initial offer
    pub async fn establish(&mut self) -> Result<Option<NegotiationOffer>, SessionError> {
        if !matches!(self.phase, SessionPhase::BeforeInitialOffer) {
            return Ok(None);
        }
        let offer = self
            .core
            .media_transport
            .create_initial_session_offer(&self.transport_user_key)
            .await
            .map(NegotiationOffer::from)
            .map_err(SfuCoreError::Transport)?;
        self.phase
            .wait_for_answer(SessionOfferPurpose::EstablishSession);
        Ok(Some(offer))
    }

    /// # Errors
    ///
    /// returns [`SessionError::NoPendingRequest`] when no offer is pending
    /// returns [`SessionError::Core`] when answer application fails
    pub async fn answer(&mut self, sdp: &str) -> Result<Vec<SessionEvent>, SessionError> {
        let Some(purpose) = self.phase.pending_offer_purpose() else {
            return Err(SessionError::NoPendingRequest);
        };
        let applied_answer = self
            .core
            .media_transport
            .apply_session_answer(&self.transport_user_key, sdp)
            .await
            .map_err(SfuCoreError::Transport)?;
        match purpose {
            SessionOfferPurpose::EstablishSession => {
                let client_capabilities = applied_answer.client_capabilities().cloned().ok_or(
                    SfuCoreError::CapabilityProjection(TransportAdapterError::InvalidInput),
                )?;
                self.room_operation()
                    .apply_session_negotiated(client_capabilities)
                    .await
                    .ok_or(SfuCoreError::SessionNegotiationRejected)?;
            }
            SessionOfferPurpose::RefreshSession => {
                self.room_operation()
                    .apply_session_refreshed()
                    .await
                    .ok_or(SfuCoreError::SessionRefreshRejected)?;
            }
        }
        let Some(follow_up_renegotiation) = self.phase.complete_answer() else {
            return Err(SessionError::NoPendingRequest);
        };
        let committed = self
            .room_operation()
            .commit_staged_publishes(&applied_answer)
            .await;
        let mut events = committed
            .into_iter()
            .map(|stream_id| SessionEvent::Publication {
                stream_id,
                active: true,
            })
            .collect::<Vec<_>>();
        let staged = self.stage_queued_publishes().await?;
        if staged || follow_up_renegotiation {
            events.extend(self.renegotiate().await?.map(SessionEvent::Renegotiation));
        }
        Ok(events)
    }

    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the media backend cannot stage a
    /// publish that needs negotiation
    pub async fn publish(
        &mut self,
        intent: SourcePublishIntent,
    ) -> Result<Vec<SessionEvent>, SessionError> {
        let stream_id = intent.stream_id().clone();
        if self.phase.has_queued_publish(&stream_id) {
            return Ok(Vec::new());
        }
        match self
            .start_publish(&intent, self.phase.can_stage_publish())
            .await?
        {
            PublishIntentOutcome::Noop => Ok(Vec::new()),
            PublishIntentOutcome::Queue => {
                self.phase.queue_publish(stream_id, intent);
                self.phase.mark_follow_up_renegotiation();
                Ok(Vec::new())
            }
            PublishIntentOutcome::Activated => Ok(vec![SessionEvent::Publication {
                stream_id,
                active: true,
            }]),
            PublishIntentOutcome::Staged => self.renegotiation_event().await,
        }
    }

    /// # Errors
    ///
    /// returns [`SessionError::Core`] when follow-up renegotiation fails after
    /// removing a live publication
    pub async fn unpublish(
        &mut self,
        stream_id: &UserStreamId,
    ) -> Result<Vec<SessionEvent>, SessionError> {
        if self.phase.remove_queued_publish(stream_id) {
            return Ok(Vec::new());
        }
        match self.room_operation().stop_publish(stream_id).await {
            UnpublishIntentOutcome::RolledBack => {
                self.phase.mark_follow_up_renegotiation();
                Ok(Vec::new())
            }
            UnpublishIntentOutcome::Unpublished => {
                let mut events = vec![SessionEvent::Publication {
                    stream_id: stream_id.clone(),
                    active: false,
                }];
                if let Some(offer) = self.renegotiate().await? {
                    events.push(SessionEvent::Renegotiation(offer));
                }
                Ok(events)
            }
            UnpublishIntentOutcome::Noop => Ok(Vec::new()),
        }
    }

    pub async fn close(&mut self) {
        self.phase.clear_queued_publishes();
        self.room_operation()
            .rollback_staged_publishes_for_connection()
            .await;
    }

    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the transport rejects
    /// renegotiation
    pub async fn renegotiate(&mut self) -> Result<Option<NegotiationOffer>, SessionError> {
        match self.phase.request_renegotiation() {
            RenegotiationDecision::CreateOfferNow => {}
            RenegotiationDecision::QueuedAfterAnswer
            | RenegotiationDecision::IgnoredBeforeInitialOffer => return Ok(None),
        }
        let offer = match self
            .core
            .media_transport
            .create_session_renegotiation_offer(&self.transport_user_key)
            .await
        {
            Ok(offer) => NegotiationOffer::from(offer),
            Err(TransportAdapterError::UnsupportedFeature) => return Ok(None),
            Err(error) => return Err(SfuCoreError::Transport(error).into()),
        };
        self.phase
            .wait_for_answer(SessionOfferPurpose::RefreshSession);
        Ok(Some(offer))
    }

    /// # Errors
    ///
    /// returns [`SessionError::Core`] when room state rejects this connection
    /// as stale
    pub async fn subscribe(
        &self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Result<(), SessionError> {
        self.room_operation()
            .apply_receiver_intent(target_user_id, intents)
            .await
            .ok_or(SfuCoreError::SubscriptionUpdateRejected)?;
        Ok(())
    }

    /// returns `None` when the transport has no endpoint for this session key
    #[must_use]
    pub fn endpoint_health(&self) -> Option<TransportSessionHealth> {
        self.core
            .media_transport
            .session_transport_health(&self.transport_user_key)
    }

    #[must_use]
    pub fn room_id(&self) -> &str {
        self.room.uuid()
    }

    pub async fn is_current_connection(&self) -> bool {
        self.room
            .has_connection(&self.user_id, self.connection_id)
            .await
    }

    #[must_use]
    pub fn available_features(&self) -> AvailableFeatures {
        self.room.available_features()
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.room.recording_state().await
    }

    pub async fn peer_snapshots(&self) -> Vec<PeerSnapshot> {
        self.room.user_snapshots_except(&self.user_id).await
    }

    fn room_operation(&self) -> RoomUserOperation<'_> {
        self.room.user_operation(
            &self.user_id,
            self.connection_id,
            &self.core.media_transport,
        )
    }

    async fn start_publish(
        &self,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> Result<PublishIntentOutcome, SfuCoreError> {
        self.room_operation()
            .start_publish(intent, can_stage)
            .await
            .map_err(SfuCoreError::Transport)
    }

    pub async fn update_info(&self, info: UserInfo) {
        self.room
            .update_user_info(
                &self.user_id,
                self.connection_id,
                &self.core.media_transport,
                info,
            )
            .await;
    }

    /// # Errors
    ///
    /// returns [`BroadcastPayloadError`] when the payload exceeds the room
    /// broadcast byte limit or cannot be measured as serialized JSON
    pub async fn broadcast(&self, message: JsonPayload) -> Result<(), BroadcastPayloadError> {
        self.room
            .broadcast(&self.user_id, self.connection_id, message)
            .await
    }

    pub async fn start_recording(&self, options: RecordingOptions) -> bool {
        self.room
            .apply_recording_start(&self.user_id, self.connection_id, options)
            .await
    }

    pub async fn stop_recording(&self) -> bool {
        self.room
            .apply_recording_stop(&self.user_id, self.connection_id)
            .await
    }

    async fn renegotiation_event(&mut self) -> Result<Vec<SessionEvent>, SessionError> {
        Ok(self
            .renegotiate()
            .await?
            .map_or_else(Vec::new, |offer| vec![SessionEvent::Renegotiation(offer)]))
    }

    async fn stage_queued_publishes(&mut self) -> Result<bool, SessionError> {
        let queued = self.phase.take_queued_publishes();
        let mut staged = false;
        for intent in queued.into_values() {
            if self.start_publish(&intent, true).await? == PublishIntentOutcome::Staged {
                staged = true;
            }
        }
        Ok(staged)
    }
}

impl From<SessionOffer> for NegotiationOffer {
    fn from(offer: SessionOffer) -> Self {
        let (sdp, upload_slots) = offer.into_parts();
        Self {
            sdp,
            upload_slots: upload_slots.into_iter().map(UploadSlot::from).collect(),
        }
    }
}

impl From<SessionUploadSlot> for UploadSlot {
    fn from(slot: SessionUploadSlot) -> Self {
        Self {
            mid: slot.mid,
            kind: slot.kind,
            codecs: slot.codecs,
            simulcast_encodings: slot
                .simulcast_encodings
                .into_iter()
                .map(UploadEncoding::from)
                .collect(),
        }
    }
}

impl From<SessionUploadEncoding> for UploadEncoding {
    fn from(encoding: SessionUploadEncoding) -> Self {
        Self {
            rid: encoding.rid,
            max_bitrate: encoding.max_bitrate,
            resolution_scale: encoding.resolution_scale,
            max_framerate: encoding.max_framerate,
        }
    }
}
