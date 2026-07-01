//! `SfuCore` admits room users into [`MediaSession`] handles
//! each handle represent one admitted user connection in one room
//! callers use it to drive the browser offer/answer lifecycle and then express
//! publish, subscribe, recording and cleanup intent without improting room or
//! transport internals
//!
//! ```text
//! SfuCore::admit_user -> MediaSession
//!
//! establish -> send offer -> answer
//! publish   -> maybe send renegotiation -> answer
//! subscribe -> room state only
//! close     -> rollback staged media and remove the connection
//! ```
//!
//! negotiation is serialize through `&mut MediaSession`
//! a new offer is not craeted while another offer is awaiting an answer
//! publish intent received during that widnow is queued and replayed after the
//! accepted answer
//!
//! # Examples
//!
//! the server runtime follows this shape when a websocket user enter a room
//!
//! ```no_run
//! use o_sfu_core::{
//!     prelude::{NegotiationOffer, SfuCore, SourcePublishIntent},
//!     server::room::{JoinUserRequest, RoomManager},
//! };
//!
//! # async fn send_offer(_: NegotiationOffer) {}
//! # async fn example(
//! #     core: SfuCore,
//! #     rooms: RoomManager,
//! #     room_id: String,
//! #     request: JoinUserRequest,
//! #     publish_intent: SourcePublishIntent,
//! #     initial_answer_sdp: String,
//! # ) -> Result<(), o_sfu_core::prelude::SessionError> {
//! let mut session = core
//!     .admit_user(&rooms, &room_id, request)
//!     .await
//!     .expect("room admission succeeds");
//!
//! if let Some(offer) = session.establish().await? {
//!     send_offer(offer).await;
//! }
//!
//! if let Some(offer) = session.answer(&initial_answer_sdp).await? {
//!     send_offer(offer).await;
//! }
//!
//! if let Some(offer) = session.publish(publish_intent).await? {
//!     send_offer(offer).await;
//! }
//! # Ok(())
//! # }
//! ```
//!
use std::{collections::BTreeMap, mem::take, sync::Arc};

use crate::{
    Bitrate, ConnectionId,
    engine::{
        AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState, UserId,
        UserInfo,
        media_transport::{
            MediaTransport, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
            TransportAdapterError, TransportSessionHealth, TransportSessionKey,
        },
        room::{
            BroadcastPayloadError, JoinUserRequest, PublishIntentOutcome, Room, RoomManager,
            RoomManagerJoinError, RoomUserOperation, UnpublishIntentOutcome,
        },
        source_model::{
            SourcePublishIntent, SourceSubscriptionIntent, SourceUnpublishIntent, UserStreamId,
        },
    },
};

/// server-authored SDP offer plus upload metadata for the client
///
/// the SDP is transport state
/// callers should send it to the client unchanged and use `upload_slots` only
/// to project browser-facing source setup hints
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiationOffer {
    /// do not parse this outside the transport boundary for routing decisions
    pub sdp: String,
    /// media sections the client may publish after applying the offer
    pub upload_slots: Vec<UploadSlot>,
}

/// one offered upload media section
///
/// `mid` binds this slot to the SDP media section
/// `kind`, `codecs` and `simulcast_encodings` are compatibility metadata for
/// the protocol layer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSlot {
    /// SDP media section id for this upload slot
    pub mid: String,
    /// audio or video kind accepted on this media section
    pub kind: o_sfu_router::MediaKind,
    /// codec names accepted for this upload slot
    pub codecs: Vec<String>,
    /// simulcast layers the client may announce for this upload slot
    pub simulcast_encodings: Vec<UploadEncoding>,
}

/// one upload encoding offered to the client
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadEncoding {
    /// RTP stream id that should appear in RID or simulcast signaling
    pub rid: String,
    /// optional send bitrate ceiling for this encoding
    pub max_bitrate: Option<Bitrate>,
    /// optional inverse scale from source resolution
    pub resolution_scale: Option<u16>,
    /// optional frame-rate ceiling for this encoding
    pub max_framerate: Option<u16>,
}

/// media-session negotiation phase
///
/// queued publishes live beside the offer state so a user action that arrives
/// while a browser answer is pneding cannot interleave with the in-flight SDP
/// exchange
#[derive(Debug, Default)]
enum SessionPhase {
    /// no initial offer has been created yet
    #[default]
    BeforeInitialOffer,
    /// no offer is awaiting an answer
    Stable {
        queued_publishes: BTreeMap<UserStreamId, SourcePublishIntent>,
    },
    /// one offer has been sent and must be answered before the next offer
    WaitingForAnswer(InFlightOffer),
}

/// in-flight offer state plus mutations deferred until its answer is accepted
#[derive(Debug)]
struct InFlightOffer {
    purpose: SessionOfferPurpose,
    queued_publishes: BTreeMap<UserStreamId, SourcePublishIntent>,
    follow_up_renegotiation: bool,
}

/// outcome of asking the phase machine for a new renegotiation offer
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

/// reason an offer is waiting for an answer
#[derive(Debug, Clone)]
enum SessionOfferPurpose {
    EstablishSession,
    RefreshSession,
}

/// error returned by [`MediaSession`] operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    /// [`MediaSession::answer`] was called without an in-flight offer
    #[error("no pending media request")]
    NoPendingRequest,
    /// lower core operation failed or rejected the request
    #[error(transparent)]
    Core(#[from] SfuCoreError),
}

impl SessionError {
    /// whether the runtime should report the error as a client or protocol fault
    ///
    /// transport failures that indicate malformed client input are client
    /// errors, while infrastructure failures are internal errors
    #[must_use]
    pub const fn is_client_error(self) -> bool {
        match self {
            Self::NoPendingRequest => true,
            Self::Core(error) => error.is_client_error(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SfuCoreError {
    /// media transport command failed
    #[error("transport operation failed")]
    Transport(#[source] TransportAdapterError),
    /// accepted answer did not yield client capabilities needed by room state
    #[error("capability projection failed")]
    CapabilityProjection(#[source] TransportAdapterError),
    /// initial answer was valid transport input but stale for room state
    #[error("session negotiation rejected")]
    SessionNegotiationRejected,
    /// refresh answer was valid transport input but stale for room state
    #[error("session refresh rejected")]
    SessionRefreshRejected,
    /// subscription intent targeted a stale or invalid room connection
    #[error("subscription update rejected")]
    SubscriptionUpdateRejected,
}

impl SfuCoreError {
    /// whether this error should close the client as a protocol fault
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

/// cloneable core handle used to create room-bound media sessions
///
/// `SfuCore` owns no room state
/// it holds the media transport service that every [`MediaSession`] uses when a
/// room mutation needs WebRTC work
#[derive(Debug, Clone)]
pub struct SfuCore {
    media_transport: MediaTransport,
}

/// one user connection in one room
///
/// mutating methods revalidate the connection through `RoomUserOperation`
/// before committing room state
/// if another connection replaced the user, negotiation or subscription calls
/// return client-visible errors and [`close`](Self::close) rolls back staged
/// media without removing the replacement
#[derive(Debug)]
pub struct MediaSession {
    core: SfuCore,
    room: Arc<Room>,
    transport_user_key: TransportSessionKey,
    phase: SessionPhase,
    closed: bool,
}

impl SfuCore {
    #[must_use]
    pub fn new(media_transport: MediaTransport) -> Self {
        Self { media_transport }
    }

    /// admits one room user and returns the media session that owns the
    /// connection
    ///
    /// # Errors
    ///
    /// returns [`RoomManagerJoinError`] when the room is missing or admission
    /// rejects the user
    pub async fn admit_user(
        &self,
        rooms: &RoomManager,
        room_id: &str,
        request: JoinUserRequest,
    ) -> Result<MediaSession, RoomManagerJoinError> {
        let admission = rooms
            .join_user(room_id, request, &self.media_transport)
            .await?;
        Ok(self.session_with_transport_key(&admission.room, admission.transport_session_key))
    }

    /// builds a media session for an existing room user
    ///
    /// production websocket admission should use [`SfuCore::admit_user`]
    /// instead, so the caller does not handle transport session keys
    #[must_use]
    pub async fn session(
        &self,
        room: &Arc<Room>,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> MediaSession {
        let transport_user_key = room.transport_user_key(user_id, connection_id).await;
        self.session_with_transport_key(room, transport_user_key)
    }

    /// builds a media session from an already committed transport key
    #[must_use]
    fn session_with_transport_key(
        &self,
        room: &Arc<Room>,
        transport_user_key: TransportSessionKey,
    ) -> MediaSession {
        MediaSession {
            core: self.clone(),
            room: Arc::clone(room),
            transport_user_key,
            phase: SessionPhase::default(),
            closed: false,
        }
    }
}

impl MediaSession {
    /// creates the first browser offer for this connection
    ///
    /// returns `Ok(None)` after the initial offer has already been requested
    /// this lets reconnect or duplicate-start paths retry safely without
    /// creating a second initial offer
    ///
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

    /// accepts the answer for the pending offer and commits any ready room work
    ///
    /// an invalid answer leaves the pending offer in place, so the caller may
    /// pass a later valid answer for the same request
    /// when queued publish intent needs another SDP round the returned offer
    /// must be sent to the client before the next answer
    ///
    /// # Errors
    ///
    /// returns [`SessionError::NoPendingRequest`] when no offer is pending
    /// returns [`SessionError::Core`] when answer application fails, capability
    /// projection fails or room state rejects the accepted answer as stale
    pub async fn answer(&mut self, sdp: &str) -> Result<Option<NegotiationOffer>, SessionError> {
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
        self.room_operation()
            .commit_staged_publishes(&applied_answer)
            .await;
        let staged = self.stage_queued_publishes().await?;
        if staged || follow_up_renegotiation {
            return self.renegotiate().await;
        }
        Ok(None)
    }

    /// applies publish intent for one user stream
    ///
    /// returns no offer when the intent is already queued, already active or
    /// must wait for an in-flight answer
    /// returns an offer when the browser must answer a new offer before the
    /// publication can commit
    ///
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the media backend cannot stage a
    /// publish that needs negotiation
    pub async fn publish(
        &mut self,
        intent: SourcePublishIntent,
    ) -> Result<Option<NegotiationOffer>, SessionError> {
        let stream_id = intent.stream_id().clone();
        if self.phase.has_queued_publish(&stream_id) {
            return Ok(None);
        }
        match self
            .start_publish(&intent, self.phase.can_stage_publish())
            .await?
        {
            PublishIntentOutcome::Noop | PublishIntentOutcome::Activated => Ok(None),
            PublishIntentOutcome::Queue => {
                self.phase.queue_publish(stream_id, intent);
                self.phase.mark_follow_up_renegotiation();
                Ok(None)
            }
            PublishIntentOutcome::Staged => self.renegotiate().await,
        }
    }

    /// removes queued, staged or live publish intent for one stream
    ///
    /// queued and staged publishes are cancelled without a client-visible
    /// publication update
    /// live publications may require a refresh offer for the browser
    ///
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when follow-up renegotiation fails after
    /// removing a live publication
    pub async fn unpublish(
        &mut self,
        intent: SourceUnpublishIntent,
    ) -> Result<Option<NegotiationOffer>, SessionError> {
        if self.phase.remove_queued_publish(intent.stream_id()) {
            return Ok(None);
        }
        match self.room_operation().stop_publish(&intent).await {
            UnpublishIntentOutcome::RolledBack => {
                self.phase.mark_follow_up_renegotiation();
                Ok(None)
            }
            UnpublishIntentOutcome::Unpublished => self.renegotiate().await,
            UnpublishIntentOutcome::Noop => Ok(None),
        }
    }

    /// closes this media session and removes its room connection if still current
    ///
    /// the call is idempotent
    /// it returns `true` only when the room manager removed the current
    /// connection
    /// current-session cleanup drains connection-scoped staged media through
    /// room state
    /// stale sessions do not remove a replacement connection for the same user
    pub async fn close(&mut self, rooms: &RoomManager) -> bool {
        if self.closed {
            return false;
        }
        self.phase.clear_queued_publishes();
        let did_close = rooms
            .close_session(
                self.room_id(),
                self.user_id(),
                self.connection_id(),
                &self.core.media_transport,
            )
            .await;
        self.closed = true;
        did_close
    }

    /// creates a refresh offer when the stable session needs renegotiation
    ///
    /// returns `Ok(None)` before the initial offer, while an answer is pending
    /// or when the transport reports that the requested refresh is unsupported
    /// a call made while an answer is pending records that another offer should
    /// be created after the answer commits
    ///
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

    /// applies receiver intent for sources published by another user
    ///
    /// subscription intent is remembered even when no producer is currently
    /// routable
    /// once negotiation makes the receiver consumable, room effects create the
    /// missing consumer routes
    ///
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
    pub fn user_id(&self) -> &UserId {
        self.transport_user_key.user_id()
    }

    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.transport_user_key.connection_id()
    }

    #[must_use]
    pub fn room_id(&self) -> &str {
        self.room.uuid()
    }

    pub async fn is_current_connection(&self) -> bool {
        self.room
            .has_connection(self.user_id(), self.connection_id())
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
        self.room.user_snapshots_except(self.user_id()).await
    }

    fn room_operation(&self) -> RoomUserOperation<'_> {
        self.room.user_operation(
            self.user_id(),
            self.connection_id(),
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
                self.user_id(),
                self.connection_id(),
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
            .broadcast(self.user_id(), self.connection_id(), message)
            .await
    }

    /// starts recording if recording is enabled and this connection may control it
    ///
    /// the facade remains async for compatibility with callers that await the
    /// recording API even when the current implementation rejects recording
    /// synchronously
    #[must_use]
    #[expect(
        clippy::unused_async,
        reason = "keeps the public MediaSession recording facade async while disabled recording is synchronous"
    )]
    pub async fn start_recording(&self, options: RecordingOptions) -> bool {
        self.room
            .apply_recording_start(self.user_id(), self.connection_id(), options)
    }

    /// stops recording if recording is enabled and this connection may control it
    ///
    /// returns `false` when recording is disabled or the room rejects the
    /// control request
    #[must_use]
    #[expect(
        clippy::unused_async,
        reason = "keeps the public MediaSession recording facade async while disabled recording is synchronous"
    )]
    pub async fn stop_recording(&self) -> bool {
        self.room
            .apply_recording_stop(self.user_id(), self.connection_id())
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
