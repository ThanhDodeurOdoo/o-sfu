//! Canonical media-core facade.
//!
//! This module keeps the public media API centered on [`SfuCore::session`].
//! A caller creates one [`MediaSession`] for a room user connection and then
//! expresses media intent through that handle. The handle keeps room identity,
//! user identity and runtime connection identity together so outer
//! orchestration does not pass the same tuple through every call.

use o_sfu_router::MediaCapabilities;

use crate::{
    ConnectionId, CoreOptions, MediaRoom, MediaSessionContext, PublicationActivity,
    PublicationActivityOutcome, PublishStageOutcome, RollbackStagedPublishOutcome,
    SessionNegotiationOutcome, SubscriptionUpdateOutcome, UnpublishOutcome, UserInfoRefresh,
    runtime::{DownloadStates, StreamType, UserId, UserInfo},
    transport::{
        AppliedSessionAnswer, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
        TransportAdapterError, TransportFacade, TransportSessionHealth,
    },
};

/// Router capabilities captured when an initial offer is created.
///
/// Pass this value back to [`MediaSession::apply_initial_answer`] for the
/// matching answer. It binds answer projection to the exact router capability
/// set that was advertised to the browser.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OfferedMediaCapabilities(MediaCapabilities);

/// Transport-neutral SDP offer returned by the media-core public API.
///
/// The transport adapter still owns the backend-specific `SessionOffer` shape.
/// `NegotiationOffer` is the stable core-facing vocabulary consumed by server
/// signaling code and mapped to the compatibility websocket payload at the
/// protocol edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiationOffer {
    /// SDP offer body sent by the signaling edge.
    ///
    /// Routing decisions should use typed room state and upload-slot metadata
    /// rather than parsing this string outside the transport boundary.
    pub sdp: String,
    /// Upload opportunities authored by the server for this offer.
    ///
    /// The browser bundle uses these slots to attach pending local tracks to
    /// the intended media sections without guessing from raw SDP.
    pub upload_slots: Vec<UploadSlot>,
}

/// Upload opportunity advertised by a core negotiation offer.
///
/// A slot is not a live publication. It becomes live only after the client
/// answers and the room commits the staged publish for this session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSlot {
    /// SDP media id that correlates the slot with one offer media section.
    pub mid: String,
    /// Technical media kind expected by the router and transport.
    pub kind: o_sfu_router::MediaKind,
    /// Codec names the sender may use for this slot.
    ///
    /// This is upload-policy metadata. The room still validates the answered
    /// transport parameters before a staged publish becomes live.
    pub codecs: Vec<String>,
    /// Optional sender encoding constraints for simulcast or future SVC paths.
    pub simulcast_encodings: Vec<UploadEncoding>,
}

/// Upload encoding constraint advertised for one sender layer.
///
/// These constraints guide browser sender setup. They do not replace the final
/// transport parameters extracted from the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadEncoding {
    /// RID the browser should use for this encoding layer.
    pub rid: String,
    /// Sender-side bitrate ceiling for this layer when the offer declares one.
    pub max_bitrate: Option<u64>,
}

#[deprecated(
    since = "0.3.0",
    note = "use NegotiationOffer; MediaNegotiationOffer was the transitional Workstream 2 name"
)]
pub type MediaNegotiationOffer = NegotiationOffer;

#[deprecated(
    since = "0.3.0",
    note = "use UploadSlot; MediaUploadSlot was the transitional Workstream 2 name"
)]
pub type MediaUploadSlot = UploadSlot;

#[deprecated(
    since = "0.3.0",
    note = "use UploadEncoding; MediaUploadEncoding was the transitional Workstream 2 name"
)]
pub type MediaUploadEncoding = UploadEncoding;

/// Best-effort transport health for a media endpoint.
///
/// This reports what the transport backend currently knows about the endpoint.
/// It is not an authoritative room membership check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaEndpointHealth {
    /// The transport endpoint is still usable.
    Connected,
    /// The transport endpoint reached a terminal disconnected state.
    Disconnected,
}

/// Errors returned by the media-core session facade.
///
/// # Error handling guidance
///
/// Transport and capability projection failures mean the media backend could
/// not apply or interpret the SDP answer. Session negotiation rejections mean
/// the transport step completed, but room state refused the callback because
/// the connection no longer owned the user session. Callers should treat stale
/// connection outcomes as protocol races, not as transport outages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SfuCoreError {
    /// The transport backend rejected a media operation.
    Transport(TransportAdapterError),
    /// The answered SDP could not be projected into router-native capabilities.
    CapabilityProjection(TransportAdapterError),
    /// Room state rejected the initial answer after transport accepted it.
    SessionNegotiationRejected(SessionNegotiationOutcome),
    /// Room state rejected a follow-up answer after transport accepted it.
    SessionRefreshRejected(SessionNegotiationOutcome),
}

/// Process-wide media facade.
///
/// `SfuCore` owns immutable core options and one transport backend. It does
/// not own websocket state, room membership or compatibility protocol mapping.
/// Those stay in the server runtime and room engine.
///
/// # Boundary role
///
/// Use this type to create [`MediaSession`] handles. Media operations are
/// intentionally not exposed as tuple-heavy methods on `SfuCore`, because a
/// room, user id and connection id must stay paired for the whole operation.
#[derive(Debug, Clone)]
pub struct SfuCore<T> {
    _options: CoreOptions,
    transport_adapter: T,
}

/// Borrowed media handle for one room user connection.
///
/// A `MediaSession` carries the room, user identity, connection identity and
/// transport session key that belong together. Reuse it inside one
/// orchestration step when several media operations target the same live
/// connection.
///
/// # Lifecycle
///
/// The handle does not keep the user connected. Mutating operations still ask
/// room state whether the connection is current before they commit. This is how
/// stale websocket callbacks become explicit outcomes instead of mutating a
/// replacement user.
///
/// # Concurrency model
///
/// Session methods are cold-path orchestration calls. They may await room
/// locks, transport commands and cleanup effects. The room boundary remains
/// responsible for releasing state locks before awaiting transport work.
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
    /// Build a media core facade over one transport backend.
    ///
    /// Production server code normally uses [`crate::RuntimeSfuCore`], which
    /// fixes the backend to [`crate::MediaTransport`]. Tests and future
    /// adapters can use any backend that satisfies [`TransportFacade`].
    #[must_use]
    pub fn new(options: CoreOptions, transport_adapter: T) -> Self {
        Self {
            _options: options,
            transport_adapter,
        }
    }

    /// Create the canonical media handle for one room user connection.
    ///
    /// This is cheap and borrow-based. It computes the transport session key
    /// from the room identity, user id and connection id, then keeps that key
    /// with the room context for later calls.
    ///
    /// The method does not validate room membership by itself. Each mutating
    /// operation validates the current connection at the point where room state
    /// would change.
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
}

impl<R, T> MediaSession<'_, R, T>
where
    T: TransportFacade,
    R: MediaRoom<T>,
{
    /// Return the current transport health for this endpoint.
    ///
    /// `None` means the transport backend has no endpoint for this session key.
    /// It does not prove that the user is absent from the room.
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

    /// Create the first transport offer for this session.
    ///
    /// Store the returned [`OfferedMediaCapabilities`] with the pending request
    /// and pass it back to [`Self::apply_initial_answer`]. The token preserves
    /// the capability set used for this offer.
    ///
    /// # Errors
    ///
    /// Returns [`SfuCoreError::Transport`] when the transport backend cannot
    /// create the offer.
    pub async fn create_initial_offer(
        &self,
    ) -> Result<(NegotiationOffer, OfferedMediaCapabilities), SfuCoreError> {
        let offered_capabilities =
            OfferedMediaCapabilities(self.room.router_rtp_capabilities().await);
        let offer = self
            .core
            .transport_adapter
            .create_initial_session_offer(self.context.transport_user_key())
            .await
            .map_err(SfuCoreError::Transport)?;
        Ok((NegotiationOffer::from(offer), offered_capabilities))
    }

    /// Create a follow-up offer after room state staged a media change.
    ///
    /// `Ok(None)` is an ordinary no-op for backends or states that cannot emit
    /// a renegotiation offer. Other transport failures are returned as errors.
    pub async fn create_renegotiation_offer(
        &self,
    ) -> Result<Option<NegotiationOffer>, SfuCoreError> {
        match self
            .core
            .transport_adapter
            .create_session_renegotiation_offer(self.context.transport_user_key())
            .await
        {
            Ok(offer) => Ok(Some(NegotiationOffer::from(offer))),
            Err(TransportAdapterError::UnsupportedFeature) => Ok(None),
            Err(error) => Err(SfuCoreError::Transport(error)),
        }
    }

    /// Apply the browser answer for the first offer.
    ///
    /// The transport answer is applied first, then the answered SDP is
    /// projected into router-native client capabilities. Room state is marked
    /// negotiated only after those transport steps succeed. Any staged publish
    /// work made valid by the answer is committed last.
    ///
    /// # Errors
    ///
    /// Transport and capability projection errors mean answer application did
    /// not complete. [`SfuCoreError::SessionNegotiationRejected`] means room
    /// state rejected the callback, usually because the connection became
    /// stale while the browser was answering.
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
        let outcome = self
            .room
            .apply_session_negotiated(
                &self.context,
                client_capabilities,
                &self.core.transport_adapter,
            )
            .await;
        if outcome != SessionNegotiationOutcome::Applied {
            return Err(SfuCoreError::SessionNegotiationRejected(outcome));
        }
        self.commit_staged_publishes(&applied_answer).await;
        Ok(())
    }

    /// Apply the browser answer for a renegotiation offer.
    ///
    /// The transport answer is applied first because it owns final negotiated
    /// media parameters. Room state is refreshed afterward and may reject stale
    /// callbacks from replaced connections. Any staged publish work made valid
    /// by the answer is committed last.
    ///
    /// # Errors
    ///
    /// Returns transport errors from answer application or
    /// [`SfuCoreError::SessionRefreshRejected`] when room state rejects the
    /// refresh.
    pub async fn apply_renegotiation_answer(&self, answer_sdp: &str) -> Result<(), SfuCoreError> {
        let applied_answer = self.apply_transport_answer(answer_sdp).await?;
        let outcome = self
            .room
            .apply_session_refreshed(&self.context, &self.core.transport_adapter)
            .await;
        if outcome != SessionNegotiationOutcome::Applied {
            return Err(SfuCoreError::SessionRefreshRejected(outcome));
        }
        self.commit_staged_publishes(&applied_answer).await;
        Ok(())
    }

    /// Check whether this connection already has a staged publish for a stream.
    ///
    /// This is an idempotency hint for websocket orchestration. It is not an
    /// authority to commit media by itself because another task can still win
    /// or clean up the staged transaction before the answer arrives.
    pub async fn has_staged_publish(&self, stream_type: StreamType) -> bool {
        self.room
            .has_staged_publish(&self.context, stream_type)
            .await
    }

    /// Check whether the room currently has a live publication for this user.
    ///
    /// The result is room-authoritative at the moment of the state read. It
    /// does not reserve the stream against a concurrent unpublish or user
    /// replacement.
    pub async fn is_stream_published(&self, stream_type: StreamType) -> bool {
        self.room
            .is_stream_published(&self.context, stream_type)
            .await
    }

    /// Sets the user-visible activity state for an already published stream.
    ///
    /// The outcome reports both room acceptance and the best-effort transport
    /// projection. A transport update failure does not mean the room-visible
    /// activity change was rejected.
    pub async fn set_publication_activity(
        &self,
        stream_type: StreamType,
        activity: PublicationActivity,
    ) -> PublicationActivityOutcome {
        self.room
            .set_publication_active(
                &self.context,
                stream_type,
                activity,
                &self.core.transport_adapter,
            )
            .await
    }

    /// Persists this session's subscription intent for a target user.
    ///
    /// The returned outcome is room-authoritative. A stale connection means the
    /// caller is acting on a session that has already been replaced or removed.
    pub async fn update_subscription(
        &self,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) -> SubscriptionUpdateOutcome {
        self.room
            .update_subscription(
                &self.context,
                target_user_id,
                states,
                &self.core.transport_adapter,
            )
            .await
    }

    /// Update room-visible user information for this connection.
    ///
    /// `refresh` controls whether other users need a full snapshot or a normal
    /// incremental update. Stale connections are ignored by the room boundary.
    pub async fn update_user_info(&self, info: UserInfo, refresh: UserInfoRefresh) {
        self.room
            .update_user_info(&self.context, info, refresh, &self.core.transport_adapter)
            .await;
    }

    /// Reserves media for a publish that still needs renegotiation.
    ///
    /// `Ok(PublishStageOutcome::Staged)` means the caller should request a new
    /// offer. Duplicate and rejected outcomes are ordinary domain decisions.
    /// `Err` means the transport could not reserve media and the publish cannot
    /// safely continue.
    pub async fn stage_publish(
        &self,
        stream_type: StreamType,
    ) -> Result<PublishStageOutcome, SfuCoreError> {
        self.room
            .stage_publish(&self.context, stream_type, &self.core.transport_adapter)
            .await
            .map_err(SfuCoreError::Transport)
    }

    /// Cancels a pending publish reservation before it becomes a live track.
    ///
    /// The cleanup result is part of the returned outcome because rollback
    /// consumes staged ownership even when transport cleanup races with session
    /// teardown.
    pub async fn rollback_staged_publish(
        &self,
        stream_type: StreamType,
    ) -> RollbackStagedPublishOutcome {
        self.room
            .rollback_staged_publish(&self.context, stream_type, &self.core.transport_adapter)
            .await
    }

    /// Roll back every staged publish owned by this connection.
    ///
    /// User replacement, websocket close and failed admission use this as
    /// best-effort cleanup for in-flight publish reservations. It does not
    /// close the transport session itself.
    pub async fn rollback_connection_publishes(&self) {
        self.room
            .rollback_connection_publishes(&self.context, &self.core.transport_adapter)
            .await;
    }

    /// Removes a live publication owned by this exact session.
    ///
    /// Missing publications are normal no-ops. Cleanup or state commit failures
    /// are explicit outcomes so callers do not infer failure reasons from a
    /// collapsed boolean.
    pub async fn unpublish(&self, stream_type: StreamType) -> UnpublishOutcome {
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
        }
    }
}
