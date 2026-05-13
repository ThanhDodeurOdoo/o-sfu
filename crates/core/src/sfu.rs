//! Canonical media-core facade.
//!
//! This module keeps the public media API centered on [`SfuCore::session`].
//! A caller creates one [`MediaSession`] for a room user connection and then
//! expresses media intent through that handle. The handle keeps room identity,
//! user identity and runtime connection identity together so outer
//! orchestration does not pass the same tuple through every call.
//!
//! # Business-layer API
//!
//! Application code that changes publication policy should use
//! [`SourcePublishIntent`] for upload intent and
//! [`SourceSubscriptionIntent`] keyed by [`UserStreamId`] for download intent.
//! Core does not accept compatibility stream labels on this facade. Those
//! labels must be translated at the application edge before calling
//! [`MediaSession::stage_publish`] or [`MediaSession::update_subscription`].

use std::collections::BTreeMap;

use o_sfu_router::MediaCapabilities;

use crate::{
    Bitrate, ConnectionId, CoreOptions, MediaSessionContext, PublicationActivity,
    PublicationActivityOutcome, PublishStageOutcome, RollbackStagedPublishOutcome,
    SessionNegotiationOutcome, SubscriptionUpdateOutcome, UnpublishOutcome, UserInfoRefresh,
    runtime::{
        UserId, UserInfo,
        media_transport::MediaTransport,
        room::Room,
        source_model::{
            SourcePublishIntent, SourceSubscriptionIntent, UploadLayerPolicyRole, UserStreamId,
        },
    },
    transport::{
        AppliedSessionAnswer, NegotiationPort, ObservabilityPort, SessionOffer,
        SessionUploadEncoding, SessionUploadSlot, TransportAdapterError, TransportSessionHealth,
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
/// The media transport still owns the backend-specific `SessionOffer` shape.
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
    pub max_bitrate: Option<Bitrate>,
    /// Sender-side resolution downscale for this layer.
    pub resolution_scale: Option<u16>,
    /// Optional sender-side frame-rate ceiling for this layer.
    pub max_framerate: Option<u16>,
    /// Server-owned role that explains how room policy may use this layer.
    pub policy_role: Option<UploadLayerPolicyRole>,
}

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
/// # Error handling
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
/// [`SfuCore`] is the main entry point for the media-core library. It owns the
/// process-wide configuration and the transport backend. It does not manage
/// websocket state or room membership because those belong to the server
/// runtime and room engine.
///
/// The core acts as a factory for [`MediaSession`] handles. By separating
/// process-wide resources from connection-specific logic, the API avoids
/// carrying technical dependencies like the transport handle through the
/// application layer.
///
/// # Usage
///
/// Usually one instance of [`SfuCore`] is created during process initialization
/// and shared across all user sessions.
///
/// ```ignore
/// let core = SfuCore::new(options, transport);
/// ```
#[derive(Debug, Clone)]
pub struct SfuCore {
    _options: CoreOptions,
    media_transport: MediaTransport,
}

/// Borrowed media handle for one room user connection.
///
/// [`MediaSession`] is the user interface of the core. It bundles a room, user
/// identity, connection identity, and the derived transport session key.
/// This grouping ensures that orchestration logic always operates on a
/// consistent tuple of identities without passing them as individual arguments
/// to every method.
///
/// # Lifecycle
///
/// Handles are intended to be short-lived and borrow-based. They should be
/// created at the start of an orchestration step and dropped once the step
/// is finished.
///
/// Holding a [`MediaSession`] does not guarantee the user is still connected
/// or present in the room. All mutating operations perform an authoritative
/// check against the room state before committing changes.
///
/// # Concurrency
///
/// Methods on this handle are cold-path orchestration calls. They may
/// involve awaiting room state locks, transport backend commands, or cleanup
/// side-effects. The room boundary is responsible for managing its own
/// synchronization to ensure that transport work does not hold room-wide
/// locks for extended periods.
///
/// # Examples
///
/// Creating a session and requesting an initial offer:
///
/// ```ignore
/// let session = core.session(&room, &user_id, connection_id);
/// let (offer, caps) = session.create_initial_offer().await?;
/// ```
///
/// Updating a subscription:
///
/// ```ignore
/// session.update_subscription(&target_user_id, &intents).await;
/// ```
#[derive(Debug)]
pub struct MediaSession<'a> {
    core: &'a SfuCore,
    room: &'a Room,
    context: MediaSessionContext<'a>,
}

impl SfuCore {
    /// Build a media core facade over the opaque runtime transport handle.
    #[must_use]
    pub fn new(options: CoreOptions, media_transport: MediaTransport) -> Self {
        Self {
            _options: options,
            media_transport,
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
    pub fn session<'a>(
        &'a self,
        room: &'a Room,
        user_id: &'a UserId,
        connection_id: ConnectionId,
    ) -> MediaSession<'a> {
        MediaSession {
            core: self,
            room,
            context: MediaSessionContext::new(
                user_id,
                connection_id,
                room.transport_user_key(user_id, connection_id),
            ),
        }
    }
}

impl MediaSession<'_> {
    /// Return the current transport health for this endpoint.
    ///
    /// None means the transport backend has no endpoint for this session key.
    /// It does not prove that the user is absent from the room.
    #[must_use]
    pub fn endpoint_health(&self) -> Option<MediaEndpointHealth> {
        self.core
            .media_transport
            .session_transport_health(self.context.transport_user_key())
            .map(|health| match health {
                TransportSessionHealth::Connected => MediaEndpointHealth::Connected,
                TransportSessionHealth::Disconnected => MediaEndpointHealth::Disconnected,
            })
    }

    /// Create the first transport offer for this session.
    ///
    /// The returned [`OfferedMediaCapabilities`] must be stored with the
    /// pending request and passed back to [`Self::apply_initial_answer`] to
    /// ensure that the answer is interpreted against the exact capability
    /// set used for the offer.
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
            .media_transport
            .create_initial_session_offer(self.context.transport_user_key())
            .await
            .map_err(SfuCoreError::Transport)?;
        Ok((NegotiationOffer::from(offer), offered_capabilities))
    }

    /// Create a follow-up offer after room state staged a media change.
    ///
    /// Some transport backends or states may not support renegotiation.
    /// In those cases this method returns Ok(None) as a normal no-op.
    /// Other transport failures are returned as errors.
    ///
    /// # Errors
    ///
    /// Returns [`SfuCoreError`] when the transport rejects renegotiation for a
    /// reason other than unsupported follow-up offers.
    pub async fn create_renegotiation_offer(
        &self,
    ) -> Result<Option<NegotiationOffer>, SfuCoreError> {
        match self
            .core
            .media_transport
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
    /// The operation applies the transport answer first, then projects the
    /// resulting SDP into router-native client capabilities. Room state is
    /// marked as negotiated only after these steps succeed, and any staged
    /// publishes made valid by the answer are committed last.
    ///
    /// # Errors
    ///
    /// Transport and capability projection errors mean answer application did
    /// not complete. [`SfuCoreError::SessionNegotiationRejected`] means room
    /// state rejected the callback because the connection became stale.
    pub async fn apply_initial_answer(
        &self,
        answer_sdp: &str,
        offered_capabilities: &OfferedMediaCapabilities,
    ) -> Result<Vec<UserStreamId>, SfuCoreError> {
        let applied_answer = self.apply_transport_answer(answer_sdp).await?;
        let client_capabilities = self
            .core
            .media_transport
            .negotiated_client_rtp_capabilities(answer_sdp, &offered_capabilities.0)
            .map_err(SfuCoreError::CapabilityProjection)?;
        let outcome = self
            .room
            .apply_session_negotiated(
                self.context.user_id(),
                self.context.connection_id(),
                client_capabilities,
                &self.core.media_transport,
            )
            .await;
        if outcome != SessionNegotiationOutcome::Applied {
            return Err(SfuCoreError::SessionNegotiationRejected(outcome));
        }
        Ok(self.commit_staged_publishes(&applied_answer).await)
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
    pub async fn apply_renegotiation_answer(
        &self,
        answer_sdp: &str,
    ) -> Result<Vec<UserStreamId>, SfuCoreError> {
        let applied_answer = self.apply_transport_answer(answer_sdp).await?;
        let outcome = self
            .room
            .apply_session_refreshed(
                self.context.user_id(),
                self.context.connection_id(),
                &self.core.media_transport,
            )
            .await;
        if outcome != SessionNegotiationOutcome::Applied {
            return Err(SfuCoreError::SessionRefreshRejected(outcome));
        }
        Ok(self.commit_staged_publishes(&applied_answer).await)
    }

    /// Check whether this connection already has a staged publish for a stream.
    ///
    /// This is an idempotency hint for websocket orchestration. It is not an
    /// authority to commit media because another task could win or clean up
    /// the staged transaction before the answer arrives.
    pub async fn has_staged_publish(&self, stream_id: &UserStreamId) -> bool {
        self.room
            .has_staged_publish(
                self.context.user_id(),
                self.context.connection_id(),
                stream_id,
            )
            .await
    }

    /// Check whether the room currently has a live publication for this user.
    ///
    /// The result is room-authoritative at the moment of the state read. It
    /// does not reserve the stream against concurrent unpublish or user
    /// replacement.
    pub async fn is_stream_published(&self, stream_id: &UserStreamId) -> bool {
        self.room
            .is_stream_published(self.context.user_id(), stream_id)
            .await
    }

    /// Set the user-visible activity state for an already published stream.
    ///
    /// The outcome reports both room acceptance and best-effort transport
    /// projection. A transport update failure does not mean the room-visible
    /// activity change was rejected.
    pub async fn set_publication_activity(
        &self,
        stream_id: &UserStreamId,
        activity: PublicationActivity,
    ) -> PublicationActivityOutcome {
        self.room
            .set_publication_active_runtime(
                self.context.user_id(),
                self.context.connection_id(),
                stream_id,
                activity,
                &self.core.media_transport,
            )
            .await
    }

    /// Persist this session subscription intent for a target user.
    ///
    /// The returned outcome is room-authoritative. A stale connection outcome
    /// means the caller is acting on a session that has already been replaced
    /// or removed. The caller owns translation from compatibility download
    /// state into the generic per-stream map.
    pub async fn update_subscription(
        &self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> SubscriptionUpdateOutcome {
        self.room
            .update_subscription_runtime(
                self.context.user_id(),
                self.context.connection_id(),
                target_user_id,
                intents,
                &self.core.media_transport,
            )
            .await
    }

    /// Update room-visible user information for this connection.
    ///
    /// Refresh controls whether other users need a full snapshot or a normal
    /// incremental update. Stale connections are ignored by the room boundary.
    pub async fn update_user_info(&self, info: UserInfo, refresh: UserInfoRefresh) {
        self.room
            .update_user_info(
                self.context.user_id(),
                self.context.connection_id(),
                info,
                refresh,
                &self.core.media_transport,
            )
            .await;
    }

    /// Reserve media for a publish that still needs renegotiation.
    ///
    /// Returns `Ok(PublishStageOutcome::Staged)` when the caller should
    /// request a new offer. Duplicate and rejected outcomes are ordinary
    /// domain decisions. Err means the transport could not reserve media
    /// and the publish cannot safely continue.
    ///
    /// The intent represents the business-layer policy handoff. Core stores
    /// the stream ID as opaque identity and uses the attached source policy
    /// when applying receiver layout and bandwidth decisions.
    ///
    /// # Errors
    ///
    /// Returns [`SfuCoreError`] when the room or transport cannot reserve the
    /// publish transaction.
    pub async fn stage_publish(
        &self,
        intent: &SourcePublishIntent,
    ) -> Result<PublishStageOutcome, SfuCoreError> {
        self.room
            .stage_negotiated_publish(
                self.context.user_id(),
                self.context.connection_id(),
                intent,
                &self.core.media_transport,
            )
            .await
            .map_err(SfuCoreError::Transport)
    }

    /// Cancel a pending publish reservation before it becomes a live track.
    ///
    /// The cleanup result is part of the returned outcome because rollback
    /// consumes staged ownership even when transport cleanup races with
    /// session teardown.
    pub async fn rollback_staged_publish(
        &self,
        stream_id: &UserStreamId,
    ) -> RollbackStagedPublishOutcome {
        self.room
            .rollback_staged_publish(
                self.context.user_id(),
                self.context.connection_id(),
                stream_id,
                &self.core.media_transport,
            )
            .await
    }

    /// Roll back every staged publish owned by this connection.
    ///
    /// User replacement, websocket close, and failed admission use this as
    /// best-effort cleanup for in-flight publish reservations. It does not
    /// close the transport session itself.
    pub async fn rollback_connection_publishes(&self) {
        self.room
            .rollback_staged_publishes_for_connection(
                self.context.user_id(),
                self.context.connection_id(),
                &self.core.media_transport,
            )
            .await;
    }

    /// Remove a live publication owned by this exact session.
    ///
    /// Missing publications are normal no-ops. Cleanup or state commit failures
    /// are explicit outcomes so callers do not infer failure reasons from a
    /// collapsed boolean.
    pub async fn unpublish(&self, stream_id: &UserStreamId) -> UnpublishOutcome {
        self.room
            .unpublish_track(
                self.context.user_id(),
                self.context.connection_id(),
                stream_id,
                &self.core.media_transport,
            )
            .await
    }

    async fn apply_transport_answer(
        &self,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, SfuCoreError> {
        self.core
            .media_transport
            .apply_session_answer(self.context.transport_user_key(), answer_sdp)
            .await
            .map_err(SfuCoreError::Transport)
    }

    async fn commit_staged_publishes(
        &self,
        applied_answer: &AppliedSessionAnswer,
    ) -> Vec<UserStreamId> {
        self.room
            .commit_staged_publishes(
                self.context.user_id(),
                self.context.connection_id(),
                applied_answer,
                &self.core.media_transport,
                &self.core.media_transport,
            )
            .await
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
            policy_role: encoding.policy_role,
        }
    }
}
