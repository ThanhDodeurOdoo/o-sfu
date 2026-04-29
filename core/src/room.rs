//! Room boundary vocabulary consumed by the media-core facade.
//!
//! # Boundary role
//!
//! `SfuCore` owns transport orchestration, while the concrete room
//! implementation owns membership, publication state and subscription intent.
//! This file defines the narrow bridge between those layers. It lets callers
//! express media intent through a `MediaSession` without exposing room maps,
//! router ids or transport reservation details.
//!
//! The outcome enums distinguish expected domain rejections from transport
//! failures. Idempotent no-ops such as a duplicate staged publish stay normal
//! outcomes. A failed transport allocation stays an error because the caller
//! cannot safely continue the publish flow without a reserved media line.

use o_sfu_router::MediaCapabilities;

use crate::{
    ConnectionId,
    runtime::{DownloadStates, StreamType, UserId, UserInfo},
    transport::{
        AppliedSessionAnswer, MediaPort, ObservabilityPort, TransportAdapterError,
        TransportSessionKey,
    },
};

#[derive(Debug, Clone)]
/// Stable room identity bundle for one media operation.
///
/// The user id is the compatibility-facing identity used by room state. The
/// connection id is runtime-local and prevents stale websocket callbacks from
/// mutating a replacement session for the same user. The transport key is
/// derived once by the room so the core facade does not need to know room
/// instance or worker placement rules.
pub struct MediaSessionContext<'a> {
    user_id: &'a UserId,
    connection_id: ConnectionId,
    transport_user_key: TransportSessionKey,
}

impl<'a> MediaSessionContext<'a> {
    /// Build a context from room-owned identity data.
    ///
    /// Normal callers should prefer [`MediaRoom::media_session_context`] so the
    /// room remains the authority for transport key derivation.
    #[must_use]
    pub const fn new(
        user_id: &'a UserId,
        connection_id: ConnectionId,
        transport_user_key: TransportSessionKey,
    ) -> Self {
        Self {
            user_id,
            connection_id,
            transport_user_key,
        }
    }

    /// Compatibility-facing user identity for this media session.
    #[must_use]
    pub const fn user_id(&self) -> &'a UserId {
        self.user_id
    }

    /// Runtime-local connection identity for this media session.
    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    /// Transport-local identity used to address WebRTC endpoint state.
    #[must_use]
    pub const fn transport_user_key(&self) -> &TransportSessionKey {
        &self.transport_user_key
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Caller intent for an already published stream.
///
/// This is not the same as transport packet gating. The room first commits the
/// user-visible publication activity and then asks the transport layer to
/// mirror that state onto the media route.
pub enum PublicationActivity {
    /// The stream should be visible as an active publication.
    Active,
    /// The stream remains published but should be treated as inactive.
    Inactive,
}

impl PublicationActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Outcome of a transport side effect that is allowed to be best-effort.
///
/// Some room transitions must consume local ownership even if transport cleanup
/// races with disconnect or backend teardown. This type records whether the
/// side effect actually reached the transport layer without turning the domain
/// transition itself into an error.
pub enum TransportEffectOutcome {
    /// The transport accepted the side effect.
    Applied,
    /// The room made the cleanup or update decision, but transport returned an error.
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of committing answer-derived session readiness into room state.
///
/// A stale connection is an expected race after replacement or disconnect. It
/// means the transport answer may have been accepted by the backend, but room
/// state refused to ready the old connection.
pub enum SessionNegotiationOutcome {
    /// The room accepted the answer-derived readiness update.
    Applied,
    /// The connection no longer owns the user session.
    StaleConnection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of reserving transport media for a publish before negotiation lands.
///
/// Staging is a cold-path transaction. The publish is not live until a later
/// answer commits the reserved media into room state. Duplicate outcomes are
/// normal idempotency results and should usually be ignored by user-facing
/// orchestration.
pub enum PublishStageOutcome {
    /// A new transport media line is reserved and awaits answer commit.
    Staged,
    /// The same connection already has this stream staged before any new reservation.
    Duplicate,
    /// A racing task reserved media first, then lost the staged transaction slot.
    ///
    /// The cleanup result reports whether the duplicate reservation was removed
    /// from the transport backend.
    DuplicateAfterReservation { cleanup: TransportEffectOutcome },
    /// Room state rejected the publish before transport reservation.
    ///
    /// Typical causes are stale connections, missing users or users that have
    /// not reached publish readiness.
    Rejected,
}

impl PublishStageOutcome {
    /// Returns whether this outcome created a pending publish transaction.
    ///
    /// Callers that only need to decide whether to request renegotiation can
    /// use this helper. More specific orchestration and telemetry should still
    /// match on the enum so duplicate and rejected paths stay visible.
    #[must_use]
    pub const fn staged(self) -> bool {
        matches!(self, Self::Staged)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of cancelling one staged publish before it becomes live.
///
/// Rollback consumes pending room ownership when present. A cleanup failure
/// does not restore the staged transaction because the caller has already
/// decided that this publish must not be committed later.
pub enum RollbackStagedPublishOutcome {
    /// A staged reservation existed and was consumed.
    RolledBack { cleanup: TransportEffectOutcome },
    /// No staged reservation existed for this connection and stream.
    NotStaged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of removing a live publication from room state.
///
/// Explicit unpublish is stricter than staged rollback. Live producer and
/// consumer routes remain authoritative until committed transport cleanup
/// succeeds, because dropping room state first could leave routable media that
/// the room can no longer address.
pub enum UnpublishOutcome {
    /// Transport cleanup succeeded and room state removed the publication.
    Unpublished,
    /// The stream was not live for this connection.
    MissingPublication,
    /// Transport cleanup failed before room state was changed.
    TransportCleanupFailed,
    /// Transport cleanup succeeded, but the room state commit no longer matched.
    StateCommitRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of changing the visible activity of an existing publication.
///
/// The room state update is authoritative for user snapshots. The transport
/// update is reported separately because it can fail after the room has already
/// committed the visible publication state.
pub enum PublicationActivityOutcome {
    /// Room state accepted the activity change.
    Applied {
        /// Whether the corresponding transport route update succeeded.
        transport_update: TransportEffectOutcome,
    },
    /// No live publication exists for this stream.
    MissingPublication,
    /// The publication changed owner between lookup and commit.
    StalePublication,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of persisting download intent and projecting subscription effects.
///
/// Subscription updates are idempotent control-plane operations. A stale
/// connection is a protocol problem for the caller, not a transport failure.
pub enum SubscriptionUpdateOutcome {
    /// The room accepted the subscription intent.
    Applied,
    /// The connection no longer owns the subscribing user.
    StaleConnection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Whether a user-info update should trigger a full peer snapshot refresh.
///
/// Most presence changes can be emitted incrementally. A refresh is reserved
/// for updates where peers need to rebuild their room-facing user snapshot.
pub enum UserInfoRefresh {
    /// Peers should receive a full refreshed snapshot.
    Needed,
    /// Peers can receive the normal incremental update.
    NotNeeded,
}

impl UserInfoRefresh {
    #[must_use]
    pub const fn from_needed(needed: bool) -> Self {
        if needed {
            Self::Needed
        } else {
            Self::NotNeeded
        }
    }

    #[must_use]
    pub const fn is_needed(self) -> bool {
        matches!(self, Self::Needed)
    }
}

#[allow(
    async_fn_in_trait,
    reason = "media room calls are static-dispatch bridges from the server room into the media core facade"
)]
/// Room operations required by the media-core session facade.
///
/// # Boundary role
///
/// Implementations keep room state authoritative for membership,
/// publication ownership and subscription intent. The media core supplies the
/// transport port needed for deferred side effects, but it does not inspect or
/// mutate room internals directly.
///
/// # Concurrency model
///
/// These calls are cold-path orchestration. Implementations should snapshot or
/// mutate room state under short locks, release those locks before awaiting
/// transport work and return domain outcomes that tell the caller whether the
/// intent was applied, ignored or rejected.
pub trait MediaRoom<T>
where
    T: MediaPort + ObservabilityPort + Send + Sync,
{
    /// Builds the transport identity for one room connection.
    ///
    /// The room owns worker and instance placement, so callers should obtain
    /// transport keys through this boundary instead of reconstructing them.
    fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey;

    /// Builds the media-session context used by [`crate::MediaSession`].
    ///
    /// The default implementation keeps the transport key derived by the room
    /// next to the user and connection identities that produced it. Override it
    /// only if the room has extra context to attach without changing the public
    /// session contract.
    fn media_session_context<'a>(
        &self,
        user_id: &'a UserId,
        connection_id: ConnectionId,
    ) -> MediaSessionContext<'a> {
        MediaSessionContext::new(
            user_id,
            connection_id,
            self.transport_user_key(user_id, connection_id),
        )
    }

    /// Returns the router RTP capabilities advertised in initial offers.
    ///
    /// The result is the room's current capability view. `MediaSession` stores
    /// the returned value with the offer so answer projection uses the same
    /// capability set that the browser received.
    async fn router_rtp_capabilities(&self) -> MediaCapabilities;

    /// Commits an initial answer into room negotiation state.
    ///
    /// The returned outcome names room-state acceptance only. Transport answer
    /// application happens before this call in `MediaSession`.
    async fn apply_session_negotiated(
        &self,
        session: &MediaSessionContext<'_>,
        capabilities: MediaCapabilities,
        media_port: &T,
    ) -> SessionNegotiationOutcome;

    /// Refreshes room-side negotiation state after a follow-up answer.
    ///
    /// Implementations should treat callbacks for replaced connections as
    /// stale rather than mutating the current session.
    async fn apply_session_refreshed(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &T,
    ) -> SessionNegotiationOutcome;

    /// Checks whether this connection already has a staged publish.
    ///
    /// This is an idempotency read for orchestration. It is not a reservation,
    /// because the staged transaction can still be committed or rolled back by
    /// a later operation.
    async fn has_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
    ) -> bool;

    /// Checks whether this user currently owns a live publication for a stream.
    ///
    /// This is a point-in-time room-state read. It does not reserve ownership
    /// against a later unpublish, disconnect or replacement.
    async fn is_stream_published(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
    ) -> bool;

    /// Applies user-visible publication activity and mirrors it to transport.
    ///
    /// The outcome separates room-state acceptance from best-effort transport
    /// projection so callers can log route update failures without pretending
    /// the visible activity change was rejected.
    async fn set_publication_active(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        activity: PublicationActivity,
        media_port: &T,
    ) -> PublicationActivityOutcome;

    /// Persists subscription intent for a target user and plans route effects.
    ///
    /// This is the room-owned bridge from compatibility download state to
    /// source-keyed subscription state. Transport work happens below this
    /// boundary after room state has planned the effect.
    async fn update_subscription(
        &self,
        session: &MediaSessionContext<'_>,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &T,
    ) -> SubscriptionUpdateOutcome;

    /// Applies room-visible user info for the session.
    ///
    /// Implementations should ignore stale connections rather than refreshing
    /// the current replacement user with old websocket state. The `refresh`
    /// flag tells fanout whether peers need a full snapshot.
    async fn update_user_info(
        &self,
        session: &MediaSessionContext<'_>,
        info: UserInfo,
        refresh: UserInfoRefresh,
        media_port: &T,
    );

    /// Reserves transport media for a publish that still needs negotiation.
    ///
    /// `TransportAdapterError` means the publish could not reserve media and
    /// should surface as an exceptional failure. `PublishStageOutcome` covers
    /// normal domain results after the room decided whether the publish intent
    /// may proceed.
    async fn stage_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &T,
    ) -> Result<PublishStageOutcome, TransportAdapterError>;

    /// Cancels one pending publish reservation for the session.
    ///
    /// Rollback is used by explicit unpublish before the answer lands. It
    /// consumes any staged reservation even when transport cleanup is reported
    /// as failed.
    async fn rollback_staged_publish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &T,
    ) -> RollbackStagedPublishOutcome;

    /// Cancels every pending publish reservation owned by the connection.
    ///
    /// This is connection cleanup, not user-visible unpublish. Implementations
    /// should drain all staged reservations for the exact connection so a
    /// replaced socket cannot later commit media it no longer owns.
    async fn rollback_connection_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        media_port: &T,
    );

    /// Commits staged publishes after the transport answer accepted them.
    ///
    /// The room must re-check current ownership before installing each live
    /// producer because connection replacement can happen while negotiation is
    /// in flight. Rejected commits should consume their staged transport
    /// reservations through cleanup.
    async fn commit_staged_publishes(
        &self,
        session: &MediaSessionContext<'_>,
        applied_answer: &AppliedSessionAnswer,
        media_port: &T,
    );

    /// Removes a live publication and its dependent consumer routes.
    ///
    /// This is stricter than staged rollback because room state already owns
    /// live routing and diagnostics entries. Implementations should report
    /// cleanup failures as `UnpublishOutcome::TransportCleanupFailed` without
    /// dropping authoritative room ownership.
    async fn unpublish(
        &self,
        session: &MediaSessionContext<'_>,
        stream_type: StreamType,
        media_port: &T,
    ) -> UnpublishOutcome;
}
