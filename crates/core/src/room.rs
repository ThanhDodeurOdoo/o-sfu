//! Room boundary vocabulary consumed by the media-core facade.
//!
//! `SfuCore` owns transport orchestration, while the concrete room
//! implementation owns membership, publication state and subscription intent.
//! This file defines the session context and domain outcomes shared by those
//! layers. It lets callers express media intent through a `MediaSession`
//! without exposing room maps, router ids or transport reservation details.
//!
//! The outcome enums distinguish expected domain rejections from transport
//! failures. Idempotent no-ops such as a duplicate staged publish stay normal
//! outcomes. A failed transport allocation stays an error because the caller
//! cannot safely continue the publish flow without a reserved media line.

use crate::{ConnectionId, runtime::UserId, transport::TransportSessionKey};

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
/// Explicit unpublish removes room ownership before transport cleanup runs.
/// Cleanup failures are retained by the room reconciler so callers can continue
/// with renegotiation after the publication is no longer visible.
pub enum UnpublishOutcome {
    /// Room state removed the publication.
    Unpublished { cleanup: TransportEffectOutcome },
    /// The stream was not live for this connection.
    MissingPublication,
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
