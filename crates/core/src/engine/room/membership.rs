//! Room membership workflows for joins, leaves, disconnects and
//! negotiation readiness.
//!
//! This module is the async boundary around pure `RoomState` membership
//! transitions. `RoomState` remains authoritative for live users, connection
//! ids, fan-out plans and media removals. The `Room` methods in this file
//! acquire the state lock, capture a transition outcome, release the lock and
//! then run transport cleanup, diagnostics, policy refresh and outbound
//! fan-out.
//!
//! The split keeps membership decisions deterministic. Transport adapters,
//! websocket senders and diagnostics never decide whether a user is present.
//! They only observe or complete work captured by a committed state
//! transition.
//!
//! # Concurrency
//!
//! Public and crate-visible entrypoints here are cold-path membership calls.
//! They must not hold the room state lock across `.await`. Cleanup calls run
//! after the state guard has been released.

use o_sfu_router::{MediaCapabilities, RouterId};

use super::{
    BroadcastPayloadError, Room, RoomJoinError, RoomMediaCounts, UserOutboundSender,
    cleanup::TransportCleanupOperation,
    effects::{
        self,
        batch::{RoomEffectContext, RoomGaugeDelta},
    },
    placement::JoinPlacementPlan,
    routing::CommittedRoutingReceipt,
    state::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, RoomState},
};
use crate::{
    SessionNegotiationOutcome,
    engine::{ConnectionId, UserId, UserPermissions, media_transport::MediaTransport},
};

/// join request prepared before acquiring the room-state write lock
///
/// this keeps the public join entrypoints small while making the transition
/// pipeline consume one owned intent
/// placement is finalized under the state lock so topology sees the current
/// committed router set without awaiting worker-pressure reads
pub(in crate::engine::room) struct JoinSessionIntent {
    /// stable room user identity
    pub user_id: UserId,
    /// browser-visible label stored on the room user
    pub label: Option<String>,
    /// permissions translated into room capability flags
    pub permissions: UserPermissions,
    /// outbound queue for post-auth room events
    pub sender: UserOutboundSender,
    /// whether existing peers should receive a joined fan-out
    pub emit_joined_fanout: bool,
    /// router and media-worker placement resolved under the room-state lock
    pub placement: JoinPlacementPlan,
}

/// Membership command applied under the `RoomState` lock.
///
/// The enum is local to this module because callers should express intent
/// through `Room` methods. Keeping leave commands in one pipeline makes the
/// lock boundary explicit and gives those transitions the same deferred
/// cleanup path.
enum UserTransition<'a> {
    /// Close one runtime connection and clean up any matching transport owner.
    Close {
        user_id: &'a UserId,
        connection_id: ConnectionId,
    },
    /// Remove the currently live sessions for a batch of user ids.
    Disconnect { user_ids: &'a [UserId] },
}

/// Result exposed by the membership transition pipeline.
///
/// This separates domain outcomes from the concrete state structs returned by
/// `RoomState`. Callers only need to know whether a join produced a connection,
/// whether a close or disconnect was applied and whether the state layer
/// produced any transition to finalize.
enum UserTransitionResult {
    /// A join committed and allocated this runtime-local connection.
    Joined(CommittedRoutingReceipt),
    /// A close or disconnect command completed its lifecycle path.
    Applied,
    /// No state transition was available to finalize.
    Missing,
}

/// Transition data captured before async effects run.
///
/// Values stored here are the only data finalization may use after the
/// `RoomState` guard is dropped. This prevents cleanup, diagnostics and fan-out
/// from rediscovering mutable room state after it has already advanced.
enum UserTransitionOutcome {
    /// A successful join, including replacement cleanup and fan-out effects.
    Join {
        outcome: JoinUserOutcome,
        count_delta: RoomGaugeDelta,
    },
    /// A close command with optional state output.
    ///
    /// The state output is missing when the connection is stale or already
    /// gone. The transport cleanup identity is still kept so runtime cleanup
    /// can release resources for the requested connection.
    Close {
        state_outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
        transport_close: Option<TransportCleanupOperation>,
        count_delta: RoomGaugeDelta,
    },
    /// A bulk disconnect outcome for every user still present in state.
    Disconnect {
        outcome: DisconnectUsersOutcome,
        count_delta: RoomGaugeDelta,
    },
}

/// cheap live-count snapshot taken while [`RoomState`] is authoritative
///
/// the manager used to read these values around async cleanup work
/// keeping the
/// snapshot here ties metrics to the same write lock that accepted the
/// membership transition
#[derive(Debug, Clone, Copy)]
struct MembershipCountSnapshot {
    /// live users visible to room state at one committed instant
    users: usize,
    /// live publication and subscription totals visible at the same instant
    media: RoomMediaCounts,
}

impl MembershipCountSnapshot {
    fn from_state(state: &RoomState) -> Self {
        Self {
            users: state.user_count(),
            media: state.media_counts(),
        }
    }

    const fn delta_to(self, after: Self) -> RoomGaugeDelta {
        RoomGaugeDelta::membership(self.users, after.users, self.media, after.media)
    }
}

impl Room {
    /// Run the room join transition with an explicit cleanup policy.
    ///
    /// This method exists so production and test callers share the same join
    /// sequencing while choosing whether transport state should be touched.
    /// The pure state transition allocates the connection id and captures any
    /// replacement cleanup while the write lock is held. Transport cleanup,
    /// policy refresh, diagnostics and fan-out run only after that lock has
    /// been released.
    pub(in crate::engine::room) async fn join_session_with_cleanup(
        &self,
        intent: JoinSessionIntent,
        context: RoomEffectContext<'_>,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> Result<CommittedRoutingReceipt, RoomJoinError> {
        let outcome = self
            .apply_join_state_transition(intent, allocate_spillover_router)
            .await?;
        let UserTransitionResult::Joined(receipt) =
            self.finalize_user_transition(outcome, context).await
        else {
            return Err(RoomJoinError::RouterState);
        };
        Ok(receipt)
    }

    /// Close one user connection through the production cleanup path.
    ///
    /// The close request is scoped by both `UserId` and `ConnectionId` so stale
    /// websocket handles cannot remove a newer session from room state. The
    /// transport close is still attempted for the requested identity after the
    /// state decision because the lower layer may own resources for that
    /// runtime-local connection.
    pub(crate) async fn remove_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        self.remove_user_with_cleanup(
            user_id,
            connection_id,
            RoomEffectContext::runtime(media_transport),
        )
        .await
    }

    /// Close one user connection with an explicit cleanup policy.
    ///
    /// The returned boolean reports whether the close command entered the room
    /// transition pipeline. It is not a proof that live room state contained
    /// the connection, because stale close requests still need best-effort
    /// transport cleanup for the requested runtime identity.
    pub(in crate::engine::room) async fn remove_user_with_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        context: RoomEffectContext<'_>,
    ) -> bool {
        !matches!(
            self.run_user_transition(
                UserTransition::Close {
                    user_id,
                    connection_id,
                },
                context,
            )
            .await,
            UserTransitionResult::Missing
        )
    }

    /// Fan a user-originated room message out to the other live sessions.
    ///
    /// The sender identity is checked against authoritative room state before
    /// fan-out is emitted. Stale senders are ignored because websocket code can
    /// race with replacement or teardown.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastPayloadError`] when the payload exceeds the room
    /// broadcast byte limit or cannot be measured as serialized JSON.
    pub async fn broadcast(
        &self,
        sender_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) -> Result<(), BroadcastPayloadError> {
        let fanout = {
            let state = self.state.read().await;
            state.broadcast_fanout(sender_id, connection_id, message)
        }?;
        if let Some(fanout) = fanout {
            fanout.emit();
        }
        Ok(())
    }

    /// Check whether room state still binds a user to a runtime connection.
    ///
    /// This is an authoritative room-state query at the instant the read lock
    /// is held. It is still a cold-path snapshot, so callers must not treat it
    /// as a lease across later awaits.
    pub async fn has_connection(&self, user_id: &UserId, connection_id: ConnectionId) -> bool {
        self.state.read().await.user_connection_id(user_id) == Some(connection_id)
    }

    /// Disconnect a batch of users through the production cleanup path.
    ///
    /// Missing users are ignored by room state. Current users are removed under
    /// one state transition, then transport cleanup, diagnostics and fan-out run
    /// outside the state lock.
    pub(crate) async fn disconnect_users(
        &self,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        self.disconnect_users_with_cleanup(user_ids, RoomEffectContext::runtime(media_transport))
            .await;
    }

    /// Disconnect a batch of users with an explicit cleanup policy.
    ///
    /// This is used by tests and lifecycle helpers that need the same room
    /// state transition as production while controlling whether transport
    /// resources are touched.
    pub(in crate::engine::room) async fn disconnect_users_with_cleanup(
        &self,
        user_ids: &[UserId],
        context: RoomEffectContext<'_>,
    ) {
        self.run_user_transition(UserTransition::Disconnect { user_ids }, context)
            .await;
    }

    /// Apply one leave-oriented membership command and finalize deferred effects.
    ///
    /// This is the common sequencing point for close and disconnect. It
    /// keeps the mutation phase and async effect phase adjacent in the code so
    /// future changes do not accidentally await while holding room state.
    async fn run_user_transition(
        &self,
        transition: UserTransition<'_>,
        context: RoomEffectContext<'_>,
    ) -> UserTransitionResult {
        let outcome = self.apply_state_transition(transition).await;
        self.finalize_user_transition(outcome, context).await
    }

    /// Mutate `RoomState` and capture all data needed after the lock is gone.
    ///
    /// No transport, diagnostics or websocket work belongs in this phase. The
    /// returned outcome carries all cleanup data so finalization never has to
    /// re-read mutable membership state.
    async fn apply_state_transition(
        &self,
        transition: UserTransition<'_>,
    ) -> UserTransitionOutcome {
        {
            let mut state = self.state.write().await;
            let counts_before = MembershipCountSnapshot::from_state(&state);
            let outcome = match transition {
                UserTransition::Close {
                    user_id,
                    connection_id,
                } => {
                    let transport_close = state
                        .committed_transport_user_key(user_id, connection_id)
                        .map(|session_key| TransportCleanupOperation::CloseUser {
                            session_key,
                            connection_id,
                        });
                    let state_outcome = state.apply_leave(user_id, connection_id);
                    if state_outcome.is_none() {
                        state
                            .routing
                            .unregister_committed_placement(user_id, connection_id);
                    }
                    UserTransitionOutcome::Close {
                        state_outcome,
                        user_id: user_id.clone(),
                        connection_id,
                        transport_close,
                        count_delta: counts_before
                            .delta_to(MembershipCountSnapshot::from_state(&state)),
                    }
                }
                UserTransition::Disconnect { user_ids } => {
                    let outcome = state.apply_disconnect_users(user_ids);
                    UserTransitionOutcome::Disconnect {
                        outcome,
                        count_delta: counts_before
                            .delta_to(MembershipCountSnapshot::from_state(&state)),
                    }
                }
            };
            drop(state);
            outcome
        }
    }

    async fn apply_join_state_transition(
        &self,
        intent: JoinSessionIntent,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> Result<UserTransitionOutcome, RoomJoinError> {
        let outcome = {
            let mut state = self.state.write().await;
            let counts_before = MembershipCountSnapshot::from_state(&state);
            let home_placement = intent
                .placement
                .resolve_for_commit(&state.placement_usage_snapshot(), allocate_spillover_router);
            let outcome = state.apply_join_on_placement(
                &intent.user_id,
                intent.label,
                intent.permissions,
                intent.sender,
                intent.emit_joined_fanout,
                home_placement,
            )?;
            let count_delta = counts_before.delta_to(MembershipCountSnapshot::from_state(&state));
            drop(state);
            UserTransitionOutcome::Join {
                outcome,
                count_delta,
            }
        };
        Ok(outcome)
    }

    /// Run the async effects produced by a committed membership transition.
    ///
    /// At this point room state has already accepted or rejected the command.
    /// Finalization may update runtime metadata, clean transport resources,
    /// refresh source selection policy and emit outbound effects, but it must
    /// not reopen the membership decision.
    async fn finalize_user_transition(
        &self,
        outcome: UserTransitionOutcome,
        context: RoomEffectContext<'_>,
    ) -> UserTransitionResult {
        let result = match outcome {
            UserTransitionOutcome::Join {
                outcome,
                count_delta,
            } => {
                let (batch, routing_receipt) =
                    effects::batch::build_join(self, count_delta, outcome);
                batch.execute(self, context).await;
                UserTransitionResult::Joined(routing_receipt)
            }
            UserTransitionOutcome::Close {
                state_outcome,
                user_id,
                connection_id,
                transport_close,
                count_delta,
            } => {
                let had_state = state_outcome.is_some();
                let batch = effects::batch::build_connection_close(
                    self,
                    count_delta,
                    state_outcome,
                    user_id,
                    connection_id,
                    transport_close,
                );
                batch.execute(self, context).await;
                if had_state {
                    UserTransitionResult::Applied
                } else {
                    UserTransitionResult::Missing
                }
            }
            UserTransitionOutcome::Disconnect {
                outcome,
                count_delta,
            } => {
                let staged_cleanup = outcome
                    .disconnected_users
                    .iter()
                    .flat_map(|session| {
                        self.drain_staged_publish_cleanup_operations(
                            &session.user_id,
                            session.connection_id,
                        )
                    })
                    .collect::<Vec<_>>();
                effects::batch::build_disconnect(self, count_delta, outcome, staged_cleanup)
                    .execute(self, context)
                    .await;
                UserTransitionResult::Applied
            }
        };
        self.reconcile_spillover_routers().await;
        result
    }

    /// Commit the answer-derived negotiated capability set for one live session.
    ///
    /// This is called after the transport boundary has accepted the browser
    /// answer and projected the negotiated RTP capabilities. Room state records
    /// the session as ready, then any missing consumer setup runs
    /// outside the state lock.
    ///
    /// `StaleConnection` means the user was replaced or removed before the
    /// answer callback reached the room. Transport negotiation may have
    /// succeeded, but room state no longer accepts the session as current.
    pub async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &MediaTransport,
    ) -> SessionNegotiationOutcome {
        self.user_operation(user_id, connection_id, media_port)
            .apply_session_negotiated(capabilities)
            .await
    }

    /// Return the current authoritative number of live room users.
    #[cfg(test)]
    pub(super) async fn user_count(&self) -> usize {
        self.state.read().await.user_count()
    }

    /// Return whether room state currently has no live users.
    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
