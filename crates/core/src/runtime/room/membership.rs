//! Room membership orchestration for joins, leaves, disconnects and
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
//! Public and crate-visible entrypoints here are cold-path orchestration calls.
//! They must not hold the room state lock across `.await`. Cleanup calls run
//! after the state guard has been released.

use o_sfu_router::{MediaCapabilities, RouterId};
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;
#[cfg(any(test, feature = "testing-transport"))]
use {
    super::placement::{RoomPlacementPlanner, WorkerLoadIndex},
    crate::{
        RoomSpilloverMode,
        runtime::{media_transport::TransportWorkerPressureSnapshot, sync::lock_unpoisoned},
    },
};

#[cfg(any(test, feature = "testing-transport"))]
use super::LocalRouterRuntimeContext;
use super::{
    BroadcastPayloadError, Room, RoomJoinError, RoomMediaCounts, RoomUserOperation,
    SourcePolicyEvent, UserOutboundSender,
    cleanup::UserCleanup,
    effects::{RoomEffectBatch, RoomEffectContext, TransportUserCleanup},
    placement::JoinPlacement,
    state::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, RoomState},
    user_negotiation::UserNegotiationUpdate,
};
use crate::{
    SessionNegotiationOutcome, UserInfoRefresh,
    runtime::{
        ConnectionId, UserId, UserInfo, UserPermissions,
        diagnostics::DiagnosticsEventData,
        media_transport::{MediaTransport, TransportConsumerRoute},
    },
};

/// join request prepared before acquiring the room-state write lock
///
/// this keeps the public join entrypoints small while making the transition
/// pipeline consume one owned intent
/// placement is finalized under the state lock so topology sees the current
/// committed router set without awaiting worker-pressure reads
pub(in crate::runtime::room) struct JoinSessionIntent {
    /// stable room user identity
    pub user_id: UserId,
    /// browser-visible label stored on the room user
    pub label: Option<String>,
    /// permissions translated into room-owned capability flags
    pub permissions: UserPermissions,
    /// outbound queue for post-auth room events
    pub sender: UserOutboundSender,
    /// whether existing peers should receive a joined fan-out
    pub emit_joined_fanout: bool,
    /// router and media-worker placement resolved under the room-state lock
    pub placement: JoinPlacement,
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
    /// A join committed and allocated this runtime-local connection id.
    Joined(ConnectionId),
    /// A close or disconnect command completed its orchestration path.
    Applied,
    /// No state transition was available to finalize.
    Missing,
}

/// State-owned transition data captured before async effects run.
///
/// Values stored here are the only data finalization may use after the
/// `RoomState` guard is dropped. This prevents cleanup, diagnostics and fan-out
/// from rediscovering mutable room state after it has already advanced.
enum UserTransitionOutcome {
    /// A successful join, including replacement cleanup and fan-out effects.
    Join {
        outcome: JoinUserOutcome,
        count_delta: MembershipCountDelta,
    },
    /// A close command with optional state output.
    ///
    /// The state output is missing when the connection is stale or already
    /// gone. The transport cleanup identity is still kept so runtime cleanup
    /// can release resources for the requested connection.
    Close {
        outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
        count_delta: MembershipCountDelta,
    },
    /// A bulk disconnect outcome for every user still present in state.
    Disconnect {
        outcome: DisconnectUsersOutcome,
        count_delta: MembershipCountDelta,
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

    const fn delta_to(self, after: Self) -> MembershipCountDelta {
        MembershipCountDelta {
            users_before: self.users,
            users_after: after.users,
            media_before: self.media,
            media_after: after.media,
        }
    }
}

/// live-count delta attached to one committed membership transition
///
/// metrics consume this before transport cleanup, diagnostics and fan-out run
/// this keeps live gauges tied to room-state ownership instead of later
/// best-effort effects
#[derive(Debug, Clone, Copy)]
struct MembershipCountDelta {
    /// user count before the room state mutation
    users_before: usize,
    /// user count after the room state mutation
    users_after: usize,
    /// media totals before the room state mutation
    media_before: RoomMediaCounts,
    /// media totals after the room state mutation
    media_after: RoomMediaCounts,
}

impl Room {
    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) async fn local_join_placement_from_worker_pressure(
        &self,
        pressure_snapshots: Vec<TransportWorkerPressureSnapshot>,
    ) -> LocalRouterRuntimeContext {
        let room_snapshot = self.placement_usage_snapshot().await;
        let policy = self.room_worker_policy();
        let mut load_index = WorkerLoadIndex::new(policy.max_local_routers(), pressure_snapshots);
        let contribution = self.worker_load_contribution().await;
        for media_worker_id in contribution.session_workers {
            load_index.record_session(media_worker_id);
        }
        for media_worker_id in contribution.consumer_workers {
            load_index.record_consumer(media_worker_id);
        }
        let planner = RoomPlacementPlanner::new(policy.max_local_routers(), policy);
        let decision = match policy.spillover() {
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_) => {
                self.handle_source_policy_event(SourcePolicyEvent::FanoutPressureChanged, None)
                    .await;
                let mut load_state = lock_unpoisoned(&self.load_triggered_placement);
                planner.choose_with_load_state(&room_snapshot, &load_index, &mut load_state)
            }
            RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
                planner.choose(&room_snapshot, &load_index)
            }
        };
        JoinPlacement::planned(decision, load_index, policy.max_local_routers(), policy)
            .resolve_for_commit(&room_snapshot, || room_snapshot.next_local_router_id())
    }

    /// Run the room-owned join transition with an explicit cleanup policy.
    ///
    /// This method exists so production and test callers share the same join
    /// sequencing while choosing whether transport state should be touched.
    /// The pure state transition allocates the connection id and captures any
    /// replacement cleanup while the write lock is held. Transport cleanup,
    /// policy refresh, diagnostics and fan-out run only after that lock has
    /// been released.
    pub(in crate::runtime::room) async fn join_session_with_cleanup(
        &self,
        intent: JoinSessionIntent,
        cleanup: UserCleanup<'_>,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> Result<ConnectionId, RoomJoinError> {
        let outcome = self
            .apply_join_state_transition(intent, allocate_spillover_router)
            .await?;
        let UserTransitionResult::Joined(connection_id) =
            self.finalize_session_transition(outcome, cleanup).await
        else {
            return Err(RoomJoinError::RouterState);
        };
        Ok(connection_id)
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
            UserCleanup::runtime(media_transport),
        )
        .await
    }

    /// Close one user connection with an explicit cleanup policy.
    ///
    /// The returned boolean reports whether the close command entered the room
    /// transition pipeline. It is not a proof that live room state contained
    /// the connection, because stale close requests still need best-effort
    /// transport cleanup for the requested runtime identity.
    pub(in crate::runtime::room) async fn remove_user_with_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) -> bool {
        self.run_session_transition(
            UserTransition::Close {
                user_id,
                connection_id,
            },
            cleanup,
        )
        .await
        .is_ok_and(|result| !matches!(result, UserTransitionResult::Missing))
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
        self.disconnect_users_with_cleanup(user_ids, UserCleanup::runtime(media_transport))
            .await;
    }

    /// Disconnect a batch of users with an explicit cleanup policy.
    ///
    /// This is used by tests and lifecycle helpers that need the same room
    /// state transition as production while controlling whether transport
    /// resources are touched.
    pub(in crate::runtime::room) async fn disconnect_users_with_cleanup(
        &self,
        user_ids: &[UserId],
        cleanup: UserCleanup<'_>,
    ) {
        let _ = self
            .run_session_transition(UserTransition::Disconnect { user_ids }, cleanup)
            .await
            .ok();
    }

    /// Apply one leave-oriented membership command and finalize deferred effects.
    ///
    /// This is the common sequencing point for close and disconnect. It
    /// keeps the mutation phase and async effect phase adjacent in the code so
    /// future changes do not accidentally await while holding room state.
    async fn run_session_transition(
        &self,
        transition: UserTransition<'_>,
        cleanup: UserCleanup<'_>,
    ) -> Result<UserTransitionResult, RoomJoinError> {
        let Some(outcome) = self.apply_state_transition(transition).await? else {
            return Ok(UserTransitionResult::Missing);
        };
        Ok(self.finalize_session_transition(outcome, cleanup).await)
    }

    /// Mutate `RoomState` and capture all data needed after the lock is gone.
    ///
    /// No transport, diagnostics or websocket work belongs in this phase. The
    /// returned outcome is intentionally owned so finalization never has to
    /// re-read mutable membership state to decide cleanup.
    async fn apply_state_transition(
        &self,
        transition: UserTransition<'_>,
    ) -> Result<Option<UserTransitionOutcome>, RoomJoinError> {
        let outcome = {
            let mut state = self.state.write().await;
            let counts_before = MembershipCountSnapshot::from_state(&state);
            let outcome = match transition {
                UserTransition::Close {
                    user_id,
                    connection_id,
                } => {
                    let outcome = state.apply_leave(user_id, connection_id);
                    UserTransitionOutcome::Close {
                        outcome,
                        user_id: user_id.clone(),
                        connection_id,
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
        };
        Ok(Some(outcome))
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
    async fn finalize_session_transition(
        &self,
        outcome: UserTransitionOutcome,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let result = match outcome {
            UserTransitionOutcome::Join {
                outcome,
                count_delta,
            } => {
                self.finalize_join_transition(outcome, count_delta, cleanup)
                    .await
            }
            UserTransitionOutcome::Close {
                outcome,
                user_id,
                connection_id,
                count_delta,
            } => {
                self.finalize_close_transition(
                    outcome,
                    user_id,
                    connection_id,
                    count_delta,
                    cleanup,
                )
                .await
            }
            UserTransitionOutcome::Disconnect {
                outcome,
                count_delta,
            } => {
                self.finalize_disconnect_transition(outcome, count_delta, cleanup)
                    .await
            }
        };
        self.reconcile_spillover_routers().await;
        result
    }

    /// Finalize a committed join after room state has allocated its connection.
    ///
    /// Replacement joins may carry media cleanup for the previous session. The
    /// new session is registered with diagnostics and transport placement only
    /// after the state transition succeeds.
    async fn finalize_join_transition(
        &self,
        outcome: JoinUserOutcome,
        count_delta: MembershipCountDelta,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let JoinUserOutcome {
            connection_id,
            effects,
            user_id,
            transport_home_placement,
            transport_media_worker_id,
            transport_removals,
            relay_effects,
        } = outcome;
        self.placement_ledger
            .register_committed_placement(connection_id, transport_home_placement);
        RoomEffectBatch::new()
            .with_user_count_delta(count_delta.users_before, count_delta.users_after)
            .with_media_count_delta(count_delta.media_before, count_delta.media_after)
            .with_relay_effects(relay_effects)
            .with_transport_removals(transport_removals)
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
            .with_lifecycle_effects(effects)
            .register_diagnostics_user(user_id.clone())
            .record_diagnostics(
                DiagnosticsEventData::for_user(self.uuid(), &user_id, telemetry_event::USER_JOINED)
                    .with_connection_id(connection_id.as_u64())
                    .with_media_worker_id(transport_media_worker_id),
            )
            .execute(self, Self::effect_context(cleanup))
            .await;
        UserTransitionResult::Joined(connection_id)
    }

    /// Finalize a close request after room state has made its stale check.
    ///
    /// If the state transition removed a live session, this emits lifecycle
    /// effects and diagnostics. The transport close is attempted even for stale
    /// state outcomes because the requested runtime identity can still own
    /// adapter resources outside room state.
    async fn finalize_close_transition(
        &self,
        outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
        count_delta: MembershipCountDelta,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let had_state = outcome.is_some();
        let media_worker_id = self
            .placement_ledger
            .media_worker_id_for_connection(connection_id);
        let mut batch = RoomEffectBatch::new()
            .with_user_count_delta(count_delta.users_before, count_delta.users_after)
            .with_media_count_delta(count_delta.media_before, count_delta.media_after)
            .with_transport_user_close(TransportUserCleanup::new(user_id.clone(), connection_id));
        if let Some(outcome) = outcome {
            batch = batch
                .with_relay_effects(outcome.relay_effects)
                .with_transport_removals(outcome.transport_removals)
                .with_lifecycle_effects(outcome.effects)
                .record_diagnostics(
                    DiagnosticsEventData::for_user(
                        self.uuid(),
                        &user_id,
                        telemetry_event::USER_CLOSED,
                    )
                    .with_connection_id(connection_id.as_u64())
                    .with_media_worker_id(media_worker_id),
                )
                .forget_diagnostics_user(user_id.clone())
                .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged);
        }
        batch.execute(self, Self::effect_context(cleanup)).await;
        self.placement_ledger
            .unregister_committed_placement(connection_id);
        if had_state {
            UserTransitionResult::Applied
        } else {
            UserTransitionResult::Missing
        }
    }

    /// Finalize a bulk disconnect after room state has removed current users.
    ///
    /// Cleanup and diagnostics are driven only by the users returned from
    /// `RoomState`, so missing ids in the request do not create transport or
    /// telemetry side effects.
    async fn finalize_disconnect_transition(
        &self,
        outcome: DisconnectUsersOutcome,
        count_delta: MembershipCountDelta,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let mut batch = RoomEffectBatch::new()
            .with_user_count_delta(count_delta.users_before, count_delta.users_after)
            .with_media_count_delta(count_delta.media_before, count_delta.media_after)
            .with_relay_effects(outcome.relay_effects)
            .with_transport_removals(outcome.transport_removals)
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
            .with_lifecycle_effects(outcome.effects);
        let mut disconnected_sessions = Vec::with_capacity(outcome.disconnected_users.len());
        for disconnected_session in outcome.disconnected_users {
            batch = batch
                .with_transport_user_close(TransportUserCleanup::new(
                    disconnected_session.user_id.clone(),
                    disconnected_session.connection_id,
                ))
                .record_diagnostics(
                    DiagnosticsEventData::for_user(
                        self.uuid(),
                        &disconnected_session.user_id,
                        telemetry_event::USER_DISCONNECTED,
                    )
                    .with_media_worker_id(
                        self.placement_ledger
                            .media_worker_id_for_connection(disconnected_session.connection_id),
                    ),
                )
                .forget_diagnostics_user(disconnected_session.user_id.clone());
            disconnected_sessions.push(disconnected_session);
        }
        batch.execute(self, Self::effect_context(cleanup)).await;
        for disconnected_session in disconnected_sessions {
            self.placement_ledger
                .unregister_committed_placement(disconnected_session.connection_id);
        }
        UserTransitionResult::Applied
    }

    fn effect_context(cleanup: UserCleanup<'_>) -> RoomEffectContext<'_> {
        RoomEffectContext::new(
            cleanup.media_transport(),
            cleanup.cleaning_media_transport(),
        )
    }

    /// Commit the answer-derived negotiated capability set for one live session.
    ///
    /// This is called after the transport boundary has accepted the browser
    /// answer and projected the negotiated RTP capabilities. Room state records
    /// the session as consumer-ready, then any missing consumer bootstrap runs
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

impl RoomUserOperation<'_> {
    /// Apply client-visible user info for one live connection.
    ///
    /// Room state decides whether the update is still current, then returns a
    /// fan-out plan that is emitted after the lock is released. A refresh may
    /// trigger a full projection fan-out and a source selection policy sync
    /// because layout or presence changes can affect video priority.
    pub(crate) async fn update_user_info(self, info: UserInfo, refresh: UserInfoRefresh) {
        let need_refresh = refresh.is_needed();
        let room = self.room();
        let outcome = {
            let mut state = room.state.write().await;
            state.apply_presence_update(self.user_id(), self.connection_id(), &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            room.handle_source_policy_event(
                SourcePolicyEvent::ReceiverIntentChanged,
                Some(self.media_transport()),
            )
            .await;
            outcome.emit();
        } else {
            warn!(
                user_id = ?self.user_id(),
                connection_id = ?self.connection_id(),
                ?info,
                need_refresh,
                "user info update was rejected by room state"
            );
        }
    }

    /// Commit the answer-derived negotiated capability set for one live session.
    ///
    /// This is called after the transport boundary has accepted the browser
    /// answer and projected the negotiated RTP capabilities. Room state records
    /// the session as consumer-ready, then any missing consumer bootstrap runs
    /// outside the state lock.
    pub(crate) async fn apply_session_negotiated(
        self,
        capabilities: MediaCapabilities,
    ) -> SessionNegotiationOutcome {
        let update = {
            let mut state = self.room().state.write().await;
            state.set_user_negotiated(self.user_id(), self.connection_id(), &capabilities)
        };
        self.apply_negotiation_update(update).await
    }

    /// Refresh consumer-side media after a renegotiation answer.
    ///
    /// This does not update the stored RTP capability set. It revalidates that
    /// the connection is still current, requests keyframes for active video
    /// consumers and bootstraps any consumers that became possible after the
    /// renegotiation.
    pub(crate) async fn apply_session_refreshed(self) -> SessionNegotiationOutcome {
        if !self.request_active_video_consumer_keyframes().await {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if !self.bootstrap_missing_consumers().await {
            return SessionNegotiationOutcome::StaleConnection;
        }
        SessionNegotiationOutcome::Applied
    }

    /// Apply the side effects that follow a negotiation state change.
    ///
    /// Consumer bootstrap is deferred until room state says the session became
    /// ready to receive. Keyframe refresh requests are best-effort transport
    /// hints and do not make the negotiation outcome fail.
    async fn apply_negotiation_update(
        self,
        update: UserNegotiationUpdate,
    ) -> SessionNegotiationOutcome {
        if !update.session_present {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if update.became_consumer_ready {
            if !self.bootstrap_missing_consumers().await {
                return SessionNegotiationOutcome::StaleConnection;
            }
            self.request_active_video_consumer_keyframes().await;
        }
        SessionNegotiationOutcome::Applied
    }

    /// Request keyframes for active video consumers owned by one live session.
    ///
    /// The target list is an authoritative room-state snapshot for the current
    /// connection. Individual transport request failures are logged but kept
    /// best-effort because a later media packet or refresh can recover video.
    async fn request_active_video_consumer_keyframes(self) -> bool {
        let room = self.room();
        let Some(keyframe_refresh_targets) = ({
            let state = room.state.read().await;
            state.active_video_consumer_keyframe_refresh_targets(
                self.user_id(),
                self.connection_id(),
            )
        }) else {
            return false;
        };
        for target in keyframe_refresh_targets {
            let route = TransportConsumerRoute::new(
                self.transport_user_key(),
                target.consumer_media,
                room.transport_user_key(&target.producer_user_id, target.producer_connection_id),
                target.source_media,
            );
            if self
                .media_transport()
                .request_consumer_keyframe(&route)
                .await
                .is_err()
            {
                warn!(
                    user_id = ?self.user_id(),
                    connection_id = ?self.connection_id(),
                    producer_user_id = ?target.producer_user_id,
                    source_transport_media_id = ?target.source_media,
                    "media transport failed to request a refreshed consumer keyframe"
                );
            }
        }
        true
    }
}
