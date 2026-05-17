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

use o_sfu_router::MediaCapabilities;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

#[cfg(any(test, feature = "testing-transport"))]
use super::placement::{RoomPlacementPlanner, WorkerLoadIndex};
use super::{
    BroadcastPayloadError, LocalRouterRuntimeContext, Room, RoomJoinError, RoomUserPermissions,
    UserOutbound, UserOutboundSender,
    cleanup::UserCleanup,
    effects::execute_relay_route_effects,
    state::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects},
    user_negotiation::UserNegotiationUpdate,
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::media_transport::TransportWorkerPressureSnapshot;
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
/// placement is resolved before the state mutation so `RoomState` can mirror
/// the session into topology without awaiting worker-pressure reads
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
    /// router and media-worker placement selected before the join commits
    pub home_placement: LocalRouterRuntimeContext,
}

/// Membership command applied under the `RoomState` lock.
///
/// The enum is local to this module because callers should express intent
/// through `Room` methods. Keeping all membership commands in one pipeline
/// makes the lock boundary explicit and gives every transition the same
/// deferred cleanup path.
enum UserTransition<'a> {
    /// Install a new session or replace the current session for the same user.
    Join {
        user_id: &'a UserId,
        label: Option<String>,
        permissions: RoomUserPermissions,
        sender: UserOutboundSender,
        emit_joined_fanout: bool,
        home_placement: LocalRouterRuntimeContext,
    },
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
    Join(JoinUserOutcome),
    /// A close command with optional state output.
    ///
    /// The state output is missing when the connection is stale or already
    /// gone. The transport cleanup identity is still kept so runtime cleanup
    /// can release resources for the requested connection.
    Close {
        outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
    },
    /// A bulk disconnect outcome for every user still present in state.
    Disconnect(DisconnectUsersOutcome),
}

impl Room {
    /// Join a user through the runtime membership boundary.
    ///
    /// This is the production join entrypoint used by the room manager after
    /// admission has selected the current room. The returned `ConnectionId` is
    /// runtime-local and must be paired with the same `UserId` for later room
    /// operations.
    ///
    /// # Error handling
    ///
    /// `RoomFull` is an admission decision made by room state. `RouterState`
    /// means the join could not be mirrored into routing topology, so callers
    /// must treat the join as not committed.
    #[cfg(test)]
    pub(crate) async fn add_user(
        &self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
        media_transport: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        let home_placement = self.local_join_placement(media_transport).await;
        self.add_user_on_placement(
            user_id,
            label,
            permissions,
            sender,
            media_transport,
            home_placement,
        )
        .await
    }

    #[cfg(test)]
    async fn local_join_placement(
        &self,
        media_transport: &MediaTransport,
    ) -> LocalRouterRuntimeContext {
        self.local_join_placement_from_worker_pressure(media_transport.worker_pressure_snapshots())
            .await
    }

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
        planner
            .choose(&room_snapshot, &load_index)
            .resolve(&room_snapshot, || room_snapshot.next_local_router_id())
    }

    /// join a user after placement has already been selected
    ///
    /// callers use this when the room manager has resolved the target router
    /// and media worker from current load
    /// the join is still allowed to fail if room state is full or topology
    /// rejects the placement while committing the membership transition
    pub(crate) async fn add_user_on_placement(
        &self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
        media_transport: &MediaTransport,
        home_placement: LocalRouterRuntimeContext,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.join_session_with_cleanup(
            JoinSessionIntent {
                user_id,
                label,
                permissions,
                sender,
                emit_joined_fanout: true,
                home_placement,
            },
            UserCleanup::runtime(media_transport),
        )
        .await
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
    ) -> Result<ConnectionId, RoomJoinError> {
        let JoinSessionIntent {
            user_id,
            label,
            permissions,
            sender,
            emit_joined_fanout,
            home_placement,
        } = intent;
        let UserTransitionResult::Joined(connection_id) = self
            .run_session_transition(
                UserTransition::Join {
                    user_id: &user_id,
                    label,
                    permissions: permissions.into(),
                    sender,
                    emit_joined_fanout,
                    home_placement,
                },
                cleanup,
            )
            .await?
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

    /// Apply client-visible user info for one live connection.
    ///
    /// Room state decides whether the update is still current, then returns a
    /// fan-out plan that is emitted after the lock is released. A refresh may
    /// trigger a full projection fan-out and a source selection policy sync
    /// because layout or presence changes can affect video priority.
    pub(crate) async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
        media_transport: &MediaTransport,
    ) {
        let need_refresh = refresh.is_needed();
        let outcome = {
            let mut state = self.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
            outcome.emit();
        } else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                need_refresh,
                "user info update was rejected by room state"
            );
        }
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

    /// Apply one membership command and finalize its deferred effects.
    ///
    /// This is the common sequencing point for join, close and disconnect. It
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
        let outcome = match transition {
            UserTransition::Join {
                user_id,
                label,
                permissions,
                sender,
                emit_joined_fanout,
                home_placement,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_join_on_placement(
                        user_id,
                        label,
                        permissions,
                        sender,
                        emit_joined_fanout,
                        home_placement,
                    )?
                };
                UserTransitionOutcome::Join(outcome)
            }
            UserTransition::Close {
                user_id,
                connection_id,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_leave(user_id, connection_id)
                };
                UserTransitionOutcome::Close {
                    outcome,
                    user_id: user_id.clone(),
                    connection_id,
                }
            }
            UserTransition::Disconnect { user_ids } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_disconnect_users(user_ids)
                };
                UserTransitionOutcome::Disconnect(outcome)
            }
        };
        Ok(Some(outcome))
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
        match outcome {
            UserTransitionOutcome::Join(outcome) => {
                self.finalize_join_transition(outcome, cleanup).await
            }
            UserTransitionOutcome::Close {
                outcome,
                user_id,
                connection_id,
            } => {
                self.finalize_close_transition(outcome, user_id, connection_id, cleanup)
                    .await
            }
            UserTransitionOutcome::Disconnect(outcome) => {
                self.finalize_disconnect_transition(outcome, cleanup).await
            }
        }
    }

    /// Finalize a committed join after room state has allocated its connection.
    ///
    /// Replacement joins may carry media cleanup for the previous session. The
    /// new session is registered with diagnostics and transport placement only
    /// after the state transition succeeds.
    async fn finalize_join_transition(
        &self,
        outcome: JoinUserOutcome,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let connection_id = outcome.connection_id;
        self.placement_ledger
            .register_committed_placement(connection_id, outcome.transport_home_placement);
        if let Some(media_transport) = cleanup.cleaning_media_transport() {
            execute_relay_route_effects(self, media_transport, &outcome.relay_effects).await;
        }
        self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
            .await;
        if let Some(media_transport) = cleanup.media_transport() {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
        }
        let user_id = outcome.user_id.clone();
        Self::emit_lifecycle_effects(outcome.effects);
        self.diagnostics.register_user(self.uuid(), &user_id);
        self.diagnostics.record(
            DiagnosticsEventData::for_user(self.uuid(), &user_id, telemetry_event::USER_JOINED)
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(outcome.transport_media_worker_id),
        );
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
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let had_state = outcome.is_some();
        let media_worker_id = self
            .placement_ledger
            .media_worker_id_for_connection(connection_id);
        if let Some(outcome) = outcome {
            if let Some(media_transport) = cleanup.cleaning_media_transport() {
                execute_relay_route_effects(self, media_transport, &outcome.relay_effects).await;
            }
            self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                .await;
            Self::emit_lifecycle_effects(outcome.effects);
        }
        self.close_transport_user_for_cleanup(&user_id, connection_id, cleanup)
            .await;
        if had_state {
            self.record_closed_user(&user_id, connection_id, media_worker_id, cleanup)
                .await;
        }
        self.placement_ledger
            .unregister_committed_placement(connection_id);
        if had_state {
            UserTransitionResult::Applied
        } else {
            UserTransitionResult::Missing
        }
    }

    /// Record diagnostics for a live session that was removed by close.
    ///
    /// This is separate from transport cleanup because diagnostics track the
    /// room-level lifecycle, while cleanup tracks adapter resources that may
    /// already be detached from state.
    async fn record_closed_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_worker_id: usize,
        cleanup: UserCleanup<'_>,
    ) {
        self.diagnostics.record(
            DiagnosticsEventData::for_user(self.uuid(), user_id, telemetry_event::USER_CLOSED)
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(media_worker_id),
        );
        self.diagnostics.forget_user(self.uuid(), user_id);
        if let Some(media_transport) = cleanup.media_transport() {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
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
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        if let Some(media_transport) = cleanup.cleaning_media_transport() {
            execute_relay_route_effects(self, media_transport, &outcome.relay_effects).await;
        }
        self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
            .await;
        for disconnected_session in &outcome.disconnected_users {
            self.close_transport_user_for_cleanup(
                &disconnected_session.user_id,
                disconnected_session.connection_id,
                cleanup,
            )
            .await;
        }
        for disconnected_session in &outcome.disconnected_users {
            self.record_disconnected_user(
                &disconnected_session.user_id,
                disconnected_session.connection_id,
            );
        }
        if let Some(media_transport) = cleanup.media_transport() {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
        }
        Self::emit_lifecycle_effects(outcome.effects);
        UserTransitionResult::Applied
    }

    /// Record diagnostics for a user removed by the bulk disconnect path.
    fn record_disconnected_user(&self, user_id: &UserId, connection_id: ConnectionId) {
        let media_worker_id = self
            .placement_ledger
            .media_worker_id_for_connection(connection_id);
        self.diagnostics.record(
            DiagnosticsEventData::for_user(
                self.uuid(),
                user_id,
                telemetry_event::USER_DISCONNECTED,
            )
            .with_media_worker_id(media_worker_id),
        );
        self.diagnostics.forget_user(self.uuid(), user_id);
        self.placement_ledger
            .unregister_committed_placement(connection_id);
    }

    /// Emit close requests and room fan-outs captured by a state transition.
    ///
    /// Send failures are ignored because lifecycle effects are best-effort
    /// notifications after room state has already committed the membership
    /// change.
    pub(super) fn emit_lifecycle_effects(effects: LifecycleEffects) {
        for close_request in effects.close_requests {
            let _ = close_request
                .sender
                .send(UserOutbound::Close(close_request.reason));
        }
        for fanout in effects.fanouts {
            fanout.emit();
        }
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
        let update = {
            let mut state = self.state.write().await;
            state.set_user_negotiated(user_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update(user_id, connection_id, update, media_port)
            .await
    }

    /// Apply the side effects that follow a negotiation state change.
    ///
    /// Consumer bootstrap is deferred until room state says the session became
    /// ready to receive. Keyframe refresh requests are best-effort transport
    /// hints and do not make the negotiation outcome fail.
    async fn apply_negotiation_update(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        update: UserNegotiationUpdate,
        media_port: &MediaTransport,
    ) -> SessionNegotiationOutcome {
        if !update.session_present {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if update.became_consumer_ready {
            if !self
                .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_port)
                .await
            {
                return SessionNegotiationOutcome::StaleConnection;
            }
            self.request_active_video_consumer_keyframes(user_id, connection_id, media_port)
                .await;
        }
        SessionNegotiationOutcome::Applied
    }

    /// Refresh consumer-side media after a renegotiation answer.
    ///
    /// This does not update the stored RTP capability set. It revalidates that
    /// the connection is still current, requests keyframes for active video
    /// consumers and bootstraps any consumers that became possible after the
    /// renegotiation.
    pub(crate) async fn apply_session_refreshed(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &MediaTransport,
    ) -> SessionNegotiationOutcome {
        if !self
            .request_active_video_consumer_keyframes(user_id, connection_id, media_port)
            .await
        {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if !self
            .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_port)
            .await
        {
            return SessionNegotiationOutcome::StaleConnection;
        }
        SessionNegotiationOutcome::Applied
    }

    /// Request keyframes for active video consumers owned by one live session.
    ///
    /// The target list is an authoritative room-state snapshot for the current
    /// connection. Individual transport request failures are logged but kept
    /// best-effort because a later media packet or refresh can recover video.
    async fn request_active_video_consumer_keyframes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &MediaTransport,
    ) -> bool {
        let Some(keyframe_refresh_targets) = ({
            let state = self.state.read().await;
            state.active_video_consumer_keyframe_refresh_targets(user_id, connection_id)
        }) else {
            return false;
        };
        for target in keyframe_refresh_targets {
            let route = TransportConsumerRoute::new(
                self.transport_user_key(user_id, connection_id),
                target.consumer_media,
                self.transport_user_key(&target.producer_user_id, target.producer_connection_id),
                target.source_media,
            );
            if media_port.request_consumer_keyframe(&route).await.is_err() {
                warn!(
                    ?user_id,
                    connection_id = ?connection_id,
                    producer_user_id = ?target.producer_user_id,
                    source_transport_media_id = ?target.source_media,
                    "media transport failed to request a refreshed consumer keyframe"
                );
            }
        }
        true
    }

    /// Return the current authoritative number of live room users.
    pub(super) async fn user_count(&self) -> usize {
        self.state.read().await.user_count()
    }

    /// Return whether room state currently has no live users.
    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
