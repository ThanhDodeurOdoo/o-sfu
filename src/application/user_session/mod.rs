//! this module owns the cold-path compatibility flow for one authenticated
//! websocket connection. it accepts Odoo protocol intent, translates stream
//! labels through [`crate::application::stream_catalog`] and calls the media
//! facade with generic source intents
//!
//! business-layer changes to publication shape should enter core as
//! [`crate::core::SourcePublishIntent`] and
//! [`crate::core::SourceSubscriptionIntent`] values. `User` sequences those
//! intents around negotiation, request tracking and user-info fanout, while the
//! pure connection-local state lives under `state/` and ordered websocket
//! output lives in `output`
//!
//! `User` is the post-auth websocket session facade. it keeps the
//! connection-scoped signaling state needed to answer one browser, including
//! pending request ids, staged renegotiation decisions and compatibility track
//! snapshots

use std::{collections::BTreeMap, sync::Arc};

use o_sfu_protocol::{
    shared::{DownloadStates, JsonPayload, StreamType, UserId, UserInfo},
    signaling::{
        RequestId, ServerMessage, ServerRequest, SessionDescriptionPayload, WelcomePayload,
    },
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    application::stream_catalog::{
        source_publish_intent_for_stream_type, stream_id_for_stream_type, stream_type_for_stream_id,
    },
    core::{
        MediaEndpointHealth, MediaSession, NegotiationOffer, PublicationActivity,
        PublicationActivityOutcome, RollbackStagedPublishOutcome, SessionNegotiationOutcome,
        SfuCore, SfuCoreError, SourceSubscriptionIntent, SubscriptionUpdateOutcome,
        TransportEffectOutcome, UnpublishOutcome, UserInfoRefresh, UserStreamId,
    },
    runtime::{
        ConnectionId,
        room::{RemoteTrackBootstrap, Room, RoomEventMessage, TrackBindingUpdate},
        telemetry::schema::event as telemetry_event,
    },
};

mod output;
mod projection;
mod state;

pub use output::{UserOutput, UserSignal};
use projection::session_description_payload;
use state::{
    PendingUserAction, PendingUserRequest, RenegotiationDisposition, ResolvedUserNegotiation,
    UserState,
};

/// User-loop exit reason derived from media endpoint health.
///
/// This is a best-effort transport observation. It is not an authoritative room
/// membership check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDisconnectReason {
    /// The media transport reached a terminal disconnected state.
    TransportDisconnected,
}

/// Media-side result of an unpublish request after queued work is removed.
///
/// The media facade may have already consumed staged ownership or attempted
/// transport cleanup before this value is returned. The caller uses this shape
/// to keep user-info fanout and renegotiation decisions explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnpublishMediaDisposition {
    /// A not-yet-committed publish was cancelled.
    RolledBackStagedPublish { cleanup: TransportEffectOutcome },
    /// A live room publication was removed and peers need a follow-up offer.
    RemovedLivePublication { cleanup: TransportEffectOutcome },
}

/// User-session failure category reported to the websocket runtime.
///
/// # Error handling
///
/// These errors are already translated out of core and room outcomes. The
/// websocket edge maps them to close codes, so callers should not inspect log
/// text to decide whether a socket stays usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    /// The browser sent a message that cannot be accepted for this session.
    ProtocolViolation,
    /// The room no longer owns this exact user connection.
    Kicked,
    /// A server-side media or transport operation failed.
    InternalError,
}

/// Post-auth application session for one websocket connection.
///
/// `User` owns connection-local negotiation state, local compatibility track
/// bindings for the connected browser and cleanup completion. It does not own
/// room membership, media publications or transport resources. Those stay
/// behind [`Room`] and [`MediaSession`], which keeps this boundary focused on
/// translating Odoo websocket intent into core media intent.
///
/// # Concurrency
///
/// Methods are cold-path orchestration calls. They may await room snapshots,
/// media transactions and transport effects. The room and core layers remain
/// responsible for not holding their state locks across transport work.
///
/// # Lifecycle
///
/// The websocket handshake constructs a `User` only after room admission. The
/// steady-state loop must call [`User::close`] before dropping it so staged
/// publishes that never reached room commit are rolled back explicitly.
#[derive(Debug)]
pub struct User {
    /// Compatibility-facing identity for room state and websocket payloads.
    id: UserId,
    /// Runtime-local identity that separates replacement sockets for one user.
    connection_id: ConnectionId,
    /// Log context for negotiation and media failures.
    ///
    /// The address is not part of authentication or room identity.
    remote_address: Arc<str>,
    /// Authoritative room facade for membership, snapshots and fanout.
    room: Arc<Room>,
    /// Process media facade used to build borrow-based session handles.
    sfu_core: SfuCore,
    /// Connection-local request sequencing and compatibility wire state.
    state: UserState,
    /// Whether async staged-publish cleanup has completed for this connection.
    cleanup_finished: bool,
}

impl User {
    /// Create the application session for a room-admitted websocket user.
    ///
    /// The caller must pass the normalized user id, the connection id returned
    /// by room admission and shared room/core handles. Construction does not
    /// emit the welcome payload or allocate the first offer. Call
    /// [`User::start`] to perform that post-admission initialization.
    #[must_use]
    pub fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: Arc<Room>,
        sfu_core: SfuCore,
    ) -> Self {
        Self {
            id: user_id,
            connection_id,
            remote_address,
            sfu_core,
            room,
            state: UserState::default(),
            cleanup_finished: false,
        }
    }

    /// Rebuild a borrow-based media session for this room, user and runtime
    /// connection identity.
    fn media(&self) -> MediaSession<'_> {
        self.sfu_core
            .session(self.room.as_ref(), &self.id, self.connection_id)
    }

    /// Return the current transport-driven disconnect reason, if one is known.
    ///
    /// `None` means the transport backend has not reported a terminal
    /// disconnection. It does not prove that the room still owns this
    /// connection.
    #[must_use]
    pub fn disconnect_reason(&self) -> Option<UserDisconnectReason> {
        self.media()
            .endpoint_health()
            .and_then(|health| match health {
                MediaEndpointHealth::Disconnected => {
                    Some(UserDisconnectReason::TransportDisconnected)
                }
                MediaEndpointHealth::Connected => None,
            })
    }

    /// Build the startup output for an authenticated room member.
    ///
    /// The output contains the welcome snapshot followed by the initial server
    /// offer request. The caller must send it before entering the steady-state
    /// websocket loop because later client messages depend on the pending
    /// negotiation request stored here.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::InternalError`] if the media transport cannot build
    /// the initial offer.
    pub async fn start(&mut self) -> Result<UserOutput, UserError> {
        let welcome = WelcomePayload {
            features: self.room.available_features(),
            recording: self.room.recording_state().await,
            peers: self.room.user_snapshots_except(&self.id).await,
        };
        let mut output = UserOutput::new().with_signal(ServerMessage::Welcome(welcome).into());
        output.extend(self.create_initial_offer().await?);
        Ok(output)
    }

    /// Run mandatory explicit cleanup for this connection.
    ///
    /// This is idempotent and only rolls back staged publishes owned by this
    /// websocket session. Room membership teardown and transport-session close
    /// remain the responsibility of the runtime room manager.
    pub async fn close(&mut self) {
        if self.cleanup_finished {
            return;
        }
        self.media().rollback_connection_publishes().await;
        self.cleanup_finished = true;
    }

    /// Apply a client-visible user-info update from this websocket.
    ///
    /// Stale connections are rejected before the room update is attempted. A
    /// successful update fans out through room state, so this method normally
    /// returns an empty direct output for the caller's socket.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] if the room no longer owns this connection.
    /// Returns [`UserError::ProtocolViolation`] when the payload exceeds the
    /// room broadcast byte limit.
    pub async fn update_info(&self, info: UserInfo) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.media()
            .update_user_info(info, UserInfoRefresh::NotNeeded)
            .await;
        Ok(UserOutput::new())
    }

    async fn update_publication_info(
        &self,
        stream_type: StreamType,
        active: bool,
    ) -> Result<(), UserError> {
        let Some(info) = publication_info_update(stream_type, active) else {
            return Ok(());
        };
        self.media()
            .update_user_info(info, UserInfoRefresh::NotNeeded)
            .await;
        Ok(())
    }

    /// Fan a client-authored opaque broadcast through room state.
    ///
    /// The sender connection is checked against authoritative room membership.
    /// The sender does not receive an echo through this direct output.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] if the room no longer owns this connection.
    pub async fn broadcast(&self, message: JsonPayload) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.room
            .broadcast(&self.id, self.connection_id, message)
            .await
            .map_err(|_error| UserError::ProtocolViolation)?;
        Ok(UserOutput::new())
    }

    /// Accept a client intent to publish one compatibility stream.
    ///
    /// This method translates the Odoo stream type through
    /// `stream_catalog`, stages media through core when needed and emits a
    /// renegotiation request only when a new offer is required. Duplicate
    /// publish requests are accepted as idempotent no-ops.
    ///
    /// If the stream is already live, the method only marks its user-visible
    /// activity as active and updates presence state for camera or screen.
    /// Publish requests received while another negotiation is pending are
    /// queued for a follow-up offer after the current answer is applied.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] for stale connections and
    /// [`UserError::InternalError`] when core cannot stage media for a publish
    /// that requires negotiation.
    #[instrument(
        name = "publish.intent",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            stream_type = ?stream_type
        )
    )]
    pub async fn publish(&mut self, stream_type: StreamType) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        let stream_id = stream_id_for_stream_type(stream_type);
        let has_queued_publish = self.state.negotiation_state.has_queued_publish(stream_type);
        {
            let media = self.media();
            if has_queued_publish || media.has_staged_publish(&stream_id) {
                return Ok(UserOutput::new());
            }
            if media.is_stream_published(&stream_id).await {
                let outcome = media
                    .set_publication_activity(&stream_id, PublicationActivity::Active)
                    .await;
                if matches!(outcome, PublicationActivityOutcome::Applied { .. }) {
                    self.update_publication_info(stream_type, true).await?;
                }
                return Ok(UserOutput::new());
            }
        }
        if self.state.negotiation_state.awaiting_answer() {
            self.state.negotiation_state.queue_publish_slot(stream_type);
            let _disposition = self.state.negotiation_state.schedule_renegotiation();
            return Ok(UserOutput::new());
        }
        if !self.stage_publish_slot(stream_type).await? {
            return Ok(UserOutput::new());
        }
        self.renegotiate().await
    }

    /// Accept a client intent to stop publishing one compatibility stream.
    ///
    /// The request first cancels queued or staged publish work for this
    /// connection. If the stream is already live, core removes the room
    /// publication and this session requests renegotiation so the browser can
    /// drop the media section.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] for stale connections and
    /// [`UserError::InternalError`] when core cannot remove a live publication
    /// cleanly.
    pub async fn unpublish(&mut self, stream_type: StreamType) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        if self
            .state
            .negotiation_state
            .clear_queued_publish(stream_type)
        {
            return Ok(UserOutput::new());
        }
        let media_disposition = {
            let media = self.media();
            let stream_id = stream_id_for_stream_type(stream_type);
            match media.rollback_staged_publish(&stream_id).await {
                RollbackStagedPublishOutcome::RolledBack { cleanup } => {
                    Some(UnpublishMediaDisposition::RolledBackStagedPublish { cleanup })
                }
                RollbackStagedPublishOutcome::NotStaged => {
                    match media.unpublish(&stream_id).await {
                        UnpublishOutcome::Unpublished { cleanup } => {
                            Some(UnpublishMediaDisposition::RemovedLivePublication { cleanup })
                        }
                        UnpublishOutcome::MissingPublication => None,
                    }
                }
            }
        };
        match media_disposition {
            Some(UnpublishMediaDisposition::RolledBackStagedPublish { cleanup }) => {
                Self::log_staged_publish_rollback(stream_type, cleanup);
                let _disposition = self.state.negotiation_state.schedule_renegotiation();
                Ok(UserOutput::new())
            }
            Some(UnpublishMediaDisposition::RemovedLivePublication { cleanup }) => {
                Self::log_live_unpublish(stream_type, cleanup);
                self.update_publication_info(stream_type, false).await?;
                self.renegotiate().await
            }
            None => Ok(UserOutput::new()),
        }
    }

    /// Persist this user's download intent for another room user.
    ///
    /// The compatibility [`DownloadStates`] payload is projected into generic
    /// source subscription intent before core sees it. The target user id must
    /// already be normalized by the websocket edge.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] if this connection is stale. Returns
    /// [`UserError::ProtocolViolation`] if room state rejects the subscription
    /// update as stale during commit.
    #[instrument(
        name = "subscribe.intent",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            target_session_id = ?target_user_id
        )
    )]
    pub async fn subscribe_to(
        &self,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        info!(
            event = telemetry_event::SUBSCRIBE_PREPARED,
            operation = "consume_prepare",
            outcome = "request_received",
            "received subscribe intent"
        );
        let source_intents = subscription_intents_from_download_states(states);
        let outcome = self
            .media()
            .update_subscription(target_user_id, &source_intents)
            .await;
        if outcome == SubscriptionUpdateOutcome::StaleConnection {
            return Err(UserError::ProtocolViolation);
        }
        info!(
            event = telemetry_event::SUBSCRIBE_SUCCEEDED,
            operation = "consume_prepare",
            outcome = ?outcome,
            "applied subscribe intent"
        );
        Ok(UserOutput::new())
    }

    /// Apply a browser answer for the pending offer or renegotiation request.
    ///
    /// The response id must match the current pending server request. A valid
    /// answer is applied through core, then any streams committed by the answer
    /// update presence state. Queued publishes or queued renegotiation requests
    /// may produce a follow-up offer in the returned output.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::ProtocolViolation`] for empty SDP, unknown request
    /// ids and core rejections caused by unusable answers or stale callbacks.
    /// Returns [`UserError::InternalError`] for transport failures.
    pub async fn complete_negotiation(
        &mut self,
        response_to: RequestId,
        answer: SessionDescriptionPayload,
    ) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.validate_negotiation_answer(&response_to, &answer)?;
        let Some(resolved) = self.resolve_negotiation_answer(&response_to) else {
            return Err(UserError::ProtocolViolation);
        };
        let committed_stream_ids = self
            .apply_negotiation_action(&resolved.pending, &answer.sdp, &response_to)
            .await?;
        for stream_id in committed_stream_ids {
            if let Some(stream_type) = stream_type_for_stream_id(&stream_id) {
                self.update_publication_info(stream_type, true).await?;
            }
        }
        info!(
            event = telemetry_event::NEGOTIATION_SUCCEEDED,
            operation = negotiation_operation_name(&resolved.pending.action),
            outcome = "answer_applied",
            ?response_to,
            "applied negotiation answer"
        );
        Self::record_staged_publishes_committed();
        self.send_follow_up_renegotiation_if_needed(&resolved).await
    }

    /// Bootstrap one newly visible remote track for this websocket user.
    ///
    /// The room has already decided that the receiver should see the source.
    /// This method updates only the user-local compatibility track snapshot and
    /// then requests renegotiation so the browser can receive the media.
    pub async fn add_remote_track(
        &mut self,
        track: RemoteTrackBootstrap,
    ) -> Result<UserOutput, UserError> {
        self.state.wire_state.apply_remote_track_bootstrap(&track);
        let mut output = UserOutput::new()
            .with_signal(ServerMessage::Tracks(self.state.wire_state.snapshot()).into());
        output.extend(self.renegotiate().await?);
        Ok(output)
    }

    /// Apply a room-authored remote track binding delta for this websocket.
    ///
    /// Activity updates only refresh the local track snapshot. Removal also
    /// requests renegotiation because the browser must stop receiving that
    /// remote media section.
    pub async fn update_remote_track(
        &mut self,
        update: TrackBindingUpdate,
    ) -> Result<UserOutput, UserError> {
        let wire_messages = self.state.wire_state.apply_track_binding_update(&update);
        self.finalize_wire_messages(wire_messages).await
    }

    async fn reject_stale_connection(&self) -> Result<(), UserError> {
        if self.room.has_connection(&self.id, self.connection_id).await {
            return Ok(());
        }
        debug!(
            user_id = ?&self.id,
            connection_id = ?self.connection_id,
            "rejecting intent from a stale user connection"
        );
        Err(UserError::Kicked)
    }

    /// Convert a room-authored notification into this user's websocket output.
    ///
    /// Room state has already authorized and applied the transition. This method
    /// only updates the connection-local wire snapshot before the websocket edge
    /// serializes the resulting signals.
    pub(crate) async fn apply_room_message(
        &mut self,
        message: RoomEventMessage,
    ) -> Result<UserOutput, UserError> {
        let wire_messages = self.state.wire_state.apply_room_event(message);
        self.finalize_wire_messages(wire_messages).await
    }

    async fn finalize_wire_messages(
        &mut self,
        wire_messages: state::UserWireMessages,
    ) -> Result<UserOutput, UserError> {
        let mut output = UserOutput::from_messages(wire_messages.messages);
        if wire_messages.needs_renegotiation {
            output.extend(self.renegotiate().await?);
        }
        Ok(output)
    }

    #[instrument(
        name = "transport.offer.create",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id
        )
    )]
    async fn create_initial_offer(&mut self) -> Result<UserOutput, UserError> {
        let (offer, offered_capabilities) =
            self.media().create_initial_offer().await.map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "initial_offer_create",
                    outcome = "transport_error",
                    user_id = ?&self.id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    ?error,
                    "failed to create initial transport offer"
                );
                UserError::InternalError
            })?;
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = "initial_offer_create",
            outcome = "offer_ready",
            "created initial transport offer"
        );
        let offer_request = ServerRequest::Offer(session_description_payload(offer));
        Ok(
            UserOutput::new().with_signal(self.issue_negotiation_request(
                offer_request,
                PendingUserAction::EstablishSession {
                    offered_capabilities,
                },
            )),
        )
    }

    fn issue_negotiation_request(
        &mut self,
        request: ServerRequest,
        action: PendingUserAction,
    ) -> UserSignal {
        let request_id = self.state.next_request_id();
        let signal = UserSignal::request(request_id.clone(), request.clone());
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = negotiation_operation_name(&action),
            outcome = "request_prepared",
            ?request_id,
            "prepared negotiation request"
        );
        self.state
            .negotiation_state
            .issue(request_id, request, action);
        signal
    }

    #[instrument(
        name = "transport.renegotiate",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id
        )
    )]
    async fn renegotiate(&mut self) -> Result<UserOutput, UserError> {
        match self.state.negotiation_state.schedule_renegotiation() {
            RenegotiationDisposition::Skip | RenegotiationDisposition::QueueOnly => {
                Ok(UserOutput::new())
            }
            RenegotiationDisposition::SendNow => {
                let Some(offer) = self.create_renegotiation_offer().await? else {
                    return Ok(UserOutput::new());
                };
                let request = ServerRequest::Renegotiate(session_description_payload(offer));
                Ok(UserOutput::new().with_signal(
                    self.issue_negotiation_request(request, PendingUserAction::RefreshSession),
                ))
            }
        }
    }

    fn validate_negotiation_answer(
        &self,
        response_to: &RequestId,
        answer: &SessionDescriptionPayload,
    ) -> Result<(), UserError> {
        if answer.sdp.is_empty() {
            warn!(
                user_id = ?&self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received empty SDP answer for negotiation request"
            );
            return Err(UserError::ProtocolViolation);
        }
        Ok(())
    }

    fn resolve_negotiation_answer(
        &mut self,
        response_to: &RequestId,
    ) -> Option<ResolvedUserNegotiation> {
        let Some(resolved) = self.state.negotiation_state.resolve_answer(response_to) else {
            warn!(
                user_id = ?&self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received negotiation answer for an unknown or stale request"
            );
            return None;
        };
        Some(resolved)
    }

    async fn send_follow_up_renegotiation_if_needed(
        &mut self,
        resolved: &ResolvedUserNegotiation,
    ) -> Result<UserOutput, UserError> {
        let needs_follow_up =
            self.stage_queued_publish_slots().await? || resolved.queued_renegotiation;
        if !needs_follow_up {
            return Ok(UserOutput::new());
        }
        self.renegotiate().await
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingUserRequest,
        answer_sdp: &str,
        response_to: &RequestId,
    ) -> Result<Vec<UserStreamId>, UserError> {
        let media = self.media();
        let result = match &pending.action {
            PendingUserAction::EstablishSession {
                offered_capabilities,
            } => {
                media
                    .apply_initial_answer(answer_sdp, offered_capabilities)
                    .await
            }
            PendingUserAction::RefreshSession => media.apply_renegotiation_answer(answer_sdp).await,
        };
        let committed_stream_ids = match result {
            Ok(committed_stream_ids) => committed_stream_ids,
            Err(error) => {
                self.log_negotiation_apply_error(response_to, pending, error);
                return Err(map_core_negotiation_error(error));
            }
        };
        Ok(committed_stream_ids)
    }

    async fn create_renegotiation_offer(&self) -> Result<Option<NegotiationOffer>, UserError> {
        self.media()
            .create_renegotiation_offer()
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "renegotiation_offer_create",
                    outcome = "transport_error",
                    user_id = ?&self.id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    ?error,
                    "failed to build a staged renegotiation offer"
                );
                UserError::InternalError
            })
    }

    fn log_negotiation_apply_error(
        &self,
        response_to: &RequestId,
        pending: &PendingUserRequest,
        error: SfuCoreError,
    ) {
        let (operation, outcome, message) = match error {
            SfuCoreError::Transport(_) => (
                "answer_apply",
                "transport_error",
                "failed to apply negotiation answer to the transport endpoint",
            ),
            SfuCoreError::CapabilityProjection(_) => (
                "answer_apply",
                "capability_projection_failed",
                "failed to project client RTP capabilities from the answered SDP",
            ),
            SfuCoreError::SessionNegotiationRejected(outcome) => (
                "answer_apply",
                match outcome {
                    SessionNegotiationOutcome::Applied => "room_commit_unexpected",
                    SessionNegotiationOutcome::StaleConnection => "stale_connection",
                },
                "failed to commit negotiated user state after initial answer",
            ),
            SfuCoreError::SessionRefreshRejected(outcome) => (
                "renegotiation_apply",
                match outcome {
                    SessionNegotiationOutcome::Applied => "room_refresh_unexpected",
                    SessionNegotiationOutcome::StaleConnection => "stale_connection",
                },
                "failed to refresh user state after renegotiation answer",
            ),
        };
        warn!(
            event = telemetry_event::NEGOTIATION_FAILED,
            operation,
            outcome,
            user_id = ?&self.id,
            connection_id = ?self.connection_id,
            remote_address = self.remote_address.as_ref(),
            ?response_to,
            request = ?pending.request,
            ?error,
            "{message}"
        );
    }

    async fn stage_publish_slot(&self, stream_type: StreamType) -> Result<bool, UserError> {
        let intent = source_publish_intent_for_stream_type(stream_type);
        let media_kind = intent.media_kind();
        let outcome = self.media().stage_publish(&intent).await.map_err(|error| {
            warn!(
                event = telemetry_event::PUBLISH_ABORTED,
                operation = "publish_prepare",
                outcome = "transport_error",
                media_kind = ?media_kind,
                stream_type = ?stream_type,
                ?error,
                "failed to stage publish stream for negotiation"
            );
            UserError::InternalError
        })?;
        let event = if outcome.staged() {
            telemetry_event::PUBLISH_PREPARED
        } else {
            telemetry_event::PUBLISH_ABORTED
        };
        info!(
            event,
            operation = "publish_prepare",
            outcome = ?outcome,
            media_kind = ?media_kind,
            stream_type = ?stream_type,
            "processed publish staging intent"
        );
        Ok(outcome.staged())
    }

    async fn stage_queued_publish_slots(&mut self) -> Result<bool, UserError> {
        let queued_publish_slots = self.state.negotiation_state.take_queued_publish_slots();
        let mut staged_any = false;
        for slot in queued_publish_slots {
            if self.stage_publish_slot(slot).await? {
                staged_any = true;
            }
        }
        Ok(staged_any)
    }

    fn log_staged_publish_rollback(stream_type: StreamType, cleanup: TransportEffectOutcome) {
        info!(
            event = telemetry_event::PUBLISH_ABORTED,
            operation = "publish_rollback",
            outcome = ?cleanup,
            stream_type = ?stream_type,
            "rolled back staged publish stream before commit"
        );
    }

    fn log_live_unpublish(stream_type: StreamType, cleanup: TransportEffectOutcome) {
        info!(
            operation = "publish_unpublish",
            outcome = ?cleanup,
            stream_type = ?stream_type,
            "removed live publish stream"
        );
    }

    fn record_staged_publishes_committed() {
        info!(
            event = telemetry_event::PUBLISH_COMMITTED,
            operation = "publish_commit",
            outcome = "applied",
            "committed staged publish streams"
        );
    }
}

impl Drop for User {
    /// Report missed explicit cleanup paths.
    ///
    /// `Drop` cannot await staged-publish rollback. The runtime must call
    /// [`User::close`] before this value is dropped.
    fn drop(&mut self) {
        if self.cleanup_finished {
            return;
        }
        error!(
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            "dropped websocket user without completing explicit cleanup"
        );
        debug_assert!(
            self.cleanup_finished,
            "websocket user dropped before explicit cleanup completed"
        );
    }
}

/// Project compatibility download state into core subscription intent.
///
/// Missing stream entries mean "leave that stream unchanged" for the room.
/// Present media or layout values become generic per-stream intents keyed by
/// [`UserStreamId`].
fn subscription_intents_from_download_states(
    states: &DownloadStates,
) -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    let mut intents = BTreeMap::new();
    let streams = [
        (StreamType::Audio, states.audio, None),
        (StreamType::Camera, states.camera, states.camera_layout),
        (StreamType::Screen, states.screen, states.screen_layout),
    ];
    for (stream_type, media, layout) in streams {
        if media.is_some() || layout.is_some() {
            intents.insert(
                stream_id_for_stream_type(stream_type),
                SourceSubscriptionIntent::new(media, layout),
            );
        }
    }
    intents
}

/// Build the user-info delta implied by a publication activity change.
///
/// Audio has no Odoo-visible user-info flag. Camera and screen publication
/// activity must mirror into presence so existing Discuss clients keep their
/// toolbar state in sync with negotiated media.
fn publication_info_update(stream_type: StreamType, active: bool) -> Option<UserInfo> {
    match stream_type {
        StreamType::Audio => None,
        StreamType::Camera => Some(UserInfo {
            is_camera_on: Some(active),
            ..UserInfo::default()
        }),
        StreamType::Screen => Some(UserInfo {
            is_screen_sharing_on: Some(active),
            ..UserInfo::default()
        }),
    }
}

/// Stable telemetry operation name for a pending negotiation action.
fn negotiation_operation_name(action: &PendingUserAction) -> &'static str {
    match action {
        PendingUserAction::EstablishSession { .. } => "initial_offer_create",
        PendingUserAction::RefreshSession => "renegotiation_offer_create",
    }
}

/// Collapse media-core negotiation errors into websocket-session errors.
///
/// Transport failures are server-side failures. Capability projection failures
/// and room rejections make the browser answer unusable for this session, so
/// callers close the socket as a protocol failure.
fn map_core_negotiation_error(error: SfuCoreError) -> UserError {
    match error {
        SfuCoreError::Transport(_) => UserError::InternalError,
        SfuCoreError::CapabilityProjection(_)
        | SfuCoreError::SessionNegotiationRejected(_)
        | SfuCoreError::SessionRefreshRejected(_) => UserError::ProtocolViolation,
    }
}
