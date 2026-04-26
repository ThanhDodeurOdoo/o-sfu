use std::{collections::BTreeMap, sync::Arc};

use o_sfu_protocol::{
    shared::{DownloadStates, JsonPayload, RecordingStateUpdate, StreamType, UserId, UserInfo},
    signaling::{
        NegotiationUploadEncoding, NegotiationUploadSlot, RequestId, ServerMessage, ServerRequest,
        ServerResponse, SessionDescriptionPayload, WelcomePayload,
    },
};
use tokio::runtime::Handle;
use tracing::{debug, info, instrument, warn};

use crate::{
    ConnectionId, MediaEndpointHealth, MediaNegotiationOffer, MediaUploadEncoding, MediaUploadSlot,
    RuntimeSfuCore, SfuCoreError,
    runtime::{
        room::{RemoteTrackBootstrap, Room, RoomEventMessage, TrackBindingUpdate},
        telemetry::schema::event as telemetry_event,
    },
    user_state::{
        PendingUserAction, PendingUserRequest, RenegotiationDisposition, ResolvedUserNegotiation,
        UserState,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserOutput {
    signals: Vec<UserSignal>,
}

impl UserOutput {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signals: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_signal(mut self, signal: UserSignal) -> Self {
        self.signals.push(signal);
        self
    }

    #[must_use]
    pub fn with_signals(mut self, signals: impl IntoIterator<Item = UserSignal>) -> Self {
        self.signals.extend(signals);
        self
    }

    pub fn extend(&mut self, other: Self) {
        self.signals.extend(other.signals);
    }

    #[must_use]
    pub fn signal_count(&self) -> usize {
        self.signals.len()
    }

    #[must_use]
    pub fn into_signals(self) -> Vec<UserSignal> {
        self.signals
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserSignal {
    Message(ServerMessage),
    Request {
        request_id: RequestId,
        request: ServerRequest,
    },
    Response {
        response_to: RequestId,
        response: ServerResponse,
    },
}

impl UserSignal {
    #[must_use]
    pub const fn request(request_id: RequestId, request: ServerRequest) -> Self {
        Self::Request {
            request_id,
            request,
        }
    }

    #[must_use]
    pub const fn response(response_to: RequestId, response: ServerResponse) -> Self {
        Self::Response {
            response_to,
            response,
        }
    }
}

impl From<ServerMessage> for UserSignal {
    fn from(message: ServerMessage) -> Self {
        Self::Message(message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDisconnectReason {
    TransportDisconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    ProtocolViolation,
    Kicked,
    InternalError,
}

#[derive(Debug)]
pub struct User {
    id: UserId,
    connection_id: ConnectionId,
    remote_address: Arc<str>,
    room: Arc<Room>,
    media_core: RuntimeSfuCore,
    state: UserState,
    cleanup_finished: bool,
}

impl User {
    #[must_use]
    pub fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: Arc<Room>,
        media_core: RuntimeSfuCore,
    ) -> Self {
        Self {
            id: user_id,
            connection_id,
            remote_address,
            media_core,
            room,
            state: UserState::default(),
            cleanup_finished: false,
        }
    }

    #[must_use]
    pub fn disconnect_reason(&self) -> Option<UserDisconnectReason> {
        self.media_core
            .endpoint_health(self.room.as_ref(), &self.id, self.connection_id)
            .and_then(|health| match health {
                MediaEndpointHealth::Disconnected => {
                    Some(UserDisconnectReason::TransportDisconnected)
                }
                MediaEndpointHealth::Connected => None,
            })
    }

    pub async fn start(&mut self) -> Result<UserOutput, UserError> {
        let mut output = UserOutput::new().with_signal(
            ServerMessage::Welcome(WelcomePayload {
                features: self.room.available_features(),
                recording: self.room.recording_state().await,
                peers: self.room.user_snapshots_except(&self.id).await,
            })
            .into(),
        );
        output.extend(self.create_initial_offer().await?);
        Ok(output)
    }

    pub async fn close(&mut self) {
        self.media_core
            .rollback_connection_publishes(self.room.as_ref(), &self.id, self.connection_id)
            .await;
        self.cleanup_finished = true;
    }

    pub async fn update_info(&self, info: UserInfo) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.media_core
            .update_user_info(
                self.room.as_ref(),
                &self.id,
                self.connection_id,
                info,
                false,
            )
            .await;
        Ok(UserOutput::new())
    }

    pub async fn broadcast(&self, message: JsonPayload) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.room
            .broadcast_runtime(&self.id, self.connection_id, message)
            .await;
        Ok(UserOutput::new())
    }

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
        if self.state.negotiation.has_queued_publish(stream_type)
            || self
                .media_core
                .has_staged_publish(
                    self.room.as_ref(),
                    &self.id,
                    self.connection_id,
                    stream_type,
                )
                .await
        {
            return Ok(UserOutput::new());
        }
        if self
            .media_core
            .is_stream_published(self.room.as_ref(), &self.id, stream_type)
            .await
        {
            self.media_core
                .set_publication_active(
                    self.room.as_ref(),
                    &self.id,
                    self.connection_id,
                    stream_type,
                    true,
                )
                .await;
            return Ok(UserOutput::new());
        }
        if self.state.negotiation.awaiting_answer() {
            self.state.negotiation.queue_publish_stream(stream_type);
            let _disposition = self.state.negotiation.request_renegotiation();
            return Ok(UserOutput::new());
        }
        if !self.stage_publish_stream(stream_type).await {
            return Ok(UserOutput::new());
        }
        self.request_renegotiation().await
    }

    pub async fn unpublish(&mut self, stream_type: StreamType) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        if self.state.negotiation.clear_queued_publish(stream_type) {
            return Ok(UserOutput::new());
        }
        if self
            .media_core
            .rollback_staged_publish(
                self.room.as_ref(),
                &self.id,
                self.connection_id,
                stream_type,
            )
            .await
        {
            let _disposition = self.state.negotiation.request_renegotiation();
            return Ok(UserOutput::new());
        }
        if !self
            .media_core
            .unpublish(
                self.room.as_ref(),
                &self.id,
                self.connection_id,
                stream_type,
            )
            .await
        {
            return Ok(UserOutput::new());
        }
        self.request_renegotiation().await
    }

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
        self.media_core
            .update_subscription(
                self.room.as_ref(),
                &self.id,
                self.connection_id,
                target_user_id,
                states,
            )
            .await;
        info!(
            event = telemetry_event::SUBSCRIBE_SUCCEEDED,
            operation = "consume_prepare",
            outcome = "applied",
            "applied subscribe intent"
        );
        Ok(UserOutput::new())
    }

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
        self.apply_negotiation_action(&resolved.pending, &answer.sdp, &response_to)
            .await?;
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

    pub async fn notify_broadcast(
        &mut self,
        sender_id: UserId,
        message: JsonPayload,
    ) -> Result<UserOutput, UserError> {
        self.apply_room_message(RoomEventMessage::Broadcast { sender_id, message })
            .await
    }

    pub async fn add_remote_user(
        &mut self,
        user_id: UserId,
        info: UserInfo,
    ) -> Result<UserOutput, UserError> {
        self.apply_room_message(RoomEventMessage::UserJoined { user_id, info })
            .await
    }

    pub async fn remove_remote_user(&mut self, user_id: UserId) -> Result<UserOutput, UserError> {
        self.apply_room_message(RoomEventMessage::UserDeparted { user_id })
            .await
    }

    pub async fn update_remote_users(
        &mut self,
        snapshot: BTreeMap<UserId, UserInfo>,
    ) -> Result<UserOutput, UserError> {
        self.apply_room_message(RoomEventMessage::UserInfoChanged(snapshot))
            .await
    }

    pub async fn update_recording_state(
        &mut self,
        state: RecordingStateUpdate,
    ) -> Result<UserOutput, UserError> {
        self.apply_room_message(RoomEventMessage::RecordingStateChanged(state))
            .await
    }

    pub async fn add_remote_track(
        &mut self,
        track: RemoteTrackBootstrap,
    ) -> Result<UserOutput, UserError> {
        self.state.tracks.apply_remote_track_bootstrap(&track);
        let mut output = UserOutput::new()
            .with_signal(ServerMessage::Tracks(self.state.tracks.snapshot()).into());
        output.extend(self.request_renegotiation().await?);
        Ok(output)
    }

    pub async fn update_remote_track(
        &mut self,
        update: TrackBindingUpdate,
    ) -> Result<UserOutput, UserError> {
        let translated = self.state.tracks.translate_track_binding_update(&update);
        let mut output =
            UserOutput::new().with_signals(translated.messages.into_iter().map(UserSignal::from));
        if translated.needs_renegotiation {
            output.extend(self.request_renegotiation().await?);
        }
        Ok(output)
    }

    async fn reject_stale_connection(&self) -> Result<(), UserError> {
        if self.room.has_connection(&self.id, self.connection_id).await {
            return Ok(());
        }
        debug!(
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            "rejecting intent from a stale user connection"
        );
        Err(UserError::Kicked)
    }

    async fn apply_room_message(
        &mut self,
        message: RoomEventMessage,
    ) -> Result<UserOutput, UserError> {
        let translated = self.state.tracks.translate_room_message(message);
        let mut output =
            UserOutput::new().with_signals(translated.messages.into_iter().map(UserSignal::from));
        if translated.needs_renegotiation {
            output.extend(self.request_renegotiation().await?);
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
        let (offer, offered_capabilities) = self
            .media_core
            .create_initial_offer(self.room.as_ref(), &self.id, self.connection_id)
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "initial_offer_create",
                    outcome = "transport_error",
                    user_id = ?self.id,
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
        self.state.negotiation.issue(request_id, request, action);
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
    async fn request_renegotiation(&mut self) -> Result<UserOutput, UserError> {
        match self.state.negotiation.request_renegotiation() {
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
                user_id = ?self.id,
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
        let Some(resolved) = self.state.negotiation.resolve_answer(response_to) else {
            warn!(
                user_id = ?self.id,
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
            self.stage_queued_publish_streams().await || resolved.queued_renegotiation;
        if !needs_follow_up {
            return Ok(UserOutput::new());
        }
        self.request_renegotiation().await
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingUserRequest,
        answer_sdp: &str,
        response_to: &RequestId,
    ) -> Result<(), UserError> {
        let result = match &pending.action {
            PendingUserAction::EstablishSession {
                offered_capabilities,
            } => {
                self.media_core
                    .apply_initial_answer(
                        self.room.as_ref(),
                        &self.id,
                        self.connection_id,
                        answer_sdp,
                        offered_capabilities,
                    )
                    .await
            }
            PendingUserAction::RefreshSession => {
                self.media_core
                    .apply_renegotiation_answer(
                        self.room.as_ref(),
                        &self.id,
                        self.connection_id,
                        answer_sdp,
                    )
                    .await
            }
        };
        if let Err(error) = result {
            self.log_negotiation_apply_error(response_to, pending, error);
            return Err(map_core_negotiation_error(error));
        }
        Ok(())
    }

    async fn create_renegotiation_offer(&self) -> Result<Option<MediaNegotiationOffer>, UserError> {
        self.media_core
            .create_renegotiation_offer(self.room.as_ref(), &self.id, self.connection_id)
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "renegotiation_offer_create",
                    outcome = "transport_error",
                    user_id = ?self.id,
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
            SfuCoreError::UserStateCommitRejected => (
                "answer_apply",
                "room_commit_failed",
                "failed to commit negotiated user state after initial answer",
            ),
            SfuCoreError::UserStateRefreshRejected => (
                "renegotiation_apply",
                "room_refresh_failed",
                "failed to refresh user state after renegotiation answer",
            ),
        };
        warn!(
            event = telemetry_event::NEGOTIATION_FAILED,
            operation,
            outcome,
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            remote_address = self.remote_address.as_ref(),
            ?response_to,
            request = ?pending.request,
            ?error,
            "{message}"
        );
    }

    async fn stage_publish_stream(&self, stream_type: StreamType) -> bool {
        let staged = self
            .media_core
            .stage_publish(
                self.room.as_ref(),
                &self.id,
                self.connection_id,
                stream_type,
            )
            .await;
        if staged {
            info!(
                event = telemetry_event::PUBLISH_PREPARED,
                operation = "publish_prepare",
                outcome = "staged",
                stream_type = ?stream_type,
                "staged publish stream for negotiation"
            );
        } else {
            info!(
                event = telemetry_event::PUBLISH_ABORTED,
                operation = "publish_prepare",
                outcome = "ignored",
                stream_type = ?stream_type,
                "publish intent did not stage new media"
            );
        }
        staged
    }

    async fn stage_queued_publish_streams(&mut self) -> bool {
        let queued_publish_streams = self.state.negotiation.take_queued_publish_streams();
        let mut staged_any = false;
        for stream_type in queued_publish_streams {
            if self.stage_publish_stream(stream_type).await {
                staged_any = true;
            }
        }
        staged_any
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
    fn drop(&mut self) {
        if self.cleanup_finished {
            return;
        }
        let media_core = self.media_core.clone();
        let room = Arc::clone(&self.room);
        let user_id = self.id.clone();
        let connection_id = self.connection_id;
        if let Ok(runtime_handle) = Handle::try_current() {
            runtime_handle.spawn(async move {
                media_core
                    .rollback_connection_publishes(room.as_ref(), &user_id, connection_id)
                    .await;
            });
        }
    }
}

fn session_description_payload(offer: MediaNegotiationOffer) -> SessionDescriptionPayload {
    SessionDescriptionPayload {
        sdp: offer.sdp,
        upload_slots: offer
            .upload_slots
            .into_iter()
            .map(protocol_upload_slot)
            .collect(),
    }
}

fn protocol_upload_slot(slot: MediaUploadSlot) -> NegotiationUploadSlot {
    NegotiationUploadSlot {
        mid: slot.mid,
        kind: slot.kind,
        codecs: slot.codecs,
        simulcast_encodings: slot
            .simulcast_encodings
            .into_iter()
            .map(protocol_upload_encoding)
            .collect(),
    }
}

fn protocol_upload_encoding(encoding: MediaUploadEncoding) -> NegotiationUploadEncoding {
    NegotiationUploadEncoding {
        rid: encoding.rid,
        max_bitrate: encoding.max_bitrate,
    }
}

fn negotiation_operation_name(action: &PendingUserAction) -> &'static str {
    match action {
        PendingUserAction::EstablishSession { .. } => "initial_offer_create",
        PendingUserAction::RefreshSession => "renegotiation_offer_create",
    }
}

fn map_core_negotiation_error(error: SfuCoreError) -> UserError {
    match error {
        SfuCoreError::Transport(_) => UserError::InternalError,
        SfuCoreError::CapabilityProjection(_)
        | SfuCoreError::UserStateCommitRejected
        | SfuCoreError::UserStateRefreshRejected => UserError::ProtocolViolation,
    }
}
