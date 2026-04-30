use std::{collections::BTreeMap, sync::Arc};

use o_sfu_protocol::{
    shared::{DownloadStates, JsonPayload, RecordingStateUpdate, StreamType, UserId, UserInfo},
    signaling::{
        NegotiationUploadEncoding, NegotiationUploadSlot, RequestId, ServerMessage, ServerRequest,
        ServerResponse, SessionDescriptionPayload,
        UploadLayerPolicyRole as ProtocolUploadLayerPolicyRole, WelcomePayload,
    },
};
use o_sfu_router::MediaKind;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    core::{
        MediaEndpointHealth, MediaSession, NegotiationOffer, PublicationActivity,
        RollbackStagedPublishOutcome, RuntimeSfuCore, RuntimeTransportAdapter,
        SessionNegotiationOutcome, SfuCoreError, SubscriptionUpdateOutcome, TransportEffectOutcome,
        UnpublishOutcome, UploadEncoding, UploadLayerPolicyRole, UploadSlot, UserInfoRefresh,
    },
    runtime::{
        ConnectionId,
        room::{RemoteTrackBootstrap, Room, RoomEventMessage, TrackBindingUpdate},
        telemetry::schema::event as telemetry_event,
    },
};

mod state;

use state::{
    PendingUserAction, PendingUserRequest, RenegotiationDisposition, ResolvedUserNegotiation,
    UserState,
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
enum UnpublishMediaDisposition {
    RolledBackStagedPublish { cleanup: TransportEffectOutcome },
    RemovedLivePublication,
    UnpublishFailed,
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

    fn media(&self) -> MediaSession<'_, Room, RuntimeTransportAdapter> {
        self.media_core
            .session(self.room.as_ref(), &self.id, self.connection_id)
    }

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

    pub async fn close(&mut self) {
        if self.cleanup_finished {
            return;
        }
        self.media().rollback_connection_publishes().await;
        self.cleanup_finished = true;
    }

    pub async fn update_info(&self, info: UserInfo) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.media()
            .update_user_info(info, UserInfoRefresh::NotNeeded)
            .await;
        Ok(UserOutput::new())
    }

    pub async fn broadcast(&self, message: JsonPayload) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.room
            .broadcast(&self.id, self.connection_id, message)
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
        let has_queued_publish = self.state.negotiation.has_queued_publish(stream_type);
        {
            let media = self.media();
            if has_queued_publish || media.has_staged_publish(stream_type).await {
                return Ok(UserOutput::new());
            }
            if media.is_stream_published(stream_type).await {
                media
                    .set_publication_activity(stream_type, PublicationActivity::Active)
                    .await;
                return Ok(UserOutput::new());
            }
        }
        if self.state.negotiation.awaiting_answer() {
            self.state.negotiation.queue_publish_slot(stream_type);
            let _disposition = self.state.negotiation.request_renegotiation();
            return Ok(UserOutput::new());
        }
        if !self.stage_publish_slot(stream_type).await? {
            return Ok(UserOutput::new());
        }
        self.request_renegotiation().await
    }

    pub async fn unpublish(&mut self, stream_type: StreamType) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        if self.state.negotiation.clear_queued_publish(stream_type) {
            return Ok(UserOutput::new());
        }
        let media_disposition = {
            let media = self.media();
            match media.rollback_staged_publish(stream_type).await {
                RollbackStagedPublishOutcome::RolledBack { cleanup } => {
                    Some(UnpublishMediaDisposition::RolledBackStagedPublish { cleanup })
                }
                RollbackStagedPublishOutcome::NotStaged => match media.unpublish(stream_type).await
                {
                    UnpublishOutcome::Unpublished => {
                        Some(UnpublishMediaDisposition::RemovedLivePublication)
                    }
                    UnpublishOutcome::MissingPublication => None,
                    UnpublishOutcome::TransportCleanupFailed
                    | UnpublishOutcome::StateCommitRejected => {
                        Some(UnpublishMediaDisposition::UnpublishFailed)
                    }
                },
            }
        };
        match media_disposition {
            Some(UnpublishMediaDisposition::RolledBackStagedPublish { cleanup }) => {
                Self::log_staged_publish_rollback(stream_type, cleanup);
                let _disposition = self.state.negotiation.request_renegotiation();
                Ok(UserOutput::new())
            }
            Some(UnpublishMediaDisposition::RemovedLivePublication) => {
                self.request_renegotiation().await
            }
            Some(UnpublishMediaDisposition::UnpublishFailed) => Err(UserError::InternalError),
            None => Ok(UserOutput::new()),
        }
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
        let outcome = self
            .media()
            .update_subscription(target_user_id, states)
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
        self.state.wire_state.apply_remote_track_bootstrap(&track);
        let mut output = UserOutput::new()
            .with_signal(ServerMessage::Tracks(self.state.wire_state.snapshot()).into());
        output.extend(self.request_renegotiation().await?);
        Ok(output)
    }

    pub async fn update_remote_track(
        &mut self,
        update: TrackBindingUpdate,
    ) -> Result<UserOutput, UserError> {
        let wire_messages = self
            .state
            .wire_state
            .messages_for_track_binding_update(&update);
        let mut output = UserOutput::new()
            .with_signals(wire_messages.messages.into_iter().map(UserSignal::from));
        if wire_messages.needs_renegotiation {
            output.extend(self.request_renegotiation().await?);
        }
        Ok(output)
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

    async fn apply_room_message(
        &mut self,
        message: RoomEventMessage,
    ) -> Result<UserOutput, UserError> {
        let wire_messages = self.state.wire_state.messages_for_room_event(message);
        let mut output = UserOutput::new()
            .with_signals(wire_messages.messages.into_iter().map(UserSignal::from));
        if wire_messages.needs_renegotiation {
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
        let Some(resolved) = self.state.negotiation.resolve_answer(response_to) else {
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
        self.request_renegotiation().await
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingUserRequest,
        answer_sdp: &str,
        response_to: &RequestId,
    ) -> Result<(), UserError> {
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
        if let Err(error) = result {
            self.log_negotiation_apply_error(response_to, pending, error);
            return Err(map_core_negotiation_error(error));
        }
        Ok(())
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
        let media_kind = media_kind_for_stream_type(stream_type);
        let outcome = self
            .media()
            .stage_publish(stream_type)
            .await
            .map_err(|error| {
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
        let queued_publish_slots = self.state.negotiation.take_queued_publish_slots();
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

const fn media_kind_for_stream_type(stream_type: StreamType) -> MediaKind {
    match stream_type {
        StreamType::Audio => MediaKind::Audio,
        StreamType::Camera | StreamType::Screen => MediaKind::Video,
    }
}

fn session_description_payload(offer: NegotiationOffer) -> SessionDescriptionPayload {
    SessionDescriptionPayload {
        sdp: offer.sdp,
        upload_slots: offer
            .upload_slots
            .into_iter()
            .map(protocol_upload_slot)
            .collect(),
    }
}

fn protocol_upload_slot(slot: UploadSlot) -> NegotiationUploadSlot {
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

fn protocol_upload_encoding(encoding: UploadEncoding) -> NegotiationUploadEncoding {
    NegotiationUploadEncoding {
        rid: encoding.rid,
        max_bitrate: encoding.max_bitrate,
        resolution_scale: encoding.resolution_scale,
        max_framerate: encoding.max_framerate,
        policy_role: encoding.policy_role.map(protocol_upload_layer_policy_role),
    }
}

fn protocol_upload_layer_policy_role(role: UploadLayerPolicyRole) -> ProtocolUploadLayerPolicyRole {
    match role {
        UploadLayerPolicyRole::Featured => ProtocolUploadLayerPolicyRole::Featured,
        UploadLayerPolicyRole::Thumbnail => ProtocolUploadLayerPolicyRole::Thumbnail,
        UploadLayerPolicyRole::DegradedThumbnail => {
            ProtocolUploadLayerPolicyRole::DegradedThumbnail
        }
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
        | SfuCoreError::SessionNegotiationRejected(_)
        | SfuCoreError::SessionRefreshRejected(_) => UserError::ProtocolViolation,
    }
}
