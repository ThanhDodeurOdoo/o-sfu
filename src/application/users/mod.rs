use std::sync::Arc;

use o_sfu_protocol::{
    shared::UserId,
    signaling::{ClientEnvelope, RequestId, ServerMessage},
};
use tokio::runtime::Handle;

mod envelope_dispatch;
mod flow_state;
mod negotiation_flow;
mod publish_flow;
mod track_projection;

use flow_state::SessionFlowState;
use track_projection::RemoteTrackProjection;

use crate::{
    application::{
        outcomes::{CallOutcome, UserEndReason, UserError, UserSignal},
        rooms::{RoomHandle, RoomMessageEvent, RoomRequestEvent, RoomTrackBindingUpdate},
    },
    core::{MediaEndpointHealth, RuntimeSfuCore},
    runtime::ConnectionId,
};

#[derive(Debug, Default)]
struct ServerRequestIdState {
    next_request_counter: u64,
}

impl ServerRequestIdState {
    fn next_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UserIntent {
    ClientEnvelope(ClientEnvelope),
}

#[derive(Debug, Clone)]
pub(crate) enum RoomEvent {
    Message(RoomMessageEvent),
    Request(RoomRequestEvent),
    TrackBindingUpdate(RoomTrackBindingUpdate),
}

/// The main orchestrator for an authenticated room user.
///
/// It centralizes envelope dispatch, renegotiation sequencing, and staged publish
/// transitions behind one user-scoped owner. The websocket edge decodes and
/// renders frames; this type owns the user-in-room business flow.
#[derive(Debug)]
pub(crate) struct User {
    id: UserId,
    connection_id: ConnectionId,
    remote_address: Arc<str>,
    room: RoomHandle,
    media_core: RuntimeSfuCore,
    request_ids: ServerRequestIdState,
    flow_state: SessionFlowState,
    track_projection: RemoteTrackProjection,
    cleanup_finished: bool,
}

impl User {
    pub(crate) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: RoomHandle,
        media_core: RuntimeSfuCore,
    ) -> Self {
        Self {
            id: user_id,
            connection_id,
            remote_address,
            media_core,
            room,
            request_ids: ServerRequestIdState::default(),
            flow_state: SessionFlowState::default(),
            track_projection: RemoteTrackProjection::default(),
            cleanup_finished: false,
        }
    }

    pub(crate) fn end_reason(&self) -> Option<UserEndReason> {
        self.media_core
            .endpoint_health(self.room.as_core_room(), &self.id, self.connection_id)
            .and_then(|health| match health {
                MediaEndpointHealth::Disconnected => Some(UserEndReason::TransportDisconnected),
                MediaEndpointHealth::Connected => None,
            })
    }

    pub(crate) async fn bootstrap(&mut self) -> Result<CallOutcome, UserError> {
        self.send_initial_offer().await
    }

    pub(crate) async fn handle_intent(
        &mut self,
        intent: UserIntent,
    ) -> Result<CallOutcome, UserError> {
        match intent {
            UserIntent::ClientEnvelope(envelope) => self.handle_client_envelope(envelope).await,
        }
    }

    pub(crate) async fn handle_room_event(
        &mut self,
        event: RoomEvent,
    ) -> Result<CallOutcome, UserError> {
        match event {
            RoomEvent::Message(message) => self.handle_room_message(message).await,
            RoomEvent::Request(request) => self.handle_room_request(request).await,
            RoomEvent::TrackBindingUpdate(update) => self.handle_track_binding_update(update).await,
        }
    }

    pub(crate) async fn finish(&mut self) {
        self.media_core
            .rollback_connection_publishes(self.room.as_core_room(), &self.id, self.connection_id)
            .await;
        self.cleanup_finished = true;
    }

    async fn handle_room_message(
        &mut self,
        message: RoomMessageEvent,
    ) -> Result<CallOutcome, UserError> {
        let translated = self.track_projection.translate_server_message(message);
        let mut call_outcome =
            CallOutcome::new().with_signals(translated.messages.into_iter().map(UserSignal::from));
        if translated.needs_renegotiation {
            call_outcome.extend(self.request_renegotiation().await?);
        }
        Ok(call_outcome)
    }

    async fn handle_room_request(
        &mut self,
        request: RoomRequestEvent,
    ) -> Result<CallOutcome, UserError> {
        match request {
            RoomRequestEvent::BootstrapRemoteTrack(payload) => {
                self.track_projection.apply_remote_track_bootstrap(&payload);
                let mut call_outcome = CallOutcome::new()
                    .with_signal(ServerMessage::Tracks(self.track_projection.snapshot()).into());
                call_outcome.extend(self.request_renegotiation().await?);
                Ok(call_outcome)
            }
        }
    }

    async fn handle_track_binding_update(
        &mut self,
        update: RoomTrackBindingUpdate,
    ) -> Result<CallOutcome, UserError> {
        let translated = self
            .track_projection
            .translate_track_binding_update(&update);
        let mut call_outcome =
            CallOutcome::new().with_signals(translated.messages.into_iter().map(UserSignal::from));
        if translated.needs_renegotiation {
            call_outcome.extend(self.request_renegotiation().await?);
        }
        Ok(call_outcome)
    }
}

impl Drop for User {
    fn drop(&mut self) {
        if self.cleanup_finished {
            return;
        }
        let media_core = self.media_core.clone();
        let room = self.room.clone();
        let user_id = self.id.clone();
        let connection_id = self.connection_id;
        if let Ok(runtime_handle) = Handle::try_current() {
            runtime_handle.spawn(async move {
                media_core
                    .rollback_connection_publishes(room.as_core_room(), &user_id, connection_id)
                    .await;
            });
        }
    }
}
