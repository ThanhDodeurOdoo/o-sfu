use tracing::error;

use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::shared::{DownloadStates, SessionId, StreamType};

use super::super::shared::{ChannelState, ConsumerKey, ConsumerState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerRouteUpdate {
    consumer_state: ConsumerState,
    stream_type: StreamType,
    active: bool,
}

impl ChannelState {
    pub(in crate::runtime::channel) fn remember_download_states(
        &mut self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        let existing_states = session
            .desired_download_states
            .entry(target_session_id.clone())
            .or_default();
        merge_download_states(existing_states, states);
        if download_states_are_empty(existing_states) {
            session.desired_download_states.remove(target_session_id);
        }
    }

    pub(in crate::runtime::channel) fn download_route_updates(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) -> Vec<ConsumerRouteUpdate> {
        let consumer_connection_id = self.session_connection_id(session_id);
        let mut route_updates = Vec::new();
        for (stream_type, active) in states.iter() {
            let key = ConsumerKey {
                consumer_session_id: session_id.clone(),
                producer_session_id: target_session_id.clone(),
                stream_type,
            };
            let Some(consumer_state) = self.consumer_index.get(&key).copied() else {
                continue;
            };
            if Some(consumer_state.consumer_connection_id) != consumer_connection_id {
                continue;
            }
            route_updates.push(ConsumerRouteUpdate {
                consumer_state,
                stream_type,
                active,
            });
        }
        route_updates
    }

    pub(in crate::runtime::channel) fn commit_download_route_updates(
        &mut self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        committed_updates: impl IntoIterator<Item = ConsumerRouteUpdate>,
    ) -> Vec<ConsumerRouteUpdate> {
        let consumer_connection_id = self.session_connection_id(session_id);
        let mut accepted_updates = Vec::new();
        for route_update in committed_updates {
            let key = ConsumerKey {
                consumer_session_id: session_id.clone(),
                producer_session_id: target_session_id.clone(),
                stream_type: route_update.stream_type,
            };
            let Some(current_consumer_state) = self.consumer_index.get(&key).copied() else {
                continue;
            };
            if current_consumer_state != route_update.consumer_state
                || Some(current_consumer_state.consumer_connection_id) != consumer_connection_id
            {
                continue;
            }
            let paused = !route_update.active;
            if self
                .topology
                .set_consumer_paused(current_consumer_state.routed_consumer_id, paused)
                .is_err()
            {
                error!(
                    ?session_id,
                    ?target_session_id,
                    ?route_update.stream_type,
                    "failed to set consumer pause state in channel router"
                );
                continue;
            }
            accepted_updates.push(route_update);
        }
        accepted_updates
    }

    pub(super) fn desired_download_active(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        stream_type: StreamType,
    ) -> bool {
        self.sessions
            .get(session_id)
            .and_then(|session| session.desired_download_states.get(target_session_id))
            .and_then(|states| download_state_for_stream_type(states, stream_type))
            .unwrap_or(true)
    }
}

impl ConsumerRouteUpdate {
    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> u64 {
        self.consumer_state.consumer_connection_id
    }

    pub(in crate::runtime::channel) const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_state.consumer_media
    }

    pub(in crate::runtime::channel) const fn source_connection_id(&self) -> u64 {
        self.consumer_state.source_connection_id
    }

    pub(in crate::runtime::channel) const fn source_media(&self) -> TransportMediaId {
        self.consumer_state.source_media
    }

    pub(in crate::runtime::channel) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(in crate::runtime::channel) const fn active(&self) -> bool {
        self.active
    }
}

fn merge_download_states(target: &mut DownloadStates, update: &DownloadStates) {
    if let Some(audio) = update.audio {
        target.audio = Some(audio);
    }
    if let Some(camera) = update.camera {
        target.camera = Some(camera);
    }
    if let Some(screen) = update.screen {
        target.screen = Some(screen);
    }
}

fn download_states_are_empty(states: &DownloadStates) -> bool {
    states.audio.is_none() && states.camera.is_none() && states.screen.is_none()
}

fn download_state_for_stream_type(
    states: &DownloadStates,
    stream_type: StreamType,
) -> Option<bool> {
    match stream_type {
        StreamType::Audio => states.audio,
        StreamType::Camera => states.camera,
        StreamType::Screen => states.screen,
    }
}
