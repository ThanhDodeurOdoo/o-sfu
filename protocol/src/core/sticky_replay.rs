use std::collections::{BTreeMap, BTreeSet};

use crate::{
    shared::{DownloadStates, SessionId, SessionInfo, StreamType},
    signaling::{
        ClientEnvelope, ClientMessage, EnvelopeBatch, StreamIntentPayload, SubscribePayload,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct StickyReplayState {
    active_uploads: BTreeSet<StreamType>,
    desired_downloads: BTreeMap<SessionId, DownloadStates>,
    desired_info: Option<SessionInfo>,
}

impl StickyReplayState {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn clear(&mut self) {
        self.active_uploads.clear();
        self.desired_downloads.clear();
        self.desired_info = None;
    }

    pub(super) fn set_upload_active(&mut self, stream_type: StreamType, active: bool) {
        if active {
            self.active_uploads.insert(stream_type);
        } else {
            self.active_uploads.remove(&stream_type);
        }
    }

    pub(super) fn remember_download_states(
        &mut self,
        session_id: &SessionId,
        states: &DownloadStates,
    ) {
        let existing_states = self
            .desired_downloads
            .entry(session_id.clone())
            .or_default();
        merge_download_states(existing_states, states);
        if download_states_are_empty(existing_states) {
            self.desired_downloads.remove(session_id);
        }
    }

    pub(super) fn remember_info(&mut self, info: &SessionInfo) {
        let existing_info = self.desired_info.get_or_insert_with(SessionInfo::default);
        merge_session_info(existing_info, info);
    }

    pub(super) fn replay_batch(&self) -> Option<EnvelopeBatch> {
        let mut replay_batch = Vec::new();

        for &stream_type in &self.active_uploads {
            let Some(envelope) =
                ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
                    stream_type,
                }))
                .into_envelope()
                .ok()
            else {
                continue;
            };
            replay_batch.push(envelope);
        }

        for (session_id, states) in &self.desired_downloads {
            let Some(envelope) =
                ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                    session_id: session_id.clone(),
                    states: states.clone(),
                }))
                .into_envelope()
                .ok()
            else {
                continue;
            };
            replay_batch.push(envelope);
        }

        if let Some(info) = self.desired_info.clone() {
            let envelope = ClientEnvelope::Message(ClientMessage::Info(info))
                .into_envelope()
                .ok()?;
            replay_batch.push(envelope);
        }

        if replay_batch.is_empty() {
            None
        } else {
            Some(replay_batch)
        }
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

fn merge_session_info(target: &mut SessionInfo, update: &SessionInfo) {
    if let Some(is_talking) = update.is_talking {
        target.is_talking = Some(is_talking);
    }
    if let Some(is_camera_on) = update.is_camera_on {
        target.is_camera_on = Some(is_camera_on);
    }
    if let Some(is_screen_sharing_on) = update.is_screen_sharing_on {
        target.is_screen_sharing_on = Some(is_screen_sharing_on);
    }
    if let Some(is_self_muted) = update.is_self_muted {
        target.is_self_muted = Some(is_self_muted);
    }
    if let Some(is_deaf) = update.is_deaf {
        target.is_deaf = Some(is_deaf);
    }
    if let Some(is_raising_hand) = update.is_raising_hand {
        target.is_raising_hand = Some(is_raising_hand);
    }
}
