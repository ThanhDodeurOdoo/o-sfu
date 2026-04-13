use std::collections::BTreeMap;

use crate::signaling::protocol::PeerSnapshot;
use crate::signaling::shared::SessionInfo;
use crate::signaling::shared::{SessionId, StreamType};

use super::presence::SessionPresence;
use super::shared::{ActiveSession, ChannelState, ProducerKey};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime::channel) struct SessionMediaView {
    pub(in crate::runtime::channel) camera_active: Option<bool>,
    pub(in crate::runtime::channel) screen_active: Option<bool>,
}

#[must_use]
pub(in crate::runtime::channel) fn project_session_info(
    presence: &SessionPresence,
    media: SessionMediaView,
) -> SessionInfo {
    SessionInfo {
        is_talking: presence.talking(),
        is_camera_on: media.camera_active,
        is_screen_sharing_on: media.screen_active,
        is_self_muted: presence.self_muted(),
        is_deaf: presence.deaf(),
        is_raising_hand: presence.raising_hand(),
    }
}

impl ChannelState {
    pub(in crate::runtime::channel) fn peer_snapshots_except(
        &self,
        excluded_session_id: &SessionId,
    ) -> Vec<PeerSnapshot> {
        self.sessions
            .iter()
            .filter(|(session_id, _session)| *session_id != excluded_session_id)
            .map(|(session_id, session)| PeerSnapshot {
                session_id: session_id.clone(),
                info: self.session_info(session_id, session),
            })
            .collect()
    }

    pub(in crate::runtime::channel) fn session_stats_counts(&self) -> (u64, u64, u64) {
        let camera_count = self
            .sessions
            .iter()
            .filter(|(session_id, session)| {
                self.session_media_view(session_id, session.connection_id)
                    .camera_active
                    == Some(true)
            })
            .count();
        let screen_count = self
            .sessions
            .iter()
            .filter(|(session_id, session)| {
                self.session_media_view(session_id, session.connection_id)
                    .screen_active
                    == Some(true)
            })
            .count();
        (
            u64::try_from(self.sessions.len()).unwrap_or(u64::MAX),
            u64::try_from(camera_count).unwrap_or(u64::MAX),
            u64::try_from(screen_count).unwrap_or(u64::MAX),
        )
    }

    pub(in crate::runtime::channel) fn session_info_snapshot(
        &self,
        session_id: &SessionId,
    ) -> Option<(SessionId, SessionInfo)> {
        let session = self.sessions.get(session_id)?;
        Some((session_id.clone(), self.session_info(session_id, session)))
    }

    pub(in crate::runtime::channel) fn session_info_snapshot_all(
        &self,
    ) -> BTreeMap<SessionId, SessionInfo> {
        self.sessions
            .iter()
            .map(|(session_id, session)| {
                (session_id.clone(), self.session_info(session_id, session))
            })
            .collect()
    }

    fn session_info(&self, session_id: &SessionId, session: &ActiveSession) -> SessionInfo {
        project_session_info(
            &session.presence,
            self.session_media_view(session_id, session.connection_id),
        )
    }

    fn session_media_view(&self, session_id: &SessionId, connection_id: u64) -> SessionMediaView {
        SessionMediaView {
            camera_active: self.stream_activity(session_id, connection_id, StreamType::Camera),
            screen_active: self.stream_activity(session_id, connection_id, StreamType::Screen),
        }
    }

    fn stream_activity(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
    ) -> Option<bool> {
        let producer_id = self
            .producer_ids_by_owner_stream
            .get(&ProducerKey::new(session_id, stream_type))?;
        let producer = self.producers.get(producer_id)?;
        if producer.owner_connection_id != connection_id {
            return None;
        }
        Some(producer.active)
    }
}
