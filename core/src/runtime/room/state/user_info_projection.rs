use std::collections::BTreeMap;

use super::{
    layout::UserLayout,
    presence::UserPresence,
    shared::{ActiveUser, RoomState, SourceKey},
};
use crate::runtime::{ConnectionId, PeerSnapshot, StreamType, UserId, UserInfo};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct UserMediaView {
    pub(in crate::runtime::room) camera_active: Option<bool>,
    pub(in crate::runtime::room) screen_active: Option<bool>,
}

#[must_use]
pub(in crate::runtime::room) fn project_user_info(
    presence: &UserPresence,
    layout: &UserLayout,
    media: UserMediaView,
) -> UserInfo {
    UserInfo {
        is_talking: presence.talking(),
        is_featured: layout.featured(),
        is_camera_on: media.camera_active,
        is_screen_sharing_on: media.screen_active,
        is_self_muted: presence.self_muted(),
        is_deaf: presence.deaf(),
        is_raising_hand: presence.raising_hand(),
    }
    .snapshot_complete()
}

impl RoomState {
    pub(in crate::runtime::room) fn user_snapshots_except(
        &self,
        excluded_user_id: &UserId,
    ) -> Vec<PeerSnapshot> {
        self.users
            .iter()
            .filter(|(user_id, _session)| *user_id != excluded_user_id)
            .map(|(user_id, user)| PeerSnapshot {
                user_id: user_id.clone(),
                info: self.user_info(user_id, user),
            })
            .collect()
    }

    pub(in crate::runtime::room) fn user_stats_counts(&self) -> (u64, u64, u64) {
        let camera_count = self
            .users
            .iter()
            .filter(|(user_id, user)| {
                self.user_media_view(user_id, user.connection_id)
                    .camera_active
                    == Some(true)
            })
            .count();
        let screen_count = self
            .users
            .iter()
            .filter(|(user_id, user)| {
                self.user_media_view(user_id, user.connection_id)
                    .screen_active
                    == Some(true)
            })
            .count();
        (
            u64::try_from(self.users.len()).unwrap_or(u64::MAX),
            u64::try_from(camera_count).unwrap_or(u64::MAX),
            u64::try_from(screen_count).unwrap_or(u64::MAX),
        )
    }

    pub(in crate::runtime::room) fn user_info_snapshot(
        &self,
        user_id: &UserId,
    ) -> Option<(UserId, UserInfo)> {
        let user = self.users.get(user_id)?;
        Some((user_id.clone(), self.user_info(user_id, user)))
    }

    pub(in crate::runtime::room) fn user_info_snapshot_all(&self) -> BTreeMap<UserId, UserInfo> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), self.user_info(user_id, user)))
            .collect()
    }

    fn user_info(&self, user_id: &UserId, user: &ActiveUser) -> UserInfo {
        project_user_info(
            &user.presence,
            &user.layout,
            self.user_media_view(user_id, user.connection_id),
        )
    }

    fn user_media_view(&self, user_id: &UserId, connection_id: ConnectionId) -> UserMediaView {
        UserMediaView {
            camera_active: self.stream_activity(user_id, connection_id, StreamType::Camera),
            screen_active: self.stream_activity(user_id, connection_id, StreamType::Screen),
        }
    }

    fn stream_activity(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<bool> {
        let producer_id = self.producer_id_for_source_key(&SourceKey::new(user_id, stream_type))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id != connection_id {
            return None;
        }
        Some(producer.active)
    }
}
