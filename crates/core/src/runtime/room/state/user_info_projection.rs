use std::collections::{BTreeMap, BTreeSet};

use super::{
    layout::UserLayout,
    presence::UserPresence,
    shared::{ActiveUser, RoomState},
};
use crate::runtime::{PeerSnapshot, UserId, UserInfo, source_model::UserStreamId};

#[must_use]
pub(in crate::runtime::room) fn project_user_info(
    presence: &UserPresence,
    layout: &UserLayout,
) -> UserInfo {
    presence
        .info()
        .clone()
        .with_featured(layout.featured())
        .snapshot_complete()
}

impl RoomState {
    pub fn user_snapshots_except(&self, excluded_user_id: &UserId) -> Vec<PeerSnapshot> {
        self.users
            .iter()
            .filter(|(user_id, _session)| *user_id != excluded_user_id)
            .map(|(user_id, user)| PeerSnapshot {
                user_id: user_id.clone(),
                info: Self::user_info(user),
            })
            .collect()
    }

    pub fn user_stats_counts(&self) -> (u64, BTreeMap<UserStreamId, u64>) {
        let mut active_users_by_stream: BTreeMap<UserStreamId, BTreeSet<UserId>> = BTreeMap::new();
        for (stream_id, owner_user_id) in self.media.active_producer_stream_owners() {
            active_users_by_stream
                .entry(stream_id.clone())
                .or_default()
                .insert(owner_user_id.clone());
        }
        let active_stream_counts = active_users_by_stream
            .into_iter()
            .map(|(stream_id, users)| (stream_id, u64::try_from(users.len()).unwrap_or(u64::MAX)))
            .collect();
        (
            u64::try_from(self.users.len()).unwrap_or(u64::MAX),
            active_stream_counts,
        )
    }

    pub fn user_info_snapshot(&self, user_id: &UserId) -> Option<(UserId, UserInfo)> {
        let user = self.users.get(user_id)?;
        Some((user_id.clone(), Self::user_info(user)))
    }

    pub fn user_info_snapshot_all(&self) -> BTreeMap<UserId, UserInfo> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), Self::user_info(user)))
            .collect()
    }

    fn user_info(user: &ActiveUser) -> UserInfo {
        project_user_info(&user.presence, &user.layout)
    }
}
