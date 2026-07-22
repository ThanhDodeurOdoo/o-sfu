use crate::engine::room::{Room, RoomMediaCounts};

pub(super) fn record_gauges(gauges: &mut Vec<RoomGaugeDelta>, room: &Room) {
    for delta in gauges.drain(..) {
        delta.record(room);
    }
}

/// counter delta captured under the same `RoomState` lock as the transition
#[derive(Debug, Clone, Copy)]
pub struct RoomGaugeDelta {
    users: i64,
    publications: i64,
    subscriptions: i64,
}

impl RoomGaugeDelta {
    pub fn membership(
        users_before: usize,
        users_after: usize,
        media_before: RoomMediaCounts,
        media_after: RoomMediaCounts,
    ) -> Self {
        Self {
            users: counter_delta(users_before, users_after),
            publications: counter_delta(media_before.publications, media_after.publications),
            subscriptions: counter_delta(media_before.subscriptions, media_after.subscriptions),
        }
    }

    pub fn media(before: RoomMediaCounts, after: RoomMediaCounts) -> Self {
        Self {
            users: 0,
            publications: counter_delta(before.publications, after.publications),
            subscriptions: counter_delta(before.subscriptions, after.subscriptions),
        }
    }

    fn record(self, room: &Room) {
        room.metrics.add_active_users(self.users);
        room.metrics.add_active_publications(self.publications);
        room.metrics.add_active_subscriptions(self.subscriptions);
    }
}

fn counter_delta(before: usize, after: usize) -> i64 {
    let before = i64::try_from(before).unwrap_or(i64::MAX);
    let after = i64::try_from(after).unwrap_or(i64::MAX);
    after.saturating_sub(before)
}
