//! Coalesces transport observations into room source-policy wakeups.

use std::{
    collections::{BTreeMap, BTreeSet},
    iter, mem,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::Notify,
    time::{Instant, sleep_until},
};

use super::MediaTransport;
use crate::{RoomInstanceId, engine::sync::lock_unpoisoned};

const SOURCE_POLICY_FOLLOW_UP_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug, Default)]
struct PendingSourcePolicyUpdates {
    rooms: BTreeSet<RoomInstanceId>,
    scheduled_by_room: BTreeMap<RoomInstanceId, Instant>,
    scheduled_by_deadline: BTreeSet<(Instant, RoomInstanceId)>,
}

#[derive(Debug, Default)]
struct SourcePolicyUpdates {
    pending: Mutex<PendingSourcePolicyUpdates>,
    notify: Notify,
}

impl SourcePolicyUpdates {
    fn take_ready(&self, now: Instant) -> (BTreeSet<RoomInstanceId>, Option<Instant>) {
        let mut pending = lock_unpoisoned(&self.pending);
        let mut rooms = mem::take(&mut pending.rooms);
        for room in &rooms {
            if let Some(deadline) = pending.scheduled_by_room.remove(room) {
                pending.scheduled_by_deadline.remove(&(deadline, *room));
            }
        }
        while let Some((deadline, room)) = pending.scheduled_by_deadline.first().copied() {
            if deadline > now {
                break;
            }
            pending.scheduled_by_deadline.pop_first();
            pending.scheduled_by_room.remove(&room);
            rooms.insert(room);
        }
        let next_deadline = pending
            .scheduled_by_deadline
            .first()
            .map(|(deadline, _room)| *deadline);
        drop(pending);
        (rooms, next_deadline)
    }
}

/// Receiver for coalesced room source-policy updates.
#[derive(Debug, Clone)]
pub struct SourcePolicyUpdateSubscription(Arc<SourcePolicyUpdates>);

impl SourcePolicyUpdateSubscription {
    /// Waits until at least one room needs a source-policy pass.
    pub async fn wait_for_update(&self) -> BTreeSet<RoomInstanceId> {
        loop {
            let (rooms, next_deadline) = self.0.take_ready(Instant::now());
            if !rooms.is_empty() {
                return rooms;
            }
            if let Some(deadline) = next_deadline {
                tokio::select! {
                    () = self.0.notify.notified() => {}
                    () = sleep_until(deadline) => {}
                }
            } else {
                self.0.notify.notified().await;
            }
        }
    }

    /// Drains updates published after the previous wait completed.
    #[must_use]
    pub fn take_pending_updates(&self) -> BTreeSet<RoomInstanceId> {
        self.0.take_ready(Instant::now()).0
    }
}

/// Sender for coalesced room source-policy updates.
#[derive(Debug, Clone, Default)]
pub struct SourcePolicySignal(Arc<SourcePolicyUpdates>);

impl SourcePolicySignal {
    /// Creates the runtime's single-consumer subscription.
    #[must_use]
    pub fn subscribe(&self) -> SourcePolicyUpdateSubscription {
        SourcePolicyUpdateSubscription(Arc::clone(&self.0))
    }

    /// Marks one room as needing a source-policy pass.
    pub fn mark_dirty(&self, room_instance_id: RoomInstanceId) {
        self.mark_dirty_rooms([room_instance_id]);
    }

    /// Marks rooms as needing a source-policy pass.
    pub fn mark_dirty_rooms(&self, room_instance_ids: impl IntoIterator<Item = RoomInstanceId>) {
        let mut room_instance_ids = room_instance_ids.into_iter();
        let Some(first_room) = room_instance_ids.next() else {
            return;
        };
        let mut pending = lock_unpoisoned(&self.0.pending);
        let mut notify = false;
        for room in iter::once(first_room).chain(room_instance_ids) {
            notify |= pending.rooms.insert(room);
            if let Some(deadline) = pending.scheduled_by_room.remove(&room) {
                pending.scheduled_by_deadline.remove(&(deadline, room));
            }
        }
        drop(pending);
        if notify {
            self.0.notify.notify_one();
        }
    }

    fn mark_dirty_after(&self, room: RoomInstanceId, delay: Duration) {
        self.mark_dirty_at(room, Instant::now() + delay);
    }

    fn mark_dirty_at(&self, room: RoomInstanceId, deadline: Instant) {
        let mut pending = lock_unpoisoned(&self.0.pending);
        if pending.rooms.contains(&room) {
            return;
        }
        let notify = match pending.scheduled_by_room.get(&room).copied() {
            Some(current) if deadline < current => {
                pending.scheduled_by_deadline.remove(&(current, room));
                pending.scheduled_by_room.insert(room, deadline);
                pending.scheduled_by_deadline.insert((deadline, room));
                true
            }
            Some(_) => false,
            None => {
                pending.scheduled_by_room.insert(room, deadline);
                pending.scheduled_by_deadline.insert((deadline, room));
                true
            }
        };
        drop(pending);
        if notify {
            self.0.notify.notify_one();
        }
    }
}

impl MediaTransport {
    pub(in crate::engine) fn schedule_source_policy_follow_up(&self, room: RoomInstanceId) {
        self.source_policy_signal
            .mark_dirty_after(room, SOURCE_POLICY_FOLLOW_UP_DELAY);
    }
}

#[cfg(test)]
#[path = "TESTS/policy_invalidation.rs"]
mod tests;
