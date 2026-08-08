//! Coalesces transport observations into room source-policy wakeups.

use std::{
    collections::{BTreeMap, BTreeSet},
    iter, mem,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{sync::Notify, time::sleep};

use super::MediaTransport;
use crate::{RoomInstanceId, engine::sync::lock_unpoisoned};

const SOURCE_POLICY_FOLLOW_UP_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug, Default)]
struct PendingSourcePolicyUpdates {
    rooms: BTreeSet<RoomInstanceId>,
    scheduled: BTreeMap<RoomInstanceId, u64>,
    next_schedule_token: u64,
}

#[derive(Debug, Default)]
struct SourcePolicyUpdates {
    pending: Mutex<PendingSourcePolicyUpdates>,
    notify: Notify,
}

impl SourcePolicyUpdates {
    fn take(&self) -> BTreeSet<RoomInstanceId> {
        mem::take(&mut lock_unpoisoned(&self.pending).rooms)
    }

    fn publish(&self, room: RoomInstanceId, token: u64) {
        let mut pending = lock_unpoisoned(&self.pending);
        if pending.scheduled.get(&room) != Some(&token) {
            return;
        }
        pending.scheduled.remove(&room);
        let notify = pending.rooms.is_empty();
        pending.rooms.insert(room);
        drop(pending);
        if notify {
            self.notify.notify_one();
        }
    }
}

/// Receiver for coalesced room source-policy updates.
#[derive(Debug, Clone)]
pub struct SourcePolicyUpdateSubscription(Arc<SourcePolicyUpdates>);

impl SourcePolicyUpdateSubscription {
    /// Waits until at least one room needs a source-policy pass.
    pub async fn wait_for_update(&self) -> BTreeSet<RoomInstanceId> {
        loop {
            let rooms = self.0.take();
            if !rooms.is_empty() {
                return rooms;
            }
            self.0.notify.notified().await;
        }
    }

    /// Drains updates published after the previous wait completed.
    #[must_use]
    pub fn take_pending_updates(&self) -> BTreeSet<RoomInstanceId> {
        self.0.take()
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
        let Some(first) = room_instance_ids.next() else {
            return;
        };
        let mut pending = lock_unpoisoned(&self.0.pending);
        // The single consumer drains `pending.rooms`, so only its
        // empty-to-nonempty transition needs a wake.
        let notify = pending.rooms.is_empty();
        if pending.scheduled.is_empty() {
            pending.rooms.insert(first);
            pending.rooms.extend(room_instance_ids);
        } else {
            for room in iter::once(first).chain(room_instance_ids) {
                pending.scheduled.remove(&room);
                pending.rooms.insert(room);
            }
        }
        drop(pending);
        if notify {
            self.0.notify.notify_one();
        }
    }

    /// Schedules one room wake unless current work makes it immediately dirty.
    ///
    /// Tokens prevent stale spawned tasks from publishing after an immediate
    /// wake cancels then reschedules the same room.
    fn mark_dirty_after(&self, room: RoomInstanceId, delay: Duration) {
        let token = {
            let mut pending = lock_unpoisoned(&self.0.pending);
            if pending.rooms.contains(&room) || pending.scheduled.contains_key(&room) {
                return;
            }
            let token = pending.next_schedule_token;
            pending.next_schedule_token = pending.next_schedule_token.wrapping_add(1);
            pending.scheduled.insert(room, token);
            token
        };
        let updates = Arc::clone(&self.0);
        tokio::spawn(async move {
            sleep(delay).await;
            updates.publish(room, token);
        });
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
