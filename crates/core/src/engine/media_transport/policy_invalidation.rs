//! Coalesces transport observations into room source-policy wakeups.

use std::{
    collections::BTreeSet,
    mem,
    sync::{Arc, Mutex},
};

use tokio::sync::Notify;

use crate::{RoomInstanceId, engine::sync::lock_unpoisoned};

#[derive(Debug, Default)]
struct SourcePolicyUpdates {
    rooms: Mutex<BTreeSet<RoomInstanceId>>,
    notify: Notify,
}

impl SourcePolicyUpdates {
    fn take(&self) -> BTreeSet<RoomInstanceId> {
        mem::take(&mut *lock_unpoisoned(&self.rooms))
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
        let mut rooms = lock_unpoisoned(&self.0.rooms);
        let notify = rooms.is_empty();
        rooms.insert(first);
        rooms.extend(room_instance_ids);
        drop(rooms);
        if notify {
            self.0.notify.notify_one();
        }
    }
}

#[cfg(test)]
#[path = "TESTS/policy_invalidation.rs"]
mod tests;
